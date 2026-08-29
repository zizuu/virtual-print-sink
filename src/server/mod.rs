pub mod ipp;
pub mod lpr;

use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, sync::broadcast};

use crate::storage::JobStorage;

pub type EventSink = Arc<dyn Fn(ServerEvent) + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub enum ServerEvent {
    Status(String),
    JobSaved {
        protocol: &'static str,
        path: PathBuf,
        bytes: usize,
    },
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub output_dir: PathBuf,
    pub lpr_addr: SocketAddr,
    pub ipp_addr: SocketAddr,
}

impl ServerConfig {
    pub fn default_for_platform(output_dir: PathBuf) -> Self {
        let lpr_default = if cfg!(target_os = "windows") { 515 } else { 1515 };
        let lpr_port = env_port("VPS_LPR_PORT", lpr_default);
        let ipp_port = env_port("VPS_IPP_PORT", 8631);
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);

        Self {
            output_dir,
            lpr_addr: SocketAddr::new(loopback, lpr_port),
            ipp_addr: SocketAddr::new(loopback, ipp_port),
        }
    }

    pub fn endpoint_summary(&self) -> String {
        format!(
            "LPR: lpd://127.0.0.1:{}/virtual    |    IPP: ipp://127.0.0.1:{}/printers/virtual",
            self.lpr_addr.port(),
            self.ipp_addr.port()
        )
    }
}

fn env_port(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default)
}

struct RunningServer {
    shutdown: broadcast::Sender<()>,
    thread: thread::JoinHandle<()>,
}

#[derive(Default)]
pub struct ServerController {
    running: Option<RunningServer>,
}

impl ServerController {
    pub fn is_running(&self) -> bool {
        self.running.is_some()
    }

    pub fn start(&mut self, config: ServerConfig, events: EventSink) -> Result<()> {
        if self.running.is_some() {
            bail!("server is already running");
        }

        std::fs::create_dir_all(&config.output_dir).with_context(|| {
            format!(
                "failed to create output directory: {}",
                config.output_dir.display()
            )
        })?;

        let (shutdown_tx, _) = broadcast::channel::<()>(8);
        let shutdown_for_thread = shutdown_tx.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), String>>(1);
        let events_for_thread = Arc::clone(&events);
        let ready_on_error = ready_tx.clone();

        let thread = thread::Builder::new()
            .name("virtual-print-server".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("virtual-print-worker")
                    .build();

                let runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let message = format!("failed to create async runtime: {err}");
                        let _ = ready_tx.send(Err(message.clone()));
                        (events_for_thread)(ServerEvent::Error(message));
                        return;
                    }
                };

                let result: Result<()> = runtime.block_on(async {
                    let lpr_listener = TcpListener::bind(config.lpr_addr)
                        .await
                        .with_context(|| format!("LPR bind failed: {}", config.lpr_addr))?;
                    let ipp_listener = TcpListener::bind(config.ipp_addr)
                        .await
                        .with_context(|| format!("IPP bind failed: {}", config.ipp_addr))?;

                    let storage = JobStorage::new(config.output_dir.clone());
                    storage.ensure_output_dir().await?;

                    let _ = ready_tx.send(Ok(()));
                    (events_for_thread)(ServerEvent::Status(format!(
                        "稼働中 - LPR {} / IPP {}",
                        config.lpr_addr, config.ipp_addr
                    )));

                    let lpr_shutdown = shutdown_for_thread.subscribe();
                    let ipp_shutdown = shutdown_for_thread.subscribe();

                    let lpr_future = lpr::run(
                        lpr_listener,
                        storage.clone(),
                        Arc::clone(&events_for_thread),
                        lpr_shutdown,
                    );
                    let ipp_future = ipp::run(
                        ipp_listener,
                        storage,
                        Arc::clone(&events_for_thread),
                        ipp_shutdown,
                        config.ipp_addr.port(),
                    );

                    let (lpr_result, ipp_result) = tokio::join!(lpr_future, ipp_future);
                    lpr_result?;
                    ipp_result?;
                    Ok(())
                });

                if let Err(err) = result {
                    let message = format!("server stopped with error: {err:#}");
                    let _ = ready_on_error.send(Err(message.clone()));
                    (events_for_thread)(ServerEvent::Error(message));
                } else {
                    (events_for_thread)(ServerEvent::Status("停止中".to_string()));
                }
            })?;

        match ready_rx.recv_timeout(Duration::from_secs(4)) {
            Ok(Ok(())) => {
                self.running = Some(RunningServer {
                    shutdown: shutdown_tx,
                    thread,
                });
                Ok(())
            }
            Ok(Err(message)) => {
                let _ = thread.join();
                bail!(message)
            }
            Err(err) => {
                let _ = shutdown_tx.send(());
                let _ = thread.join();
                bail!("server start timed out: {err}")
            }
        }
    }

    pub fn stop(&mut self) {
        if let Some(running) = self.running.take() {
            let _ = running.shutdown.send(());
            let _ = running.thread.join();
        }
    }
}

impl Drop for ServerController {
    fn drop(&mut self) {
        self.stop();
    }
}
