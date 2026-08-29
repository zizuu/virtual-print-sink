#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

mod server;
mod storage;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use server::{EventSink, ServerConfig, ServerController, ServerEvent};

slint::include_modules!();

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .compact()
        .init();

    let output_dir = default_output_dir()?;
    std::fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "failed to create default output directory: {}",
            output_dir.display()
        )
    })?;

    let app = AppWindow::new()?;
    let controller = Arc::new(Mutex::new(ServerController::default()));
    let output_dir = Arc::new(Mutex::new(output_dir));

    refresh_static_text(&app, &output_dir.lock().expect("output path mutex poisoned"));

    let app_weak = app.as_weak();
    let events: EventSink = Arc::new(move |event| {
        let weak = app_weak.clone();
        let _ = weak.upgrade_in_event_loop(move |window| match event {
            ServerEvent::Status(text) => {
                window.set_status_text(text.into());
            }
            ServerEvent::JobSaved {
                protocol,
                path,
                bytes,
            } => {
                window.set_last_job_text(
                    format!("[{protocol}] {} ({} bytes)", path.display(), bytes).into(),
                );
            }
            ServerEvent::Error(message) => {
                window.set_status_text(format!("エラー: {message}").into());
            }
        });
    });

    {
        let app_weak = app.as_weak();
        let output_dir = Arc::clone(&output_dir);
        app.on_choose_output_requested(move || {
            let current = output_dir
                .lock()
                .expect("output path mutex poisoned")
                .clone();

            let mut dialog = rfd::FileDialog::new().set_title("印刷データの出力先を選択");
            if current.exists() {
                dialog = dialog.set_directory(&current);
            }

            if let Some(selected) = dialog.pick_folder() {
                *output_dir.lock().expect("output path mutex poisoned") = selected.clone();
                if let Some(window) = app_weak.upgrade() {
                    window.set_output_path(selected.display().to_string().into());
                }
            }
        });
    }

    {
        let app_weak = app.as_weak();
        let output_dir = Arc::clone(&output_dir);
        let controller = Arc::clone(&controller);
        let events = Arc::clone(&events);
        app.on_start_requested(move || {
            let selected = output_dir
                .lock()
                .expect("output path mutex poisoned")
                .clone();
            let config = ServerConfig::default_for_platform(selected);

            if let Some(window) = app_weak.upgrade() {
                window.set_endpoint_text(config.endpoint_summary().into());
                window.set_status_text("起動中...".into());
            }

            let result = controller
                .lock()
                .expect("server controller mutex poisoned")
                .start(config, Arc::clone(&events));

            if let Some(window) = app_weak.upgrade() {
                match result {
                    Ok(()) => {
                        window.set_server_running(true);
                    }
                    Err(err) => {
                        window.set_server_running(false);
                        window.set_status_text(format!("起動失敗: {err:#}").into());
                    }
                }
            }
        });
    }

    {
        let app_weak = app.as_weak();
        let controller = Arc::clone(&controller);
        app.on_stop_requested(move || {
            controller
                .lock()
                .expect("server controller mutex poisoned")
                .stop();
            if let Some(window) = app_weak.upgrade() {
                window.set_server_running(false);
                window.set_status_text("停止中".into());
            }
        });
    }

    app.run()?;
    controller
        .lock()
        .expect("server controller mutex poisoned")
        .stop();
    Ok(())
}

fn refresh_static_text(app: &AppWindow, output_dir: &PathBuf) {
    let config = ServerConfig::default_for_platform(output_dir.clone());
    app.set_output_path(output_dir.display().to_string().into());
    app.set_endpoint_text(config.endpoint_summary().into());
}

fn default_output_dir() -> Result<PathBuf> {
    Ok(std::env::current_dir()?.join("print_jobs"))
}
