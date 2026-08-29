use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};

use crate::{
    server::{EventSink, ServerEvent},
    storage::{JobMetadata, JobStorage},
};

const MAX_JOB_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
struct LprControlInfo {
    host: Option<String>,
    user: Option<String>,
    job_name: Option<String>,
    source_name: Option<String>,
    raw: String,
}

impl LprControlInfo {
    fn parse(raw: &[u8]) -> Self {
        let text = String::from_utf8_lossy(raw).into_owned();
        let mut info = Self {
            raw: text.clone(),
            ..Default::default()
        };

        for line in text.lines() {
            let mut chars = line.chars();
            let Some(code) = chars.next() else { continue };
            let value = chars.as_str().trim().to_string();
            match code {
                'H' => info.host = Some(value),
                'P' => info.user = Some(value),
                'J' => info.job_name = Some(value),
                'N' => info.source_name = Some(value),
                _ => {}
            }
        }

        info
    }
}

#[derive(Debug)]
struct PendingDataFile {
    protocol_name: String,
    data: Vec<u8>,
}

pub async fn run(
    listener: TcpListener,
    storage: JobStorage,
    events: EventSink,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("LPR accept failed")?;
                let storage = storage.clone();
                let events = Arc::clone(&events);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, peer, storage, Arc::clone(&events)).await {
                        (events)(ServerEvent::Error(format!("LPR error from {peer}: {err:#}")));
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    storage: JobStorage,
    events: EventSink,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let command = match reader.read_u8().await {
        Ok(command) => command,
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    match command {
        0x02 => {
            let queue = read_line(&mut reader).await?;
            write_ack(&mut write_half, true).await?;
            receive_job(
                &mut reader,
                &mut write_half,
                peer,
                queue,
                storage,
                events,
            )
            .await
        }
        0x03 | 0x04 => {
            let _ = read_line(&mut reader).await?;
            write_half
                .write_all(b"Virtual Print Sink: no queued jobs\n")
                .await?;
            Ok(())
        }
        0x01 | 0x05 => {
            let _ = read_line(&mut reader).await?;
            Ok(())
        }
        other => bail!("unsupported LPR command: 0x{other:02x}"),
    }
}

async fn receive_job<R, W>(
    reader: &mut R,
    writer: &mut W,
    peer: SocketAddr,
    queue: String,
    storage: JobStorage,
    events: EventSink,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut control: Option<LprControlInfo> = None;
    let mut pending_data: Vec<PendingDataFile> = Vec::new();

    loop {
        let subcommand = match reader.read_u8().await {
            Ok(value) => value,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        };

        match subcommand {
            0x01 => {
                let _ = read_line(reader).await?;
                return Ok(());
            }
            0x02 => {
                let header = read_line(reader).await?;
                let (count, _name) = parse_file_header(&header)?;
                if count > MAX_JOB_BYTES {
                    write_ack(writer, false).await?;
                    bail!("LPR control file exceeds limit: {count} bytes");
                }

                write_ack(writer, true).await?;
                let bytes = read_exact_vec(reader, count).await?;
                let terminator = reader.read_u8().await?;
                let ok = terminator == 0;
                write_ack(writer, ok).await?;
                if !ok {
                    bail!("invalid LPR control-file terminator");
                }
                control = Some(LprControlInfo::parse(&bytes));
            }
            0x03 => {
                let header = read_line(reader).await?;
                let (count, name) = parse_file_header(&header)?;
                if count == 0 {
                    write_ack(writer, false).await?;
                    bail!("zero-length/streaming LPR data files are not supported by this MVP");
                }
                if count > MAX_JOB_BYTES {
                    write_ack(writer, false).await?;
                    bail!("LPR print data exceeds limit: {count} bytes");
                }

                write_ack(writer, true).await?;
                let data = read_exact_vec(reader, count).await?;
                let terminator = reader.read_u8().await?;
                let ok = terminator == 0;
                write_ack(writer, ok).await?;
                if !ok {
                    bail!("invalid LPR data-file terminator");
                }

                pending_data.push(PendingDataFile {
                    protocol_name: name,
                    data,
                });
            }
            other => bail!("unsupported LPR receive-job subcommand: 0x{other:02x}"),
        }
    }

    let control = control.unwrap_or_default();
    for (index, data_file) in pending_data.into_iter().enumerate() {
        let mut attributes = BTreeMap::new();
        if !control.raw.is_empty() {
            attributes.insert("lpr-control-file".to_string(), control.raw.clone());
        }
        if let Some(host) = &control.host {
            attributes.insert("originating-host".to_string(), host.clone());
        }

        let job_name = control
            .job_name
            .clone()
            .or_else(|| control.source_name.clone())
            .or_else(|| Some(format!("lpr-job-{}", index + 1)));

        let saved = storage
            .save_bytes(
                &data_file.data,
                JobMetadata {
                    protocol: "LPR".to_string(),
                    source: Some(peer.to_string()),
                    queue: Some(queue.clone()),
                    user: control.user.clone(),
                    job_name,
                    protocol_file_name: Some(data_file.protocol_name),
                    attributes,
                    ..Default::default()
                },
            )
            .await?;

        (events)(ServerEvent::JobSaved {
            protocol: "LPR",
            path: saved.raw_path,
            bytes: saved.bytes,
        });
    }

    Ok(())
}

async fn read_line<R>(reader: &mut R) -> Result<String>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::new();
    let count = reader.read_until(b'\n', &mut bytes).await?;
    if count == 0 {
        bail!("unexpected EOF while reading LPR line");
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn parse_file_header(line: &str) -> Result<(usize, String)> {
    let mut parts = line.splitn(2, ' ');
    let count = parts
        .next()
        .context("missing LPR file length")?
        .parse::<usize>()
        .context("invalid LPR file length")?;
    let name = parts
        .next()
        .context("missing LPR file name")?
        .trim()
        .to_string();
    Ok((count, name))
}

async fn read_exact_vec<R>(reader: &mut R, count: usize) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut data = vec![0_u8; count];
    reader.read_exact(&mut data).await?;
    Ok(data)
}

async fn write_ack<W>(writer: &mut W, success: bool) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&[if success { 0 } else { 1 }]).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_control_file() {
        let info = LprControlInfo::parse(b"Hhost\nPuser\nJReport\nNreport.pdf\n");
        assert_eq!(info.host.as_deref(), Some("host"));
        assert_eq!(info.user.as_deref(), Some("user"));
        assert_eq!(info.job_name.as_deref(), Some("Report"));
    }

    #[test]
    fn parses_file_header() {
        let (count, name) = parse_file_header("123 dfA001host").unwrap();
        assert_eq!(count, 123);
        assert_eq!(name, "dfA001host");
    }
}
