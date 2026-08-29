use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobMetadata {
    pub protocol: String,
    pub received_at: String,
    pub source: Option<String>,
    pub queue: Option<String>,
    pub user: Option<String>,
    pub job_name: Option<String>,
    pub document_format: Option<String>,
    pub protocol_file_name: Option<String>,
    pub bytes: usize,
    pub raw_file_name: Option<String>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SavedJob {
    pub raw_path: PathBuf,
    pub metadata_path: PathBuf,
    pub bytes: usize,
}

#[derive(Clone)]
pub struct JobStorage {
    output_dir: PathBuf,
    sequence: Arc<AtomicU64>,
}

impl JobStorage {
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn ensure_output_dir(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.output_dir)
            .await
            .with_context(|| format!("failed to create output directory: {}", self.output_dir.display()))
    }

    pub async fn save_bytes(&self, data: &[u8], mut metadata: JobMetadata) -> Result<SavedJob> {
        self.ensure_output_dir().await?;

        let now = Local::now();
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let protocol = sanitize_component(&metadata.protocol.to_uppercase());
        let job_name = sanitize_component(metadata.job_name.as_deref().unwrap_or("print-job"));
        let extension = detect_extension(data, metadata.document_format.as_deref());
        let base = format!(
            "{}_{}_{:06}_{}",
            now.format("%Y%m%d_%H%M%S_%3f"),
            protocol,
            seq,
            job_name
        );

        let raw_path = self.output_dir.join(format!("{base}.{extension}"));
        let metadata_path = self.output_dir.join(format!("{base}.json"));

        tokio::fs::write(&raw_path, data)
            .await
            .with_context(|| format!("failed to write print data: {}", raw_path.display()))?;

        metadata.received_at = now.to_rfc3339();
        metadata.bytes = data.len();
        metadata.raw_file_name = raw_path
            .file_name()
            .and_then(|v| v.to_str())
            .map(ToOwned::to_owned);

        let json = serde_json::to_vec_pretty(&metadata)?;
        tokio::fs::write(&metadata_path, json)
            .await
            .with_context(|| format!("failed to write metadata: {}", metadata_path.display()))?;

        Ok(SavedJob {
            raw_path,
            metadata_path,
            bytes: data.len(),
        })
    }

    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') {
            out.push(c);
        } else {
            out.push('_');
        }
    }

    let trimmed = out.trim_matches(|c| c == '_' || c == '.').to_string();
    if trimmed.is_empty() {
        "print-job".to_string()
    } else {
        trimmed.chars().take(80).collect()
    }
}

fn detect_extension(data: &[u8], document_format: Option<&str>) -> &'static str {
    if let Some(format) = document_format {
        match format.to_ascii_lowercase().as_str() {
            "application/pdf" => return "pdf",
            "application/postscript" => return "ps",
            "application/vnd.hp-pcl" | "application/pcl" => return "pcl",
            "image/pwg-raster" => return "pwg",
            "image/urf" => return "urf",
            "image/jpeg" => return "jpg",
            "image/png" => return "png",
            "text/plain" => return "txt",
            _ => {}
        }
    }

    if data.starts_with(b"%PDF-") {
        "pdf"
    } else if data.starts_with(b"%!PS") {
        "ps"
    } else if data.starts_with(&[0xff, 0xd8, 0xff]) {
        "jpg"
    } else if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "png"
    } else if data.starts_with(b"RaS2") || data.starts_with(b"RaS3") {
        "pwg"
    } else if data.starts_with(b"UNIRAST") {
        "urf"
    } else if data.starts_with(b"\x1b%-12345X") {
        "pcl"
    } else {
        "prn"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_pdf() {
        assert_eq!(detect_extension(b"%PDF-1.7", None), "pdf");
    }

    #[test]
    fn sanitizes_file_names() {
        assert_eq!(sanitize_component("A/B:C"), "A_B_C");
    }
}
