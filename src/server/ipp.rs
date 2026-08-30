use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::get,
};
use tokio::{net::TcpListener, sync::broadcast};

use crate::{
    server::{EventSink, ServerEvent},
    storage::{JobMetadata, JobStorage},
};

const MAX_IPP_BODY_BYTES: usize = 256 * 1024 * 1024;

const OP_PRINT_JOB: u16 = 0x0002;
const OP_VALIDATE_JOB: u16 = 0x0004;
const OP_CREATE_JOB: u16 = 0x0005;
const OP_SEND_DOCUMENT: u16 = 0x0006;
const OP_CANCEL_JOB: u16 = 0x0008;
const OP_GET_JOB_ATTRIBUTES: u16 = 0x0009;
const OP_GET_JOBS: u16 = 0x000a;
const OP_GET_PRINTER_ATTRIBUTES: u16 = 0x000b;

const STATUS_OK: u16 = 0x0000;
const STATUS_CLIENT_ERROR_NOT_FOUND: u16 = 0x0406;
const STATUS_SERVER_ERROR_OPERATION_NOT_SUPPORTED: u16 = 0x0501;

const GROUP_OPERATION: u8 = 0x01;
const GROUP_JOB: u8 = 0x02;
const GROUP_END: u8 = 0x03;
const GROUP_PRINTER: u8 = 0x04;

const TAG_INTEGER: u8 = 0x21;
const TAG_BOOLEAN: u8 = 0x22;
const TAG_ENUM: u8 = 0x23;
const TAG_RESOLUTION: u8 = 0x32;
const TAG_RANGE: u8 = 0x33;
const TAG_TEXT: u8 = 0x41;
const TAG_NAME: u8 = 0x42;
const TAG_KEYWORD: u8 = 0x44;
const TAG_URI: u8 = 0x45;
const TAG_CHARSET: u8 = 0x47;
const TAG_NATURAL_LANGUAGE: u8 = 0x48;
const TAG_MIME: u8 = 0x49;

// Keep these values stable. Windows derives the Microsoft IPP Class Driver
// hardware ID used for PSA association from printer-device-id.
const PRINTER_DEVICE_ID: &str = "MFG:OpenAI;MDL:Virtual Print Sink;CMD:PDF,POSTSCRIPT,PCL,PWGRASTER,URF;CLS:PRINTER;DES:Virtual Print Sink IPP Printer;";
const PRINTER_UUID: &str = "urn:uuid:8fddfcaf-d219-4c69-998c-e6a6681b2d11";

#[derive(Clone)]
struct IppState {
    storage: JobStorage,
    events: EventSink,
    ipp_port: u16,
    next_job_id: Arc<AtomicU32>,
    jobs: Arc<Mutex<HashMap<u32, IppJobRecord>>>,
}

#[derive(Debug, Clone)]
struct IppJobRecord {
    id: u32,
    name: String,
    user: String,
    state: i32,
    document_format: String,
    saved_path: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedAttribute {
    group: u8,
    tag: u8,
    values: Vec<Vec<u8>>,
}

#[derive(Debug)]
struct ParsedIppRequest {
    version_major: u8,
    version_minor: u8,
    operation: u16,
    request_id: u32,
    attributes: BTreeMap<String, ParsedAttribute>,
    document_offset: usize,
}

impl ParsedIppRequest {
    fn parse(body: &[u8]) -> Result<Self> {
        if body.len() < 9 {
            bail!("IPP request is too short");
        }

        let version_major = body[0];
        let version_minor = body[1];
        let operation = u16::from_be_bytes([body[2], body[3]]);
        let request_id = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);

        let mut index = 8usize;
        let mut current_group = 0u8;
        let mut last_name: Option<String> = None;
        let mut attributes: BTreeMap<String, ParsedAttribute> = BTreeMap::new();

        while index < body.len() {
            let tag = body[index];
            index += 1;

            if tag == GROUP_END {
                return Ok(Self {
                    version_major,
                    version_minor,
                    operation,
                    request_id,
                    attributes,
                    document_offset: index,
                });
            }

            if (0x01..=0x05).contains(&tag) {
                current_group = tag;
                last_name = None;
                continue;
            }

            let name_len = read_u16(body, &mut index)? as usize;
            if index + name_len > body.len() {
                bail!("invalid IPP attribute name length");
            }
            let name = if name_len == 0 {
                last_name
                    .clone()
                    .context("IPP attribute with empty name has no predecessor")?
            } else {
                let name = String::from_utf8_lossy(&body[index..index + name_len]).into_owned();
                index += name_len;
                last_name = Some(name.clone());
                name
            };

            let value_len = read_u16(body, &mut index)? as usize;
            if index + value_len > body.len() {
                bail!("invalid IPP attribute value length");
            }
            let value = body[index..index + value_len].to_vec();
            index += value_len;

            attributes
                .entry(name)
                .and_modify(|attr| attr.values.push(value.clone()))
                .or_insert(ParsedAttribute {
                    group: current_group,
                    tag,
                    values: vec![value],
                });
        }

        bail!("IPP end-of-attributes tag was not found")
    }

    fn get_string(&self, name: &str) -> Option<String> {
        self.attributes
            .get(name)
            .and_then(|attr| attr.values.first())
            .map(|value| String::from_utf8_lossy(value).into_owned())
    }

    fn get_i32(&self, name: &str) -> Option<i32> {
        self.attributes
            .get(name)
            .and_then(|attr| attr.values.first())
            .and_then(|value| {
                if value.len() == 4 {
                    Some(i32::from_be_bytes([value[0], value[1], value[2], value[3]]))
                } else {
                    None
                }
            })
    }

    fn version(&self) -> (u8, u8) {
        (self.version_major, self.version_minor)
    }
}

pub async fn run(
    listener: TcpListener,
    storage: JobStorage,
    events: EventSink,
    mut shutdown: broadcast::Receiver<()>,
    ipp_port: u16,
) -> Result<()> {
    let state = IppState {
        storage,
        events,
        ipp_port,
        next_job_id: Arc::new(AtomicU32::new(1)),
        jobs: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/", get(health))
        .route("/printers/virtual", get(health).post(handle_ipp))
        .route("/jobs/{id}", get(health).post(handle_ipp))
        .layer(DefaultBodyLimit::max(MAX_IPP_BODY_BYTES))
        .with_state(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
        })
        .await
        .context("IPP HTTP server failed")?;

    Ok(())
}

async fn health() -> &'static str {
    "Virtual Print Sink IPP endpoint. Send IPP POST requests to /printers/virtual.\n"
}

async fn handle_ipp(State(state): State<IppState>, body: Bytes) -> Response {
    match process_ipp_request(state, body).await {
        Ok(bytes) => ipp_http_response(bytes),
        Err(err) => {
            let message = format!("IPP request error: {err:#}");
            tracing::warn!("{message}");
            let mut response = Response::new(Body::from(message));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
        }
    }
}

async fn process_ipp_request(state: IppState, body: Bytes) -> Result<Vec<u8>> {
    let request = ParsedIppRequest::parse(&body)?;
    let document = &body[request.document_offset..];

    match request.operation {
        OP_GET_PRINTER_ATTRIBUTES => Ok(printer_attributes_response(&request, state.ipp_port)),
        OP_VALIDATE_JOB => Ok(success_response(&request)),
        OP_PRINT_JOB => print_job(&state, &request, document).await,
        OP_CREATE_JOB => create_job(&state, &request),
        OP_SEND_DOCUMENT => send_document(&state, &request, document).await,
        OP_GET_JOB_ATTRIBUTES => get_job_attributes(&state, &request),
        OP_GET_JOBS => Ok(get_jobs_response(&state, &request)),
        OP_CANCEL_JOB => cancel_job(&state, &request),
        _ => Ok(operation_not_supported_response(&request)),
    }
}

async fn print_job(
    state: &IppState,
    request: &ParsedIppRequest,
    document: &[u8],
) -> Result<Vec<u8>> {
    let id = state.next_job_id.fetch_add(1, Ordering::Relaxed);
    let name = request
        .get_string("job-name")
        .unwrap_or_else(|| format!("ipp-job-{id}"));
    let user = request
        .get_string("requesting-user-name")
        .unwrap_or_else(|| "unknown".to_string());
    let document_format = request
        .get_string("document-format")
        .unwrap_or_else(|| "application/octet-stream".to_string());

    {
        let mut jobs = state.jobs.lock().expect("IPP job mutex poisoned");
        jobs.insert(
            id,
            IppJobRecord {
                id,
                name: name.clone(),
                user: user.clone(),
                state: 5,
                document_format: document_format.clone(),
                saved_path: None,
            },
        );
    }

    let saved = state
        .storage
        .save_bytes(
            document,
            JobMetadata {
                protocol: "IPP".to_string(),
                user: Some(user.clone()),
                job_name: Some(name.clone()),
                document_format: Some(document_format.clone()),
                attributes: attributes_to_metadata(request),
                ..Default::default()
            },
        )
        .await?;

    {
        let mut jobs = state.jobs.lock().expect("IPP job mutex poisoned");
        if let Some(job) = jobs.get_mut(&id) {
            job.state = 9;
            job.saved_path = Some(saved.raw_path.display().to_string());
        }
    }

    (state.events)(ServerEvent::JobSaved {
        protocol: "IPP",
        path: saved.raw_path,
        bytes: saved.bytes,
    });

    Ok(job_response(request, id, &name, &user, 9, state.ipp_port))
}

fn create_job(state: &IppState, request: &ParsedIppRequest) -> Result<Vec<u8>> {
    let id = state.next_job_id.fetch_add(1, Ordering::Relaxed);
    let name = request
        .get_string("job-name")
        .unwrap_or_else(|| format!("ipp-job-{id}"));
    let user = request
        .get_string("requesting-user-name")
        .unwrap_or_else(|| "unknown".to_string());
    let document_format = request
        .get_string("document-format")
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut jobs = state.jobs.lock().expect("IPP job mutex poisoned");
    jobs.insert(
        id,
        IppJobRecord {
            id,
            name: name.clone(),
            user: user.clone(),
            state: 3,
            document_format,
            saved_path: None,
        },
    );

    Ok(job_response(request, id, &name, &user, 3, state.ipp_port))
}

async fn send_document(
    state: &IppState,
    request: &ParsedIppRequest,
    document: &[u8],
) -> Result<Vec<u8>> {
    let id = request
        .get_i32("job-id")
        .and_then(|id| u32::try_from(id).ok())
        .context("Send-Document requires job-id")?;

    let existing = {
        let jobs = state.jobs.lock().expect("IPP job mutex poisoned");
        jobs.get(&id).cloned()
    };

    let Some(job) = existing else {
        return Ok(not_found_response(request));
    };

    let document_format = request
        .get_string("document-format")
        .unwrap_or_else(|| job.document_format.clone());

    let saved = state
        .storage
        .save_bytes(
            document,
            JobMetadata {
                protocol: "IPP".to_string(),
                user: Some(job.user.clone()),
                job_name: Some(job.name.clone()),
                document_format: Some(document_format),
                attributes: attributes_to_metadata(request),
                ..Default::default()
            },
        )
        .await?;

    {
        let mut jobs = state.jobs.lock().expect("IPP job mutex poisoned");
        if let Some(job) = jobs.get_mut(&id) {
            job.state = 9;
            job.saved_path = Some(saved.raw_path.display().to_string());
        }
    }

    (state.events)(ServerEvent::JobSaved {
        protocol: "IPP",
        path: saved.raw_path,
        bytes: saved.bytes,
    });

    Ok(job_response(
        request,
        id,
        &job.name,
        &job.user,
        9,
        state.ipp_port,
    ))
}

fn get_job_attributes(state: &IppState, request: &ParsedIppRequest) -> Result<Vec<u8>> {
    let id = request
        .get_i32("job-id")
        .and_then(|id| u32::try_from(id).ok())
        .context("Get-Job-Attributes requires job-id")?;

    let jobs = state.jobs.lock().expect("IPP job mutex poisoned");
    let Some(job) = jobs.get(&id) else {
        return Ok(not_found_response(request));
    };

    Ok(job_response(
        request,
        job.id,
        &job.name,
        &job.user,
        job.state,
        state.ipp_port,
    ))
}

fn cancel_job(state: &IppState, request: &ParsedIppRequest) -> Result<Vec<u8>> {
    if let Some(id) = request
        .get_i32("job-id")
        .and_then(|id| u32::try_from(id).ok())
    {
        let mut jobs = state.jobs.lock().expect("IPP job mutex poisoned");
        if let Some(job) = jobs.get_mut(&id) {
            job.state = 7;
            return Ok(job_response(
                request,
                job.id,
                &job.name,
                &job.user,
                job.state,
                state.ipp_port,
            ));
        }
    }

    Ok(not_found_response(request))
}

fn get_jobs_response(_state: &IppState, request: &ParsedIppRequest) -> Vec<u8> {
    // Jobs are completed immediately after their document is persisted, so the
    // active queue is intentionally reported as empty.
    success_response(request)
}

fn success_response(request: &ParsedIppRequest) -> Vec<u8> {
    let mut encoder = IppEncoder::new(request.version(), STATUS_OK, request.request_id);
    encoder.operation_defaults();
    encoder.finish()
}

fn operation_not_supported_response(request: &ParsedIppRequest) -> Vec<u8> {
    let mut encoder = IppEncoder::new(
        request.version(),
        STATUS_SERVER_ERROR_OPERATION_NOT_SUPPORTED,
        request.request_id,
    );
    encoder.operation_defaults();
    encoder.attr_string(TAG_TEXT, "status-message", "IPP operation not supported");
    encoder.finish()
}

fn not_found_response(request: &ParsedIppRequest) -> Vec<u8> {
    let mut encoder = IppEncoder::new(
        request.version(),
        STATUS_CLIENT_ERROR_NOT_FOUND,
        request.request_id,
    );
    encoder.operation_defaults();
    encoder.attr_string(TAG_TEXT, "status-message", "job not found");
    encoder.finish()
}

fn job_response(
    request: &ParsedIppRequest,
    id: u32,
    name: &str,
    user: &str,
    state: i32,
    ipp_port: u16,
) -> Vec<u8> {
    let mut encoder = IppEncoder::new(request.version(), STATUS_OK, request.request_id);
    encoder.operation_defaults();
    encoder.begin_group(GROUP_JOB);
    encoder.attr_string(
        TAG_URI,
        "job-uri",
        &format!("ipp://127.0.0.1:{ipp_port}/jobs/{id}"),
    );
    encoder.attr_integer("job-id", id as i32);
    encoder.attr_string(TAG_NAME, "job-name", name);
    encoder.attr_string(TAG_NAME, "job-originating-user-name", user);
    encoder.attr_enum("job-state", state);
    encoder.attr_string(
        TAG_KEYWORD,
        "job-state-reasons",
        if state == 9 {
            "job-completed-successfully"
        } else if state == 7 {
            "job-canceled-by-user"
        } else {
            "none"
        },
    );
    encoder.attr_string(
        TAG_URI,
        "job-printer-uri",
        &format!("ipp://127.0.0.1:{ipp_port}/printers/virtual"),
    );
    encoder.finish()
}

fn printer_attributes_response(request: &ParsedIppRequest, ipp_port: u16) -> Vec<u8> {
    let uri = format!("ipp://127.0.0.1:{ipp_port}/printers/virtual");
    let mut encoder = IppEncoder::new(request.version(), STATUS_OK, request.request_id);
    encoder.operation_defaults();
    encoder.begin_group(GROUP_PRINTER);

    encoder.attr_string(TAG_URI, "printer-uri-supported", &uri);
    encoder.attr_strings(TAG_KEYWORD, "uri-authentication-supported", &["none"]);
    encoder.attr_strings(TAG_KEYWORD, "uri-security-supported", &["none"]);
    encoder.attr_string(TAG_NAME, "printer-name", "Virtual Print Sink");
    encoder.attr_string(
        TAG_TEXT,
        "printer-info",
        "Rust LPR/IPP file capture printer",
    );
    encoder.attr_string(TAG_TEXT, "printer-location", "localhost");
    encoder.attr_string(
        TAG_TEXT,
        "printer-make-and-model",
        "OpenAI Virtual Print Sink 0.1",
    );
    encoder.attr_string(TAG_TEXT, "printer-device-id", PRINTER_DEVICE_ID);
    encoder.attr_string(TAG_URI, "printer-uuid", PRINTER_UUID);

    encoder.attr_enum("printer-state", 3);
    encoder.attr_strings(TAG_KEYWORD, "printer-state-reasons", &["none"]);
    encoder.attr_boolean("printer-is-accepting-jobs", true);
    encoder.attr_integer("queued-job-count", 0);
    encoder.attr_integer("printer-config-change-time", 0);
    encoder.attr_integer("printer-up-time", 0);
    encoder.attr_integer("pages-per-minute", 1);
    encoder.attr_integer("pages-per-minute-color", 1);
    encoder.attr_boolean("color-supported", true);

    encoder.attr_strings(TAG_KEYWORD, "ipp-versions-supported", &["1.1", "2.0"]);
    encoder.attr_strings(TAG_KEYWORD, "ipp-features-supported", &["ipp-everywhere"]);
    encoder.attr_strings(
        TAG_KEYWORD,
        "printer-get-attributes-supported",
        &["document-format"],
    );
    encoder.attr_enums(
        "operations-supported",
        &[
            OP_PRINT_JOB as i32,
            OP_VALIDATE_JOB as i32,
            OP_CREATE_JOB as i32,
            OP_SEND_DOCUMENT as i32,
            OP_CANCEL_JOB as i32,
            OP_GET_JOB_ATTRIBUTES as i32,
            OP_GET_JOBS as i32,
            OP_GET_PRINTER_ATTRIBUTES as i32,
        ],
    );

    encoder.attr_string(TAG_CHARSET, "charset-configured", "utf-8");
    encoder.attr_strings(TAG_CHARSET, "charset-supported", &["utf-8"]);
    encoder.attr_string(TAG_NATURAL_LANGUAGE, "natural-language-configured", "en");
    encoder.attr_strings(
        TAG_NATURAL_LANGUAGE,
        "generated-natural-language-supported",
        &["en"],
    );

    encoder.attr_string(
        TAG_MIME,
        "document-format-default",
        "application/octet-stream",
    );
    encoder.attr_string(TAG_MIME, "document-format-preferred", "application/pdf");
    encoder.attr_strings(
        TAG_MIME,
        "document-format-supported",
        &[
            "application/octet-stream",
            "application/pdf",
            "application/postscript",
            "application/vnd.hp-pcl",
            "image/jpeg",
            "image/pwg-raster",
            "image/urf",
            "text/plain",
        ],
    );
    encoder.attr_strings(TAG_KEYWORD, "compression-supported", &["none"]);
    encoder.attr_string(TAG_KEYWORD, "pdl-override-supported", "not-attempted");
    encoder.attr_boolean("multiple-document-jobs-supported", false);
    encoder.attr_boolean("job-ids-supported", true);
    encoder.attr_strings(
        TAG_KEYWORD,
        "which-jobs-supported",
        &[
            "completed",
            "not-completed",
            "aborted",
            "all",
            "canceled",
            "pending",
            "pending-held",
            "processing",
            "processing-stopped",
        ],
    );

    encoder.attr_strings(
        TAG_KEYWORD,
        "job-creation-attributes-supported",
        &[
            "copies",
            "document-format",
            "job-name",
            "media",
            "media-col",
            "orientation-requested",
            "page-ranges",
            "print-color-mode",
            "print-quality",
            "printer-resolution",
            "requesting-user-name",
            "sides",
        ],
    );

    encoder.attr_integer("copies-default", 1);
    encoder.attr_range("copies-supported", 1, 999);
    encoder.attr_integer("job-priority-default", 50);
    encoder.attr_integer("job-priority-supported", 100);
    encoder.attr_range(
        "job-k-octets-supported",
        0,
        (MAX_IPP_BODY_BYTES / 1024) as i32,
    );
    encoder.attr_enum("finishings-default", 3);
    encoder.attr_enums("finishings-supported", &[3]);

    encoder.attr_string(TAG_KEYWORD, "media-default", "iso_a4_210x297mm");
    encoder.attr_strings(
        TAG_KEYWORD,
        "media-supported",
        &["iso_a4_210x297mm", "na_letter_8.5x11in"],
    );
    encoder.attr_strings(
        TAG_KEYWORD,
        "media-ready",
        &["iso_a4_210x297mm", "na_letter_8.5x11in"],
    );
    encoder.attr_strings(
        TAG_KEYWORD,
        "media-col-supported",
        &["media-size", "media-source", "media-type"],
    );
    encoder.attr_string(TAG_KEYWORD, "media-source-default", "auto");
    encoder.attr_strings(TAG_KEYWORD, "media-source-supported", &["auto"]);
    encoder.attr_string(TAG_KEYWORD, "media-type-default", "stationery");
    encoder.attr_strings(TAG_KEYWORD, "media-type-supported", &["stationery"]);

    encoder.attr_string(TAG_KEYWORD, "sides-default", "one-sided");
    encoder.attr_strings(
        TAG_KEYWORD,
        "sides-supported",
        &["one-sided", "two-sided-long-edge", "two-sided-short-edge"],
    );

    encoder.attr_enum("orientation-requested-default", 3);
    encoder.attr_enums("orientation-requested-supported", &[3, 4, 5, 6]);

    encoder.attr_enum("print-quality-default", 4);
    encoder.attr_enums("print-quality-supported", &[3, 4, 5]);

    encoder.attr_string(TAG_KEYWORD, "print-color-mode-default", "color");
    encoder.attr_strings(
        TAG_KEYWORD,
        "print-color-mode-supported",
        &["monochrome", "color"],
    );
    encoder.attr_strings(
        TAG_KEYWORD,
        "print-scaling-supported",
        &["auto", "auto-fit", "fill", "fit", "none"],
    );
    encoder.attr_string(TAG_KEYWORD, "print-scaling-default", "auto");
    encoder.attr_string(TAG_KEYWORD, "print-content-optimize-default", "auto");
    encoder.attr_strings(
        TAG_KEYWORD,
        "print-content-optimize-supported",
        &["auto", "graphic", "photo", "text", "text-and-graphic"],
    );

    encoder.attr_resolution("printer-resolution-default", 600, 600, 3);
    encoder.attr_resolutions(
        "printer-resolution-supported",
        &[(300, 300, 3), (600, 600, 3)],
    );
    encoder.attr_resolutions(
        "pwg-raster-document-resolution-supported",
        &[(300, 300, 3), (600, 600, 3)],
    );
    encoder.attr_strings(
        TAG_KEYWORD,
        "pwg-raster-document-type-supported",
        &["black_1", "sgray_8", "srgb_8"],
    );
    encoder.attr_strings(
        TAG_KEYWORD,
        "urf-supported",
        &["CP1", "IS1", "MT1-2-3-4-5", "RS300-600", "SRGB24", "W8"],
    );

    encoder.attr_boolean("page-ranges-supported", true);
    encoder.attr_integer("number-up-default", 1);
    encoder.attr_integers("number-up-supported", &[1, 2, 4, 6, 9, 16]);
    encoder.attr_string(TAG_KEYWORD, "output-bin-default", "face-down");
    encoder.attr_strings(TAG_KEYWORD, "output-bin-supported", &["face-down"]);

    encoder.finish()
}

fn attributes_to_metadata(request: &ParsedIppRequest) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, attr) in &request.attributes {
        let values = attr
            .values
            .iter()
            .map(|value| render_value(attr.tag, value))
            .collect::<Vec<_>>()
            .join(", ");
        out.insert(format!("g{:02x}:{name}", attr.group), values);
    }
    out
}

fn render_value(tag: u8, value: &[u8]) -> String {
    match tag {
        TAG_INTEGER | TAG_ENUM if value.len() == 4 => {
            i32::from_be_bytes([value[0], value[1], value[2], value[3]]).to_string()
        }
        TAG_BOOLEAN if value.len() == 1 => (value[0] != 0).to_string(),
        TAG_TEXT | TAG_NAME | TAG_KEYWORD | TAG_URI | TAG_CHARSET | TAG_NATURAL_LANGUAGE
        | TAG_MIME => String::from_utf8_lossy(value).into_owned(),
        _ => value
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(""),
    }
}

fn read_u16(body: &[u8], index: &mut usize) -> Result<u16> {
    if *index + 2 > body.len() {
        bail!("unexpected end of IPP message");
    }
    let value = u16::from_be_bytes([body[*index], body[*index + 1]]);
    *index += 2;
    Ok(value)
}

fn ipp_http_response(bytes: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/ipp"));
    response
}

struct IppEncoder {
    bytes: Vec<u8>,
    group_started: bool,
}

impl IppEncoder {
    fn new(version: (u8, u8), status: u16, request_id: u32) -> Self {
        let mut bytes = Vec::with_capacity(1024);
        bytes.push(version.0);
        bytes.push(version.1);
        bytes.extend_from_slice(&status.to_be_bytes());
        bytes.extend_from_slice(&request_id.to_be_bytes());
        Self {
            bytes,
            group_started: false,
        }
    }

    fn operation_defaults(&mut self) {
        self.begin_group(GROUP_OPERATION);
        self.attr_string(TAG_CHARSET, "attributes-charset", "utf-8");
        self.attr_string(TAG_NATURAL_LANGUAGE, "attributes-natural-language", "en");
    }

    fn begin_group(&mut self, group: u8) {
        self.bytes.push(group);
        self.group_started = true;
    }

    fn attr_raw(&mut self, tag: u8, name: &str, value: &[u8]) {
        debug_assert!(self.group_started);
        self.bytes.push(tag);
        self.bytes
            .extend_from_slice(&(name.len() as u16).to_be_bytes());
        self.bytes.extend_from_slice(name.as_bytes());
        self.bytes
            .extend_from_slice(&(value.len() as u16).to_be_bytes());
        self.bytes.extend_from_slice(value);
    }

    fn attr_raw_continuation(&mut self, tag: u8, value: &[u8]) {
        self.bytes.push(tag);
        self.bytes.extend_from_slice(&0u16.to_be_bytes());
        self.bytes
            .extend_from_slice(&(value.len() as u16).to_be_bytes());
        self.bytes.extend_from_slice(value);
    }

    fn attr_string(&mut self, tag: u8, name: &str, value: &str) {
        self.attr_raw(tag, name, value.as_bytes());
    }

    fn attr_strings(&mut self, tag: u8, name: &str, values: &[&str]) {
        if let Some((first, rest)) = values.split_first() {
            self.attr_raw(tag, name, first.as_bytes());
            for value in rest {
                self.attr_raw_continuation(tag, value.as_bytes());
            }
        }
    }

    fn attr_integer(&mut self, name: &str, value: i32) {
        self.attr_raw(TAG_INTEGER, name, &value.to_be_bytes());
    }

    fn attr_integers(&mut self, name: &str, values: &[i32]) {
        if let Some((first, rest)) = values.split_first() {
            self.attr_raw(TAG_INTEGER, name, &first.to_be_bytes());
            for value in rest {
                self.attr_raw_continuation(TAG_INTEGER, &value.to_be_bytes());
            }
        }
    }

    fn attr_enum(&mut self, name: &str, value: i32) {
        self.attr_raw(TAG_ENUM, name, &value.to_be_bytes());
    }

    fn attr_enums(&mut self, name: &str, values: &[i32]) {
        if let Some((first, rest)) = values.split_first() {
            self.attr_raw(TAG_ENUM, name, &first.to_be_bytes());
            for value in rest {
                self.attr_raw_continuation(TAG_ENUM, &value.to_be_bytes());
            }
        }
    }

    fn attr_boolean(&mut self, name: &str, value: bool) {
        self.attr_raw(TAG_BOOLEAN, name, &[u8::from(value)]);
    }

    fn attr_range(&mut self, name: &str, min: i32, max: i32) {
        let mut data = [0u8; 8];
        data[..4].copy_from_slice(&min.to_be_bytes());
        data[4..].copy_from_slice(&max.to_be_bytes());
        self.attr_raw(TAG_RANGE, name, &data);
    }

    fn attr_resolution(&mut self, name: &str, x: i32, y: i32, units: u8) {
        let mut data = [0u8; 9];
        data[..4].copy_from_slice(&x.to_be_bytes());
        data[4..8].copy_from_slice(&y.to_be_bytes());
        data[8] = units;
        self.attr_raw(TAG_RESOLUTION, name, &data);
    }

    fn attr_resolutions(&mut self, name: &str, values: &[(i32, i32, u8)]) {
        if let Some(((x, y, units), rest)) = values.split_first() {
            let first = resolution_bytes(*x, *y, *units);
            self.attr_raw(TAG_RESOLUTION, name, &first);
            for (x, y, units) in rest {
                let data = resolution_bytes(*x, *y, *units);
                self.attr_raw_continuation(TAG_RESOLUTION, &data);
            }
        }
    }

    fn finish(mut self) -> Vec<u8> {
        self.bytes.push(GROUP_END);
        self.bytes
    }
}

fn resolution_bytes(x: i32, y: i32, units: u8) -> [u8; 9] {
    let mut data = [0u8; 9];
    data[..4].copy_from_slice(&x.to_be_bytes());
    data[4..8].copy_from_slice(&y.to_be_bytes());
    data[8] = units;
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_get_printer_attributes_request() {
        let body = vec![
            0x01, 0x01, 0x00, 0x0b, 0, 0, 0, 1, 0x01, 0x47, 0, 18, b'a', b't', b't', b'r', b'i',
            b'b', b'u', b't', b'e', b's', b'-', b'c', b'h', b'a', b'r', b's', b'e', b't', 0, 5,
            b'u', b't', b'f', b'-', b'8', 0x03,
        ];
        let req = ParsedIppRequest::parse(&body).unwrap();
        assert_eq!(req.operation, OP_GET_PRINTER_ATTRIBUTES);
        assert_eq!(
            req.get_string("attributes-charset").as_deref(),
            Some("utf-8")
        );
        assert_eq!(req.document_offset, body.len());
    }

    #[test]
    fn response_has_matching_request_id() {
        let request = ParsedIppRequest {
            version_major: 1,
            version_minor: 1,
            operation: OP_GET_PRINTER_ATTRIBUTES,
            request_id: 0x01020304,
            attributes: BTreeMap::new(),
            document_offset: 0,
        };
        let response = printer_attributes_response(&request, 8631);
        assert_eq!(&response[4..8], &[1, 2, 3, 4]);
        assert_eq!(response.last(), Some(&GROUP_END));
    }

    #[test]
    fn printer_attributes_include_stable_psa_identity() {
        let request = ParsedIppRequest {
            version_major: 2,
            version_minor: 0,
            operation: OP_GET_PRINTER_ATTRIBUTES,
            request_id: 7,
            attributes: BTreeMap::new(),
            document_offset: 0,
        };

        let response = printer_attributes_response(&request, 8631);
        let parsed = ParsedIppRequest::parse(&response).unwrap();

        assert_eq!(
            parsed.get_string("printer-device-id").as_deref(),
            Some(PRINTER_DEVICE_ID)
        );
        assert_eq!(
            parsed.get_string("printer-uuid").as_deref(),
            Some(PRINTER_UUID)
        );
        assert_eq!(
            parsed.get_string("document-format-preferred").as_deref(),
            Some("application/pdf")
        );
        assert_eq!(parsed.get_i32("job-priority-supported"), Some(100));
        assert_eq!(
            parsed.get_string("print-scaling-default").as_deref(),
            Some("auto")
        );
        assert_eq!(
            parsed
                .attributes
                .get("printer-device-id")
                .map(|attribute| attribute.tag),
            Some(TAG_TEXT)
        );
    }
}
