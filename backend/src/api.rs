//! HTTP API for Ryu Clips (`/api/clips/*`).
//!
//! Surfaces the record/ingest/list/frame/stream flow, proxying each call to the
//! Shadow sidecar over loopback. See the crate root for the transport split and
//! the host-inversion rationale.
//!
//! The router is built with its own state ([`ClipsCtx`]) inside this crate so it
//! returns a state-less, mergeable `Router<()>`. Routes are declared relative to
//! `/api/clips` (Core nests this service at that prefix behind the Clips-App
//! gate), while the OpenAPI annotations keep the full external paths. The static
//! collection route is registered before the `:id` routes (convention).

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{shadow_base, SharedClipsHost, ShadowAuth};

/// Router state for the clips HTTP surface: the shared HTTP client (for the Shadow
/// proxy) and the injected [`crate::ClipsHost`] (yt-dlp ingest + Space filing).
/// Kept as a named type so the router bakes a concrete state and returns
/// `Router<()>`.
#[derive(Clone)]
pub struct ClipsCtx {
    pub client: reqwest::Client,
    pub host: SharedClipsHost,
}

impl ClipsCtx {
    pub fn new(client: reqwest::Client, host: SharedClipsHost) -> Self {
        Self { client, host }
    }
}

/// Build the `/api/clips/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/clips` behind the App gate.
pub fn routes(ctx: ClipsCtx) -> Router<()> {
    Router::new()
        .route("/", get(list_clips))
        .route("/ingest", post(ingest))
        .route("/sources", get(get_sources))
        .route("/recent-activity", get(recent_activity))
        .route("/start", post(start_clip))
        .route("/:id/stop", post(stop_clip))
        .route("/:id/pause", post(pause_clip))
        .route("/:id/resume", post(resume_clip))
        .route("/:id/context", get(get_context))
        .route("/:id/frame", get(get_frame))
        .route("/:id/file", get(get_file))
        .route("/:id/diagnostics", post(post_diagnostics))
        .with_state(ctx)
}

/// The OpenAPI sub-document for the clips surface, merged into Core's spec when
/// the `clips` feature is enabled.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <ClipsApiDoc as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    get_context,
    get_file,
    get_frame,
    get_sources,
    ingest,
    list_clips,
    pause_clip,
    post_diagnostics,
    recent_activity,
    resume_clip,
    start_clip,
    stop_clip,
))]
struct ClipsApiDoc;

/// How long to wait for Shadow before declaring it unavailable. Clips involve
/// ffmpeg on the Shadow side, so this is more generous than a plain query.
const SHADOW_TIMEOUT_SECS: u64 = 15;

/// How long to wait for a clip ingest (yt-dlp download + Shadow ffmpeg passes).
/// Far more generous than the plain-proxy timeout — a full video download plus
/// scene-detect extraction can run for minutes.
const INGEST_TIMEOUT_SECS: u64 = 600;

/// Build the fail-soft body returned when Shadow can't be reached.
fn unavailable(reason: impl Into<String>) -> Value {
    json!({ "available": false, "reason": reason.into() })
}

/// A clip id is a single path segment that we interpolate directly into a
/// loopback Shadow URL (`{shadow_base}/clips/{id}/...`). Axum percent-decodes
/// `Path<String>` before we see it, so an encoded `/` or `..` would let a caller
/// escape the intended `/clips/<id>/...` shape and reach arbitrary Shadow routes
/// (path traversal into the loopback service). Reject any id that could do so.
fn clip_id_is_safe(id: &str) -> bool {
    !id.is_empty() && !id.contains('/') && !id.contains("..")
}

/// Rewrite a manifest's `framesEndpoint` from the Shadow-relative `/clips/{id}/frame`
/// to the Core-served `/api/clips/{id}/frame` so the desktop hits Core, not Shadow.
fn rewrite_frames_endpoint(body: &mut Value, id: &str) {
    if let Some(obj) = body.as_object_mut() {
        obj.insert(
            "framesEndpoint".to_string(),
            json!(format!("/api/clips/{id}/frame")),
        );
    }
}

/// Body for `POST /api/clips/ingest` from the desktop `ingestClip`.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct IngestBody {
    /// A URL (`http`/`https`) to download, or a local video file path.
    pub source: String,
    /// Detail mode: `transcript` | `efficient` | `balanced` | `tokenBurner`.
    pub detail: Option<String>,
    /// Optional trim (ms).
    pub start: Option<u64>,
    pub end: Option<u64>,
}

impl Default for IngestBody {
    fn default() -> Self {
        Self {
            source: String::new(),
            detail: None,
            start: None,
            end: None,
        }
    }
}

/// The local video extensions accepted for a local-file ingest.
const LOCAL_VIDEO_EXTS: &[&str] = &["mp4", "mov", "mkv", "webm"];

/// Local-file ingest is restricted to an allowlist of user media folders so an
/// authenticated caller cannot use the endpoint to read arbitrary files on disk:
/// `$HOME/{Movies,Downloads,Desktop}` plus any colon-separated extra roots in
/// `RYU_CLIPS_ALLOWED_DIRS`. Bases are canonicalized before the prefix check so
/// symlinked paths (e.g. macOS `/var` → `/private/var`) compare correctly.
fn local_ingest_allowed(canonical: &std::path::Path) -> bool {
    let mut bases: Vec<std::path::PathBuf> = Vec::new();
    if let Some(home) = std::env::var_os("HOME") {
        let home = std::path::PathBuf::from(home);
        for sub in ["Movies", "Downloads", "Desktop"] {
            bases.push(home.join(sub));
        }
    }
    if let Some(extra) = std::env::var_os("RYU_CLIPS_ALLOWED_DIRS") {
        bases.extend(std::env::split_paths(&extra));
    }
    bases
        .into_iter()
        .filter_map(|b| b.canonicalize().ok())
        .any(|b| canonical.starts_with(&b))
}

/// POST /api/clips/ingest — turn a watched URL or a local video file into a clip
/// bundle indistinguishable from a recorded one. Core resolves the source (yt-dlp
/// for URLs → local mp4 + best-effort captions; validation for local files), then
/// hands the local path to Shadow's `/clips/ingest`, which owns the sensor half
/// (normalize + budgeted keyframe extraction + transcript + bundle). Core rewrites
/// `framesEndpoint` so the desktop hits Core, not Shadow.
///
/// Placement (CLAUDE.md §1): binary management + ingest orchestration + bundle
/// build are "what runs" → Core/Shadow. Routing the Whisper model call is "what
/// is measured/paid" → the Gateway (Shadow selects `sttEngine`; Core only emits
/// slot headers in `voice::transcribe_wav`).
#[utoipa::path(
    post,
    path = "/api/clips/ingest",
    tag = "Clips",
    summary = "turn a watched URL or a local video file into a clip",
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn ingest(
    State(ctx): State<ClipsCtx>,
    Json(body): Json<IngestBody>,
) -> (StatusCode, Json<Value>) {
    let source = body.source.trim().to_string();
    if source.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing `source` (a video URL or local file path)" })),
        );
    }

    let is_url = source.starts_with("http://") || source.starts_with("https://");

    // Resolve the source to a local path (+ optional captions). To keep a SINGLE
    // trim: a URL with a fully-bounded `[start, end)` is trimmed at download by
    // yt-dlp (bandwidth saver) and its start/end are then NOT forwarded to Shadow;
    // every other case (local file, or a one-sided URL trim) downloads/passes the
    // whole video and lets Shadow own the trim via start/end.
    let (video_path, captions, caption_segments, fwd_start, fwd_end) = if is_url {
        if let Err(e) = ctx.host.ensure_ytdlp().await {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "error": format!("could not install the yt-dlp downloader: {e}")
                })),
            );
        }

        let work_dir = ctx
            .host
            .tmp_dir()
            .join(format!("clip-ingest-{}", uuid::Uuid::new_v4().simple()));

        let trim_at_download = body.start.is_some() && body.end.is_some();
        let (dl_start, dl_end) = if trim_at_download {
            (body.start, body.end)
        } else {
            (None, None)
        };

        match ctx
            .host
            .download_video(&source, &work_dir, dl_start, dl_end)
            .await
        {
            Ok(dl) => {
                let (fwd_start, fwd_end) = if trim_at_download {
                    (None, None)
                } else {
                    (body.start, body.end)
                };
                let caption_segments: Vec<Value> = dl
                    .caption_segments
                    .iter()
                    .map(|c| json!({ "startMs": c.start_ms, "endMs": c.end_ms, "text": c.text }))
                    .collect();
                (
                    dl.video.to_string_lossy().to_string(),
                    dl.captions,
                    caption_segments,
                    fwd_start,
                    fwd_end,
                )
            }
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "error": format!("downloading the video failed: {e}") })),
                );
            }
        }
    } else {
        let path = std::path::PathBuf::from(&source);
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("local file not found: {source}") })),
                );
            }
        };
        if !canonical.is_file() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("not a file: {source}") })),
            );
        }
        if !local_ingest_allowed(&canonical) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "local file is outside the allowed ingest folders (Movies, Downloads, Desktop, or RYU_CLIPS_ALLOWED_DIRS)"
                })),
            );
        }
        let ext_ok = canonical
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| LOCAL_VIDEO_EXTS.contains(&e.to_ascii_lowercase().as_str()))
            .unwrap_or(false);
        if !ext_ok {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "unsupported video type (expected .mp4, .mov, .mkv, or .webm)"
                })),
            );
        }
        (
            canonical.to_string_lossy().to_string(),
            None,
            Vec::new(),
            body.start,
            body.end,
        )
    };

    // STT engine for the (captions-absent) transcript path: a swappable default,
    // "gateway" (Gateway-routed Whisper, default Groq) unless re-pointed.
    let stt_engine = std::env::var("RYU_CLIP_STT_ENGINE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "gateway".to_string());

    let detail = body
        .detail
        .clone()
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
        .unwrap_or_else(|| "balanced".to_string());

    let payload = json!({
        "videoPath": video_path,
        "captions": captions,
        "captionSegments": caption_segments,
        "detail": detail,
        "start": fwd_start,
        "end": fwd_end,
        "sttEngine": stt_engine,
    });

    let url = format!("{}/clips/ingest", shadow_base());
    let resp = ctx
        .client
        .post(&url)
        .shadow_auth()
        .json(&payload)
        .timeout(std::time::Duration::from_secs(INGEST_TIMEOUT_SECS))
        .send()
        .await;

    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(mut b) => {
                    if let Some(id) = b.get("id").and_then(Value::as_str).map(String::from) {
                        rewrite_frames_endpoint(&mut b, &id);
                    }
                    // A finished ingest (2xx) is auto-filed into the "Clips" space,
                    // fire-and-forget so it never delays or alters this response.
                    if status.is_success() {
                        tokio::spawn(file_clip_into_space(ctx.clone(), b.clone()));
                    }
                    (status, Json(b))
                }
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        // Fail-soft when Shadow is down, like the other proxy handlers.
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// GET /api/clips — list clips (proxied from Shadow).
#[utoipa::path(
    get,
    path = "/api/clips",
    tag = "Clips",
    summary = "list clips (proxied from Shadow).",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_clips(State(ctx): State<ClipsCtx>) -> Json<Value> {
    let url = format!("{}/clips", shadow_base());
    let resp = ctx
        .client
        .get(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => match r.json::<Value>().await {
            Ok(body) => Json(body),
            Err(e) => Json(unavailable(format!(
                "Shadow returned an invalid response: {e}"
            ))),
        },
        Ok(r) => Json(unavailable(format!("Shadow returned HTTP {}", r.status()))),
        Err(e) => Json(unavailable(format!("Shadow is not reachable: {e}"))),
    }
}

/// POST /api/clips/start — start a clip (proxied from Shadow).
#[utoipa::path(
    post,
    path = "/api/clips/start",
    tag = "Clips",
    summary = "start a clip (proxied from Shadow).",
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn start_clip(
    State(ctx): State<ClipsCtx>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let url = format!("{}/clips/start", shadow_base());
    let resp = ctx
        .client
        .post(&url)
        .shadow_auth()
        .json(&body)
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(body) => (status, Json(body)),
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// POST /api/clips/:id/stop — finalize a clip; rewrites `framesEndpoint`.
#[utoipa::path(
    post,
    path = "/api/clips/{id}/stop",
    tag = "Clips",
    summary = "finalize a clip; rewrites `framesEndpoint`.",
    params(("id" = String, Path)),
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn stop_clip(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    if !clip_id_is_safe(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid clip id" })),
        );
    }
    let url = format!("{}/clips/{id}/stop", shadow_base());
    let resp = ctx
        .client
        .post(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(mut body) => {
                    rewrite_frames_endpoint(&mut body, &id);
                    // A finalized clip (2xx) is auto-filed into the "Clips" space,
                    // fire-and-forget so it never delays or alters this response.
                    if status.is_success() {
                        tokio::spawn(file_clip_into_space(ctx.clone(), body.clone()));
                    }
                    (status, Json(body))
                }
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// POST /api/clips/:id/pause — pause the in-progress clip (proxied from Shadow).
#[utoipa::path(
    post,
    path = "/api/clips/{id}/pause",
    tag = "Clips",
    summary = "pause the in-progress clip (proxied from Shadow).",
    params(("id" = String, Path)),
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn pause_clip(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    if !clip_id_is_safe(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid clip id" })),
        );
    }
    proxy_clip_post(&ctx, &format!("clips/{id}/pause")).await
}

/// POST /api/clips/:id/resume — resume a paused clip (proxied from Shadow).
#[utoipa::path(
    post,
    path = "/api/clips/{id}/resume",
    tag = "Clips",
    summary = "resume a paused clip (proxied from Shadow).",
    params(("id" = String, Path)),
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn resume_clip(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    if !clip_id_is_safe(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid clip id" })),
        );
    }
    proxy_clip_post(&ctx, &format!("clips/{id}/resume")).await
}

/// Shared bodyless POST proxy to a Shadow `clips/*` path (pause/resume).
async fn proxy_clip_post(ctx: &ClipsCtx, path: &str) -> (StatusCode, Json<Value>) {
    let url = format!("{}/{path}", shadow_base());
    let resp = ctx
        .client
        .post(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(body) => (status, Json(body)),
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// GET /api/clips/sources — the displays + windows a clip can capture from
/// (proxied from Shadow). Fail-soft like `list_clips`.
#[utoipa::path(
    get,
    path = "/api/clips/sources",
    tag = "Clips",
    summary = "the displays + windows a clip can capture from",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_sources(State(ctx): State<ClipsCtx>) -> Json<Value> {
    let url = format!("{}/clips/sources", shadow_base());
    let resp = ctx
        .client
        .get(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => match r.json::<Value>().await {
            Ok(body) => Json(body),
            Err(e) => Json(unavailable(format!(
                "Shadow returned an invalid response: {e}"
            ))),
        },
        Ok(r) => Json(unavailable(format!("Shadow returned HTTP {}", r.status()))),
        Err(e) => Json(unavailable(format!("Shadow is not reachable: {e}"))),
    }
}

/// GET /api/clips/:id/context — the clip manifest; rewrites `framesEndpoint`.
#[utoipa::path(
    get,
    path = "/api/clips/{id}/context",
    tag = "Clips",
    summary = "the clip manifest; rewrites `framesEndpoint`.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_context(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    if !clip_id_is_safe(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid clip id" })),
        );
    }
    let url = format!("{}/clips/{id}/context", shadow_base());
    let resp = ctx
        .client
        .get(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(mut body) => {
                    rewrite_frames_endpoint(&mut body, &id);
                    (status, Json(body))
                }
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// Query for GET /api/clips/:id/frame.
#[derive(Debug, Deserialize)]
pub struct FrameQuery {
    #[serde(rename = "atMs", default)]
    pub at_ms: u64,
}

/// GET /api/clips/:id/frame?atMs= — stream a JPEG frame from Shadow.
#[utoipa::path(
    get,
    path = "/api/clips/{id}/frame",
    tag = "Clips",
    summary = "stream a JPEG frame from Shadow.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_frame(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
    Query(q): Query<FrameQuery>,
) -> Response {
    if !clip_id_is_safe(&id) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let url = format!("{}/clips/{id}/frame", shadow_base());
    proxy_bytes(
        ctx.client
            .get(&url)
            .shadow_auth()
            .query(&[("atMs", q.at_ms)])
            .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS)),
        "image/jpeg",
    )
    .await
}

/// GET /api/clips/:id/file — stream the clip.mp4 bytes from Shadow.
#[utoipa::path(
    get,
    path = "/api/clips/{id}/file",
    tag = "Clips",
    summary = "stream the clip.mp4 bytes from Shadow.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_file(State(ctx): State<ClipsCtx>, Path(id): Path<String>) -> Response {
    if !clip_id_is_safe(&id) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let url = format!("{}/clips/{id}/file", shadow_base());
    proxy_bytes(
        ctx.client
            .get(&url)
            .shadow_auth()
            .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS)),
        "video/mp4",
    )
    .await
}

/// POST /api/clips/:id/diagnostics — append diagnostics (proxied from Shadow).
#[utoipa::path(
    post,
    path = "/api/clips/{id}/diagnostics",
    tag = "Clips",
    summary = "append diagnostics (proxied from Shadow).",
    params(("id" = String, Path)),
    request_body = serde_json::Value,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn post_diagnostics(
    State(ctx): State<ClipsCtx>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !clip_id_is_safe(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid clip id" })),
        );
    }
    let url = format!("{}/clips/{id}/diagnostics", shadow_base());
    let resp = ctx
        .client
        .post(&url)
        .shadow_auth()
        .json(&body)
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::OK);
            match r.json::<Value>().await {
                Ok(body) => (status, Json(body)),
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    Json(unavailable(format!(
                        "Shadow returned an invalid response: {e}"
                    ))),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// Query for GET /api/clips/recent-activity.
#[derive(Debug, Deserialize)]
pub struct RecentActivityQuery {
    #[serde(default)]
    pub minutes: Option<u32>,
}

/// GET /api/clips/recent-activity?minutes=<n> — proxy Shadow's ephemeral
/// "last N minutes" keyframe bundle straight through (nothing persisted). Core
/// only clamps `minutes` to 1..=15 (default 3) and passes the JSON unchanged.
/// Fail-soft like the other clips proxies.
#[utoipa::path(
    get,
    path = "/api/clips/recent-activity",
    tag = "Clips",
    summary = "proxy Shadow's ephemeral",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn recent_activity(
    State(ctx): State<ClipsCtx>,
    Query(q): Query<RecentActivityQuery>,
) -> (StatusCode, Json<Value>) {
    let minutes = q.minutes.unwrap_or(3).clamp(1, 15);
    let url = format!("{}/activity/recent?minutes={minutes}", shadow_base());
    let resp = ctx
        .client
        .get(&url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => match r.json::<Value>().await {
            Ok(body) => (StatusCode::OK, Json(body)),
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                Json(unavailable(format!(
                    "Shadow returned an invalid response: {e}"
                ))),
            ),
        },
        Ok(r) => (
            StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(unavailable(format!("Shadow returned HTTP {}", r.status()))),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(unavailable(format!("Shadow is not reachable: {e}"))),
        ),
    }
}

/// Best-effort: file a just-finished clip into the "Clips" system Space. Fetches
/// the muxed mp4 from Shadow (the manifest `video` is a Shadow-internal relative
/// path, so Core cannot read it off disk), builds a short markdown summary, and
/// hands both to the host's [`crate::ClipsHost::store_clip`] (the Space filing is
/// kernel machinery). Fail-soft: this NEVER affects the clip HTTP response. Spawn
/// it, don't await it.
async fn file_clip_into_space(ctx: ClipsCtx, bundle: Value) {
    let Some(id) = bundle.get("id").and_then(Value::as_str) else {
        return;
    };
    let id = id.to_string();
    let title = bundle
        .get("title")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("Clip")
        .to_string();
    let duration_ms = bundle
        .get("durationMs")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    // The mp4 blob — bytes come from Shadow over HTTP (the manifest `video` is a
    // Shadow-internal relative path, so Core cannot read it off disk).
    let file_url = format!("{}/clips/{id}/file", shadow_base());
    let mp4: Option<Vec<u8>> = match ctx
        .client
        .get(&file_url)
        .shadow_auth()
        .timeout(std::time::Duration::from_secs(SHADOW_TIMEOUT_SECS))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.bytes().await {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(e) => {
                tracing::warn!("clips auto-file: reading mp4 bytes failed: {e}");
                None
            }
        },
        Ok(r) => {
            tracing::warn!("clips auto-file: Shadow /file returned HTTP {}", r.status());
            None
        }
        Err(e) => {
            tracing::warn!("clips auto-file: Shadow /file unreachable: {e}");
            None
        }
    };

    let summary = build_clip_summary_md(&title, duration_ms, &bundle);
    ctx.host.store_clip(&title, mp4, &summary).await;
}

/// Render a compact markdown summary from a finalized clip manifest.
fn build_clip_summary_md(title: &str, duration_ms: u64, bundle: &Value) -> String {
    let secs = duration_ms / 1000;
    let mm = secs / 60;
    let ss = secs % 60;
    let mut md = format!("# {title}\n\n- Duration: {mm:02}:{ss:02}\n");
    if let Some(w) = bundle.get("scanWarning").and_then(Value::as_str) {
        md.push_str(&format!("- Coverage: {w}\n"));
    }
    if let Some(moments) = bundle.get("recommendedMoments").and_then(Value::as_array) {
        if !moments.is_empty() {
            md.push_str("\n## Highlights\n");
            for m in moments {
                let at = m.get("atMs").and_then(Value::as_u64).unwrap_or(0) / 1000;
                let reason = m.get("reason").and_then(Value::as_str).unwrap_or("");
                md.push_str(&format!("- {at:02}s: {reason}\n"));
            }
        }
    }
    md
}

/// Stream a binary body from Shadow, forwarding its Content-Type (falling back to
/// `default_ct`). A transport failure or non-2xx becomes `502 Bad Gateway`.
async fn proxy_bytes(request: reqwest::RequestBuilder, default_ct: &str) -> Response {
    let resp = match request.send().await {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };
    if !resp.status().is_success() {
        return StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY)
            .into_response();
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(default_ct)
        .to_string();
    match resp.bytes().await {
        Ok(bytes) => ([(header::CONTENT_TYPE, content_type)], bytes.to_vec()).into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_id_is_safe_rejects_traversal_and_separators() {
        // Ordinary ids pass.
        assert!(clip_id_is_safe("abc123"));
        assert!(clip_id_is_safe("clip-2026-07-16_09-30"));
        // Empty is rejected (would collapse the URL path).
        assert!(!clip_id_is_safe(""));
        // A raw or (percent-)decoded slash would escape the `/clips/<id>/...` shape.
        assert!(!clip_id_is_safe("a/b"));
        assert!(!clip_id_is_safe("../secret"));
        assert!(!clip_id_is_safe("id/../../admin"));
        assert!(!clip_id_is_safe(".."));
        // Dot-dot anywhere is rejected, even without a separator.
        assert!(!clip_id_is_safe("a..b"));
    }

    #[test]
    fn rewrite_frames_endpoint_points_at_core() {
        let mut body = json!({ "id": "abc", "framesEndpoint": "/clips/abc/frame" });
        rewrite_frames_endpoint(&mut body, "abc");
        assert_eq!(body["framesEndpoint"], json!("/api/clips/abc/frame"));
    }

    #[test]
    fn rewrite_frames_endpoint_inserts_when_absent() {
        let mut body = json!({ "id": "xy" });
        rewrite_frames_endpoint(&mut body, "xy");
        assert_eq!(body["framesEndpoint"], json!("/api/clips/xy/frame"));
    }

    #[test]
    fn rewrite_frames_endpoint_noop_on_non_object() {
        let mut body = json!("not an object");
        rewrite_frames_endpoint(&mut body, "z");
        assert_eq!(body, json!("not an object"));
    }

    #[test]
    fn summary_renders_duration_mmss() {
        let md = build_clip_summary_md("My Clip", 125_000, &json!({}));
        assert!(md.contains("# My Clip"), "title missing: {md}");
        assert!(md.contains("- Duration: 02:05"), "duration wrong: {md}");
    }

    #[test]
    fn summary_includes_coverage_warning() {
        let md = build_clip_summary_md("C", 0, &json!({ "scanWarning": "partial" }));
        assert!(md.contains("- Coverage: partial"), "coverage missing: {md}");
    }

    #[test]
    fn summary_includes_highlights() {
        let bundle = json!({
            "recommendedMoments": [
                { "atMs": 3000, "reason": "intro" },
                { "atMs": 42000, "reason": "key point" },
            ]
        });
        let md = build_clip_summary_md("C", 60_000, &bundle);
        assert!(
            md.contains("## Highlights"),
            "highlights header missing: {md}"
        );
        assert!(md.contains("- 03s: intro"), "first moment missing: {md}");
        assert!(
            md.contains("- 42s: key point"),
            "second moment missing: {md}"
        );
    }

    #[test]
    fn summary_omits_empty_highlights() {
        let md = build_clip_summary_md("C", 0, &json!({ "recommendedMoments": [] }));
        assert!(
            !md.contains("## Highlights"),
            "should omit empty highlights: {md}"
        );
    }

    // ── async handler tests: an in-process mock Shadow on loopback ────────────
    //
    // The handlers resolve Shadow via `shadow_base()` (reads `RYU_SHADOW_URL` on
    // every call), so a test points that env at a per-test axum mock bound to an
    // ephemeral loopback port. `SHADOW_ENV` serializes every env-touching test so
    // the shared process env can't race; the guard is held across `.await` which is
    // safe under the default current-thread `#[tokio::test]` runtime.

    use std::net::Ipv4Addr;
    use std::path::{Path as StdPath, PathBuf};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::extract::RawQuery;

    use crate::{CaptionCue, ClipsHost, DownloadedClip};

    static SHADOW_ENV: Mutex<()> = Mutex::new(());

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        SHADOW_ENV.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[derive(Clone)]
    enum MockBody {
        Json(Value),
        Text(String),
        Bytes(Option<String>, Vec<u8>),
    }

    #[derive(Clone)]
    struct MockState {
        status: u16,
        body: MockBody,
    }

    async fn mock_handler(State(s): State<MockState>) -> Response {
        let st = StatusCode::from_u16(s.status).unwrap_or(StatusCode::OK);
        match s.body {
            MockBody::Json(v) => (st, Json(v)).into_response(),
            MockBody::Text(t) => (st, t).into_response(),
            MockBody::Bytes(ct, b) => match ct {
                Some(ct) => (st, [(header::CONTENT_TYPE, ct)], b).into_response(),
                None => (st, b).into_response(),
            },
        }
    }

    /// Spawn a mock Shadow that answers every route with `state`; returns its base URL.
    async fn spawn_mock(state: MockState) -> String {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().fallback(mock_handler).with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_json_mock(status: u16, body: Value) -> String {
        spawn_mock(MockState {
            status,
            body: MockBody::Json(body),
        })
        .await
    }

    async fn spawn_text_mock(status: u16, text: &str) -> String {
        spawn_mock(MockState {
            status,
            body: MockBody::Text(text.to_string()),
        })
        .await
    }

    async fn spawn_bytes_mock(status: u16, ct: Option<&str>, bytes: Vec<u8>) -> String {
        spawn_mock(MockState {
            status,
            body: MockBody::Bytes(ct.map(str::to_string), bytes),
        })
        .await
    }

    /// A base URL whose port is bound then freed, so a connect is refused fast —
    /// the "Shadow unreachable" fixture.
    async fn dead_base() -> String {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    async fn echo_query_handler(RawQuery(q): RawQuery) -> Response {
        (StatusCode::OK, Json(json!({ "query": q }))).into_response()
    }

    /// A mock that echoes the raw query it received on `/activity/recent`, so a test
    /// can assert the clamp that `recent_activity` applies before proxying.
    async fn spawn_activity_echo_mock() -> String {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/activity/recent", get(echo_query_handler));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    /// Records the couplings the crate drives through [`ClipsHost`], with fully
    /// configurable `ensure_ytdlp` / `download_video` outcomes.
    struct FakeHost {
        tmp: PathBuf,
        ytdlp: Result<(), String>,
        download: Result<DownloadedClip, String>,
        download_args: Arc<Mutex<Vec<(String, Option<u64>, Option<u64>)>>>,
        stored: Arc<Mutex<Vec<(String, bool, String)>>>,
    }

    impl FakeHost {
        fn new() -> Self {
            Self {
                tmp: std::env::temp_dir(),
                ytdlp: Ok(()),
                download: Ok(DownloadedClip {
                    video: PathBuf::from("/tmp/clip.mp4"),
                    captions: Some("hello".into()),
                    caption_segments: vec![CaptionCue {
                        start_ms: 0,
                        end_ms: 1000,
                        text: "cue".into(),
                    }],
                }),
                download_args: Arc::new(Mutex::new(Vec::new())),
                stored: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ClipsHost for FakeHost {
        fn tmp_dir(&self) -> PathBuf {
            self.tmp.clone()
        }
        async fn ensure_ytdlp(&self) -> Result<(), String> {
            self.ytdlp.clone()
        }
        async fn download_video(
            &self,
            url: &str,
            _work_dir: &StdPath,
            start: Option<u64>,
            end: Option<u64>,
        ) -> Result<DownloadedClip, String> {
            self.download_args
                .lock()
                .unwrap()
                .push((url.to_string(), start, end));
            self.download.clone()
        }
        async fn store_clip(&self, title: &str, mp4: Option<Vec<u8>>, summary_md: &str) {
            self.stored
                .lock()
                .unwrap()
                .push((title.to_string(), mp4.is_some(), summary_md.to_string()));
        }
    }

    fn ctx_with(host: Arc<FakeHost>) -> ClipsCtx {
        ClipsCtx::new(reqwest::Client::new(), host)
    }

    async fn body_of(resp: Response) -> (StatusCode, Value) {
        let st = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (st, v)
    }

    async fn raw_of(resp: Response) -> (StatusCode, String, Vec<u8>) {
        let st = resp.status();
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (st, ct, bytes.to_vec())
    }

    // ── list_clips ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_clips_returns_shadow_body_on_success() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "clips": ["a"] })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = list_clips(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(body, json!({ "clips": ["a"] }));
    }

    #[tokio::test]
    async fn list_clips_failsoft_on_non_2xx() {
        let _g = env_lock();
        let base = spawn_json_mock(500, json!({})).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = list_clips(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(body["available"], json!(false));
        assert!(body["reason"].as_str().unwrap().contains("HTTP 500"));
    }

    #[tokio::test]
    async fn list_clips_failsoft_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "not json").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = list_clips(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(body["available"], json!(false));
        assert!(body["reason"].as_str().unwrap().contains("invalid response"));
    }

    #[tokio::test]
    async fn list_clips_failsoft_when_shadow_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = list_clips(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(body["available"], json!(false));
        assert!(body["reason"].as_str().unwrap().contains("not reachable"));
    }

    #[test]
    fn local_ingest_rejects_paths_outside_allowlist_and_honors_env_extra_roots() {
        let dir =
            std::env::temp_dir().join(format!("ryu-clips-ingest-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("video.mp4");
        std::fs::write(&file, b"x").unwrap();
        let canonical = file.canonicalize().unwrap();

        std::env::remove_var("RYU_CLIPS_ALLOWED_DIRS");
        assert!(
            !local_ingest_allowed(&canonical),
            "a temp-dir file must be rejected by the default allowlist"
        );

        std::env::set_var("RYU_CLIPS_ALLOWED_DIRS", &dir);
        assert!(
            local_ingest_allowed(&canonical),
            "RYU_CLIPS_ALLOWED_DIRS must extend the allowlist"
        );
        std::env::remove_var("RYU_CLIPS_ALLOWED_DIRS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── start_clip ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn start_clip_passes_through_shadow_status_and_body() {
        let _g = env_lock();
        let base = spawn_json_mock(201, json!({ "id": "s1", "state": "recording" })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            start_clip(State(ctx_with(Arc::new(FakeHost::new()))), Json(json!({ "source": "d" })))
                .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::CREATED);
        assert_eq!(body["id"], json!("s1"));
    }

    #[tokio::test]
    async fn start_clip_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "nope").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            start_clip(State(ctx_with(Arc::new(FakeHost::new()))), Json(json!({}))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert_eq!(body["available"], json!(false));
    }

    #[tokio::test]
    async fn start_clip_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            start_clip(State(ctx_with(Arc::new(FakeHost::new()))), Json(json!({}))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert!(body["reason"].as_str().unwrap().contains("not reachable"));
    }

    // ── stop_clip ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn stop_clip_rejects_unsafe_id() {
        let (st, Json(body)) =
            stop_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("../x".into())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], json!("invalid clip id"));
    }

    #[tokio::test]
    async fn stop_clip_rewrites_frames_endpoint_on_success() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "id": "cid", "durationMs": 5000 })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            stop_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["framesEndpoint"], json!("/api/clips/cid/frame"));
    }

    #[tokio::test]
    async fn stop_clip_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            stop_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn stop_clip_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            stop_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    // ── pause_clip / resume_clip (proxy_clip_post) ───────────────────────────────

    #[tokio::test]
    async fn pause_clip_rejects_unsafe_id() {
        let (st, _) =
            pause_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("a/b".into())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn pause_clip_passes_through_on_success() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "state": "paused" })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            pause_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["state"], json!("paused"));
    }

    #[tokio::test]
    async fn resume_clip_rejects_unsafe_id() {
        let (st, _) =
            resume_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("..".into())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resume_clip_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            resume_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn proxy_clip_post_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            resume_clip(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert_eq!(body["available"], json!(false));
    }

    // ── get_sources ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_sources_returns_body_on_success() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "displays": [1] })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = get_sources(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(body["displays"], json!([1]));
    }

    #[tokio::test]
    async fn get_sources_failsoft_on_non_2xx() {
        let _g = env_lock();
        let base = spawn_json_mock(503, json!({})).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = get_sources(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert!(body["reason"].as_str().unwrap().contains("HTTP 503"));
    }

    #[tokio::test]
    async fn get_sources_failsoft_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = get_sources(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert!(body["reason"].as_str().unwrap().contains("invalid response"));
    }

    #[tokio::test]
    async fn get_sources_failsoft_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let Json(body) = get_sources(State(ctx_with(Arc::new(FakeHost::new())))).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert!(body["reason"].as_str().unwrap().contains("not reachable"));
    }

    // ── get_context ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_context_rejects_unsafe_id() {
        let (st, _) =
            get_context(State(ctx_with(Arc::new(FakeHost::new()))), Path("a/b".into())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_context_rewrites_frames_endpoint() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "id": "cid", "framesEndpoint": "/clips/cid/frame" }))
            .await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            get_context(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["framesEndpoint"], json!("/api/clips/cid/frame"));
    }

    #[tokio::test]
    async fn get_context_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            get_context(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn get_context_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            get_context(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    // ── get_frame / get_file (proxy_bytes) ───────────────────────────────────────

    #[tokio::test]
    async fn get_frame_rejects_unsafe_id() {
        let resp = get_frame(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("../x".into()),
            Query(FrameQuery { at_ms: 0 }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_frame_forwards_bytes_and_content_type() {
        let _g = env_lock();
        let base = spawn_bytes_mock(200, Some("image/png"), vec![1, 2, 3]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let resp = get_frame(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Query(FrameQuery { at_ms: 42 }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        let (st, ct, bytes) = raw_of(resp).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(ct, "image/png");
        assert_eq!(bytes, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn get_frame_forwards_shadow_content_type_verbatim() {
        // When Shadow sends a bytes body with no explicit content-type, axum stamps
        // `application/octet-stream`; proxy_bytes forwards that header verbatim (the
        // `default_ct` fallback only fires if the header is truly absent/non-utf8).
        let _g = env_lock();
        let base = spawn_bytes_mock(200, None, vec![9]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let resp = get_frame(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Query(FrameQuery { at_ms: 0 }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        let (st, ct, bytes) = raw_of(resp).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(ct, "application/octet-stream");
        assert_eq!(bytes, vec![9]);
    }

    #[tokio::test]
    async fn get_frame_forwards_non_2xx_status() {
        let _g = env_lock();
        let base = spawn_bytes_mock(404, Some("image/jpeg"), vec![]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let resp = get_frame(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Query(FrameQuery { at_ms: 0 }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_frame_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let resp = get_frame(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Query(FrameQuery { at_ms: 0 }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn get_file_rejects_unsafe_id() {
        let resp =
            get_file(State(ctx_with(Arc::new(FakeHost::new()))), Path("a/b".into())).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_file_forwards_mp4_bytes() {
        let _g = env_lock();
        let base = spawn_bytes_mock(200, Some("video/mp4"), vec![7, 7]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let resp =
            get_file(State(ctx_with(Arc::new(FakeHost::new()))), Path("cid".into())).await;
        std::env::remove_var("RYU_SHADOW_URL");
        let (st, ct, bytes) = raw_of(resp).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(ct, "video/mp4");
        assert_eq!(bytes, vec![7, 7]);
    }

    // ── post_diagnostics ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn post_diagnostics_rejects_unsafe_id() {
        let (st, _) = post_diagnostics(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("..".into()),
            Json(json!({})),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn post_diagnostics_passes_through_on_success() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "ok": true })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) = post_diagnostics(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Json(json!({ "level": "warn" })),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["ok"], json!(true));
    }

    #[tokio::test]
    async fn post_diagnostics_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) = post_diagnostics(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Json(json!({})),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn post_diagnostics_bad_gateway_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) = post_diagnostics(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Path("cid".into()),
            Json(json!({})),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    // ── recent_activity (clamp + fail-soft) ──────────────────────────────────────

    #[tokio::test]
    async fn recent_activity_clamps_minutes() {
        let _g = env_lock();
        let base = spawn_activity_echo_mock().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        // default (None) -> 3
        let (st, Json(b)) =
            recent_activity(State(ctx_with(Arc::new(FakeHost::new()))), Query(RecentActivityQuery { minutes: None }))
                .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(b["query"], json!("minutes=3"));
        // 0 -> clamp up to 1
        let (_, Json(b)) = recent_activity(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Query(RecentActivityQuery { minutes: Some(0) }),
        )
        .await;
        assert_eq!(b["query"], json!("minutes=1"));
        // 99 -> clamp down to 15
        let (_, Json(b)) = recent_activity(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Query(RecentActivityQuery { minutes: Some(99) }),
        )
        .await;
        assert_eq!(b["query"], json!("minutes=15"));
        // in-range passes unchanged
        let (_, Json(b)) = recent_activity(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Query(RecentActivityQuery { minutes: Some(7) }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(b["query"], json!("minutes=7"));
    }

    #[tokio::test]
    async fn recent_activity_forwards_non_2xx_status() {
        let _g = env_lock();
        let base = spawn_json_mock(500, json!({})).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) =
            recent_activity(State(ctx_with(Arc::new(FakeHost::new()))), Query(RecentActivityQuery { minutes: Some(3) }))
                .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body["reason"].as_str().unwrap().contains("HTTP 500"));
    }

    #[tokio::test]
    async fn recent_activity_bad_gateway_on_invalid_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "x").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, _) =
            recent_activity(State(ctx_with(Arc::new(FakeHost::new()))), Query(RecentActivityQuery { minutes: None }))
                .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn recent_activity_service_unavailable_when_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, body) =
            recent_activity(State(ctx_with(Arc::new(FakeHost::new()))), Query(RecentActivityQuery { minutes: None }))
                .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0["reason"].as_str().unwrap().contains("not reachable"));
    }

    // ── ingest ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ingest_rejects_empty_source() {
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Json(IngestBody {
                source: "   ".into(),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("missing `source`"));
    }

    #[tokio::test]
    async fn ingest_local_file_not_found_is_bad_request() {
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Json(IngestBody {
                source: "/definitely/not/here/nope.mp4".into(),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn ingest_url_bad_gateway_when_ytdlp_unavailable() {
        let mut host = FakeHost::new();
        host.ytdlp = Err("no yt-dlp".into());
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(host))),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert!(body["error"].as_str().unwrap().contains("yt-dlp"));
    }

    #[tokio::test]
    async fn ingest_url_bad_gateway_when_download_fails() {
        let mut host = FakeHost::new();
        host.download = Err("network down".into());
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(host))),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert!(body["error"].as_str().unwrap().contains("downloading the video failed"));
    }

    #[tokio::test]
    async fn ingest_url_success_rewrites_frames_and_forwards_untrimmed() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "id": "iid" })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let host = Arc::new(FakeHost::new());
        let args = host.download_args.clone();
        let (st, Json(body)) = ingest(
            State(ctx_with(host)),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                detail: Some("balanced".into()),
                start: None,
                end: None,
            }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["framesEndpoint"], json!("/api/clips/iid/frame"));
        // No trim => download not asked to trim.
        let recorded = args.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], ("https://example.com/v".to_string(), None, None));
    }

    #[tokio::test]
    async fn ingest_url_fully_bounded_trim_downloads_trimmed() {
        let _g = env_lock();
        let base = spawn_json_mock(200, json!({ "id": "iid" })).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let host = Arc::new(FakeHost::new());
        let args = host.download_args.clone();
        let (st, _) = ingest(
            State(ctx_with(host)),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                detail: None,
                start: Some(1000),
                end: Some(5000),
            }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::OK);
        // Fully-bounded trim => trimmed at download (start/end passed to yt-dlp).
        let recorded = args.lock().unwrap();
        assert_eq!(recorded[0], ("https://example.com/v".to_string(), Some(1000), Some(5000)));
    }

    #[tokio::test]
    async fn ingest_failsoft_when_shadow_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                ..Default::default()
            }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["available"], json!(false));
    }

    #[tokio::test]
    async fn ingest_bad_gateway_on_invalid_shadow_json() {
        let _g = env_lock();
        let base = spawn_text_mock(200, "not json").await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let (st, Json(body)) = ingest(
            State(ctx_with(Arc::new(FakeHost::new()))),
            Json(IngestBody {
                source: "https://example.com/v".into(),
                ..Default::default()
            }),
        )
        .await;
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(st, StatusCode::BAD_GATEWAY);
        assert_eq!(body["available"], json!(false));
    }

    // ── file_clip_into_space + summary integration ──────────────────────────────

    #[tokio::test]
    async fn file_clip_into_space_noops_without_id() {
        let host = Arc::new(FakeHost::new());
        let stored = host.stored.clone();
        file_clip_into_space(ctx_with(host), json!({ "title": "x" })).await;
        assert!(stored.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn file_clip_into_space_stores_mp4_and_summary() {
        let _g = env_lock();
        let base = spawn_bytes_mock(200, Some("video/mp4"), vec![1, 2, 3, 4]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let host = Arc::new(FakeHost::new());
        let stored = host.stored.clone();
        let bundle = json!({
            "id": "cid",
            "title": "Demo",
            "durationMs": 65000,
            "recommendedMoments": [{ "atMs": 3000, "reason": "intro" }]
        });
        file_clip_into_space(ctx_with(host), bundle).await;
        std::env::remove_var("RYU_SHADOW_URL");
        let s = stored.lock().unwrap();
        assert_eq!(s.len(), 1);
        let (title, has_mp4, summary) = &s[0];
        assert_eq!(title, "Demo");
        assert!(has_mp4, "mp4 bytes should be filed");
        assert!(summary.contains("# Demo"));
        assert!(summary.contains("01:05"));
        assert!(summary.contains("intro"));
    }

    #[tokio::test]
    async fn file_clip_into_space_stores_without_mp4_when_file_unreachable() {
        let _g = env_lock();
        let base = dead_base().await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let host = Arc::new(FakeHost::new());
        let stored = host.stored.clone();
        file_clip_into_space(ctx_with(host), json!({ "id": "cid", "title": "T" })).await;
        std::env::remove_var("RYU_SHADOW_URL");
        let s = stored.lock().unwrap();
        assert_eq!(s.len(), 1);
        assert!(!s[0].1, "no mp4 when Shadow /file is unreachable");
    }

    #[tokio::test]
    async fn file_clip_into_space_falls_back_title_when_blank() {
        let _g = env_lock();
        let base = spawn_bytes_mock(404, Some("video/mp4"), vec![]).await;
        std::env::set_var("RYU_SHADOW_URL", &base);
        let host = Arc::new(FakeHost::new());
        let stored = host.stored.clone();
        file_clip_into_space(ctx_with(host), json!({ "id": "cid", "title": "   " })).await;
        std::env::remove_var("RYU_SHADOW_URL");
        let s = stored.lock().unwrap();
        assert_eq!(s[0].0, "Clip");
        assert!(!s[0].1, "404 => no mp4 filed");
    }

    // ── small pure helpers + type contracts ─────────────────────────────────────

    #[test]
    fn unavailable_body_shape() {
        assert_eq!(
            unavailable("boom"),
            json!({ "available": false, "reason": "boom" })
        );
    }

    #[test]
    fn ingest_body_defaults_and_partial_deserialize() {
        let b: IngestBody = serde_json::from_value(json!({})).unwrap();
        assert!(b.source.is_empty());
        assert!(b.detail.is_none());
        assert!(b.start.is_none() && b.end.is_none());

        let b: IngestBody =
            serde_json::from_value(json!({ "source": "u", "detail": "efficient", "start": 5 }))
                .unwrap();
        assert_eq!(b.source, "u");
        assert_eq!(b.detail.as_deref(), Some("efficient"));
        assert_eq!(b.start, Some(5));
        assert!(b.end.is_none());
    }

    #[test]
    fn frame_query_defaults_at_ms_to_zero() {
        let q: FrameQuery = serde_json::from_value(json!({})).unwrap();
        assert_eq!(q.at_ms, 0);
        let q: FrameQuery = serde_json::from_value(json!({ "atMs": 250 })).unwrap();
        assert_eq!(q.at_ms, 250);
    }

    #[test]
    fn recent_activity_query_defaults_minutes_to_none() {
        let q: RecentActivityQuery = serde_json::from_value(json!({})).unwrap();
        assert!(q.minutes.is_none());
        let q: RecentActivityQuery = serde_json::from_value(json!({ "minutes": 9 })).unwrap();
        assert_eq!(q.minutes, Some(9));
    }

    #[test]
    fn routes_builds_without_panicking() {
        let _ = routes(ctx_with(Arc::new(FakeHost::new())));
    }

    #[test]
    fn openapi_doc_lists_clip_paths() {
        let doc = openapi();
        assert!(doc.paths.paths.contains_key("/api/clips"));
        assert!(doc.paths.paths.contains_key("/api/clips/ingest"));
        assert!(doc.paths.paths.contains_key("/api/clips/{id}/stop"));
    }

    // ── crate-root helpers: shadow_base / shadow_token / ShadowAuth ──────────────

    #[test]
    fn shadow_base_defaults_and_honors_env() {
        let _g = env_lock();
        std::env::remove_var("RYU_SHADOW_URL");
        assert_eq!(shadow_base(), "http://127.0.0.1:3030");
        std::env::set_var("RYU_SHADOW_URL", "http://127.0.0.1:9999");
        assert_eq!(shadow_base(), "http://127.0.0.1:9999");
        std::env::remove_var("RYU_SHADOW_URL");
    }

    #[test]
    fn shadow_token_trims_and_treats_empty_as_none() {
        let _g = env_lock();
        std::env::remove_var("SHADOW_API_TOKEN");
        assert_eq!(crate::shadow_token(), None);
        std::env::set_var("SHADOW_API_TOKEN", "   ");
        assert_eq!(crate::shadow_token(), None);
        std::env::set_var("SHADOW_API_TOKEN", "  tok  ");
        assert_eq!(crate::shadow_token().as_deref(), Some("tok"));
        std::env::remove_var("SHADOW_API_TOKEN");
    }

    #[test]
    fn shadow_auth_adds_bearer_only_when_token_present() {
        let _g = env_lock();
        let client = reqwest::Client::new();

        std::env::remove_var("SHADOW_API_TOKEN");
        let req = client
            .get("http://127.0.0.1/x")
            .shadow_auth()
            .build()
            .unwrap();
        assert!(req.headers().get(reqwest::header::AUTHORIZATION).is_none());

        std::env::set_var("SHADOW_API_TOKEN", "sekret");
        let req = client
            .get("http://127.0.0.1/x")
            .shadow_auth()
            .build()
            .unwrap();
        assert_eq!(
            req.headers()
                .get(reqwest::header::AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer sekret"
        );
        std::env::remove_var("SHADOW_API_TOKEN");
    }
}
