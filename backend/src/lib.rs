//! Clips: agent-native Loom/Jam for Ryu (the Core→Shadow capture proxy).
//!
//! Shadow owns the sensor half of clips (screen + audio capture, ffmpeg mux, the
//! agent-context.json bundle). Core owns *what runs* — the clip session the
//! desktop drives — so this crate exposes a stable `/api/clips/*` surface (built
//! in [`api`]) and proxies each call to the Shadow sidecar over loopback.
//!
//! Placement (CLAUDE.md §1): capture + bundle is "what runs" (Core/Shadow).
//! Redacting diagnostics on egress is "what is shared" (a Gateway concern); v1
//! redacts client-side in the extension, so nothing here enforces policy.
//!
//! Fail-soft: when Shadow is down these handlers return `{ available: false,
//! reason }` (the same shape as the Shadow MCP provider) rather than a 5xx, so a
//! stopped sidecar degrades gracefully in the UI instead of erroring.
//!
//! ## Host inversion
//! This crate has ZERO dependency on `apps/core`. The two kernel couplings the
//! moved code needs are inverted through the [`ClipsHost`] trait, which
//! `apps/core` implements in its `clips_host` shim and injects into [`ClipsCtx`]:
//! - **yt-dlp ingest** — resolving a watched URL to a local video (over Core's
//!   `DownloadCenter`) is kernel binary management.
//! - **auto-file into the `Clips` Space** — a finished clip's mp4 + summary are
//!   stored in the `Clips` system Space (a Core store).
//! Everything else — the Shadow HTTP proxying, `framesEndpoint` rewriting, the
//! ingest orchestration (URL vs local-file resolution), and the summary render —
//! lives in the crate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

pub mod api;

pub use api::{routes, ClipsCtx};

/// Resolve the Shadow base URL: `RYU_SHADOW_URL` if set, else loopback Shadow.
/// Mirrors `sidecar/mcp/shadow.rs` so the address stays in one convention.
pub(crate) fn shadow_base() -> String {
    std::env::var("RYU_SHADOW_URL").unwrap_or_else(|_| "http://127.0.0.1:3030".into())
}

/// The shared-secret bearer Shadow's HTTP surface requires (everything except
/// `/health` — see `apps/shadow/src/server.rs`). Read from `SHADOW_API_TOKEN`,
/// the SAME env var Shadow itself resolves: Core injects it at sidecar spawn
/// (`manifest_sidecar::inject_ext_env`), and an operator export works for a
/// standalone run. `None` = send no bearer; Shadow then rejects the call (401)
/// and the fail-soft handlers surface `{ available: false }`.
///
/// Core also passes the token file's path (`SHADOW_API_TOKEN_FILE`). The file
/// is read on every call and wins over the env value, because Core can rotate
/// the token after this sidecar was spawned.
pub(crate) fn shadow_token() -> Option<String> {
    resolve_shadow_token(
        std::env::var_os("SHADOW_API_TOKEN_FILE"),
        std::env::var("SHADOW_API_TOKEN").ok(),
    )
}

/// The token from the file at `file` when it is readable and non-empty, else
/// `env_token`. Either value is trimmed; an empty one counts as absent.
fn resolve_shadow_token(
    file: Option<std::ffi::OsString>,
    env_token: Option<String>,
) -> Option<String> {
    let non_empty = |value: String| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    };
    file.and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(non_empty)
        .or_else(|| env_token.and_then(non_empty))
}

/// Recent successful Shadow peer proofs, shared by every proxied hop.
static SHADOW_PEER_PROOF: ryu_sidecar_runtime::peer_proof::ProofCache =
    ryu_sidecar_runtime::peer_proof::ProofCache::new(std::time::Duration::from_secs(2));

/// Why a Shadow hop returned no response.
#[derive(Debug)]
pub(crate) enum ShadowSendError {
    /// No `SHADOW_API_TOKEN`: Shadow would reject the call, so it is not made.
    NoToken,
    /// Not sent to a verified Shadow (see [`ShadowSend::send_shadow`]), or the
    /// request to it failed.
    Send(ryu_sidecar_runtime::peer_proof::VerifiedSendError),
}

impl std::fmt::Display for ShadowSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoToken => f.write_str("no Shadow API token is configured"),
            Self::Send(error) => error.fmt(f),
        }
    }
}

/// Send a request to Shadow with its bearer. Extension trait so every proxied
/// Shadow hop in `api.rs` ends in `.send_shadow()` and the token handling can
/// never drift per call site.
///
/// The bearer goes only to a listener that first proved it holds the token
/// (`ryu_sidecar_runtime::peer_proof`): a process squatting on Shadow's port
/// gets neither the token nor the request.
pub(crate) trait ShadowSend {
    async fn send_shadow(self) -> Result<reqwest::Response, ShadowSendError>;
}

impl ShadowSend for reqwest::RequestBuilder {
    async fn send_shadow(self) -> Result<reqwest::Response, ShadowSendError> {
        let token = shadow_token().ok_or(ShadowSendError::NoToken)?;
        ryu_sidecar_runtime::peer_proof::send_with_verified_bearer(self, &token, &SHADOW_PEER_PROOF)
            .await
            .map_err(ShadowSendError::Send)
    }
}

/// The default, undeletable system Space that finished clips are auto-filed into.
/// Seeded eagerly in Core's `main.rs` (same pattern as "Artifacts") and re-resolved
/// idempotently at file-time by the host's [`ClipsHost::store_clip`].
pub const CLIPS_SPACE_NAME: &str = "Clips";
pub const CLIPS_SPACE_DESC: &str = "Screen recordings and clips captured by Ryu";

// ── Host inversion (the kernel couplings live in apps/core) ───────────────────

/// One timed caption cue parsed from a downloaded video's subtitles. Mirrors the
/// host's yt-dlp `CaptionCue` so the crate never imports Core.
#[derive(Debug, Clone)]
pub struct CaptionCue {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}

/// A downloaded video (from a watched URL) plus any captions yt-dlp pulled
/// alongside it — the result of [`ClipsHost::download_video`].
#[derive(Debug, Clone)]
pub struct DownloadedClip {
    /// Local path to the downloaded video file.
    pub video: PathBuf,
    /// Plain-text captions (deduped, tags stripped) for the transcript body.
    pub captions: Option<String>,
    /// Timed caption cues parsed from the same subtitles, for transcript segments.
    pub caption_segments: Vec<CaptionCue>,
}

/// The narrow seam this crate needs from `apps/core`'s kernel machinery. It carries
/// ONLY the two couplings the moved clips code uses: the yt-dlp downloader (over
/// Core's `DownloadCenter`) for URL ingest, and filing a finished clip into the
/// `Clips` system Space. `apps/core` implements this in its `clips_host` shim.
#[async_trait]
pub trait ClipsHost: Send + Sync {
    /// Feature availability of the concrete host, projected to API callers.
    fn supports_url_ingest(&self) -> bool {
        true
    }
    fn supports_space_filing(&self) -> bool {
        true
    }

    /// The base directory under which ingest work dirs are created
    /// (`<data dir>/tmp/clips` in the sidecar). The crate appends a unique `clip-ingest-<uuid>` segment.
    fn tmp_dir(&self) -> PathBuf;

    /// Ensure the yt-dlp downloader is installed (a URL ingest needs it). The
    /// `Err` string is the display of the underlying install error.
    async fn ensure_ytdlp(&self) -> Result<(), String>;

    /// Download a watched video URL into `work_dir` (+ optional captions/segments),
    /// trimming to `[start, end)` (ms) when both are set. The `Err` string is the
    /// display of the underlying download error.
    async fn download_video(
        &self,
        url: &str,
        work_dir: &Path,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<DownloadedClip, String>;

    /// File a finished clip into the `Clips` system Space (idempotent get-or-create
    /// of the space, then the mp4 blob when present + a short markdown summary).
    /// Fail-soft: the host logs and continues on any step's error — this NEVER
    /// changes the clip HTTP success status (the crate spawns it, doesn't await it).
    /// The API checks `supports_space_filing` before scheduling, and reports filing
    /// as unavailable or pending; it never claims a detached task has completed.
    async fn store_clip(&self, title: &str, mp4: Option<Vec<u8>>, summary_md: &str);
}

/// A shared handle to the injected host.
pub type SharedClipsHost = Arc<dyn ClipsHost>;

#[cfg(test)]
mod tests {
    use super::resolve_shadow_token;

    #[test]
    fn shadow_token_prefers_the_current_token_file_over_the_spawn_env() {
        let path = std::env::temp_dir().join(format!(
            "ryu-clips-shadow-token-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(&path, "  rotated-token\n").unwrap();
        assert_eq!(
            resolve_shadow_token(Some(path.clone().into()), Some("spawn-token".into())),
            Some("rotated-token".to_owned())
        );
        // An empty or missing file falls back to the injected value.
        std::fs::write(&path, " \n").unwrap();
        assert_eq!(
            resolve_shadow_token(Some(path.clone().into()), Some(" spawn-token ".into())),
            Some("spawn-token".to_owned())
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            resolve_shadow_token(Some(path.into()), Some("spawn-token".into())),
            Some("spawn-token".to_owned())
        );
        assert_eq!(resolve_shadow_token(None, Some("  ".into())), None);
        assert_eq!(resolve_shadow_token(None, None), None);
    }
}
