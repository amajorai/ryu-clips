//! Inlined data-dir resolution (tracer copy of `apps/core/src/paths.rs`, matching
//! `apps-store/mail/backend/src/paths.rs` and `apps-store/teams/backend/src/paths.rs`).
//!
//! The sidecar MUST resolve the SAME data dir Core uses so its ingest work dirs
//! (`ryu_dir()/tmp`) co-locate with the node. The load-bearing rule is
//! `RYU_DIR`-env-first: Core/Kernel passes `RYU_DIR` to the sidecar at spawn,
//! guaranteeing co-location. The pointer-file read + `RYU_PROFILE` suffix are
//! replicated for faithfulness in the headless case, but env-first + default is
//! what actually guarantees the shared path.

use std::path::PathBuf;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

const RYU_DIR_ENV: &str = "RYU_DIR";
const RYU_PROFILE_ENV: &str = "RYU_PROFILE";
const RELEASE_PROFILE: &str = "release";

/// Data-dir / config-dir suffix for the active profile: `""` for release,
/// `-<profile>` otherwise (e.g. `-dev`). Mirrors `crate::profile::suffix`.
fn suffix() -> String {
    let profile = std::env::var(RYU_PROFILE_ENV)
        .ok()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| RELEASE_PROFILE.to_string());
    if profile == RELEASE_PROFILE {
        String::new()
    } else {
        format!("-{}", profile.trim())
    }
}

/// The default data dir: `~/.ryu{suffix}` (falling back to `./.ryu` if home is
/// unknown).
fn default_ryu_dir() -> PathBuf {
    let name = format!(".ryu{}", suffix());
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(name)
}

/// Config dir holding the bootstrap pointer file (`ryu{suffix}` under the OS
/// config dir), NOT inside the data dir.
fn config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(default_ryu_dir)
        .join(format!("ryu{}", suffix()))
}

fn pointer_path() -> PathBuf {
    config_dir().join("data-path.json")
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct DataPathPointer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_dir: Option<String>,
}

fn read_pointer() -> DataPathPointer {
    let Ok(bytes) = std::fs::read(pointer_path()) else {
        return DataPathPointer::default();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn resolve() -> PathBuf {
    if let Some(v) = std::env::var_os(RYU_DIR_ENV) {
        let p = PathBuf::from(v);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    if let Some(dir) = read_pointer().data_dir {
        let p = PathBuf::from(dir);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    default_ryu_dir()
}

static RYU_DIR: OnceLock<PathBuf> = OnceLock::new();

/// The active data dir, resolved once and cached for the process lifetime.
pub fn ryu_dir() -> PathBuf {
    RYU_DIR.get_or_init(resolve).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serializes the process-wide env this module reads (RYU_PROFILE / RYU_DIR).
    static ENV: Mutex<()> = Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        ENV.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn suffix_is_empty_for_release_and_suffixed_otherwise() {
        let _g = lock();
        std::env::set_var(RYU_PROFILE_ENV, RELEASE_PROFILE);
        assert_eq!(suffix(), "");
        std::env::set_var(RYU_PROFILE_ENV, "dev");
        assert_eq!(suffix(), "-dev");
        // Whitespace is trimmed around the profile name.
        std::env::set_var(RYU_PROFILE_ENV, "  staging  ");
        assert_eq!(suffix(), "-staging");
        // Blank profile collapses to release (empty suffix).
        std::env::set_var(RYU_PROFILE_ENV, "   ");
        assert_eq!(suffix(), "");
        // Unset also means release.
        std::env::remove_var(RYU_PROFILE_ENV);
        assert_eq!(suffix(), "");
    }

    #[test]
    fn default_ryu_dir_carries_the_profile_suffix() {
        let _g = lock();
        std::env::set_var(RYU_PROFILE_ENV, "dev");
        assert!(default_ryu_dir().to_string_lossy().ends_with(".ryu-dev"));
        std::env::remove_var(RYU_PROFILE_ENV);
        assert!(default_ryu_dir().to_string_lossy().ends_with(".ryu"));
    }

    #[test]
    fn config_and_pointer_paths_use_the_suffix() {
        let _g = lock();
        std::env::set_var(RYU_PROFILE_ENV, "dev");
        assert!(config_dir().to_string_lossy().contains("ryu-dev"));
        assert!(pointer_path().ends_with("data-path.json"));
        std::env::remove_var(RYU_PROFILE_ENV);
    }

    #[test]
    fn resolve_prefers_ryu_dir_env() {
        let _g = lock();
        let want = std::env::temp_dir().join("ryu-clips-paths-resolve");
        std::env::set_var(RYU_DIR_ENV, &want);
        assert_eq!(resolve(), want);
        std::env::remove_var(RYU_DIR_ENV);
    }

    #[test]
    fn resolve_ignores_empty_ryu_dir_env_and_falls_back_to_default() {
        let _g = lock();
        std::env::remove_var(RYU_PROFILE_ENV);
        std::env::set_var(RYU_DIR_ENV, "");
        // Empty RYU_DIR is skipped; with no pointer file it lands on the default.
        assert!(resolve().to_string_lossy().ends_with(".ryu"));
        std::env::remove_var(RYU_DIR_ENV);
    }

    #[test]
    fn read_pointer_defaults_when_file_absent() {
        let _g = lock();
        std::env::set_var(RYU_PROFILE_ENV, "no-such-profile-xyz");
        // No pointer file exists under this made-up profile => default (None).
        assert!(read_pointer().data_dir.is_none());
        std::env::remove_var(RYU_PROFILE_ENV);
    }

    #[test]
    fn data_path_pointer_deserializes_both_shapes() {
        let p: DataPathPointer = serde_json::from_str(r#"{"data_dir":"/x"}"#).unwrap();
        assert_eq!(p.data_dir.as_deref(), Some("/x"));
        let empty: DataPathPointer = serde_json::from_str("{}").unwrap();
        assert!(empty.data_dir.is_none());
    }

    #[test]
    fn ryu_dir_is_cached_and_stable() {
        let _g = lock();
        let a = ryu_dir();
        let b = ryu_dir();
        assert_eq!(a, b);
        assert!(!a.as_os_str().is_empty());
    }
}
