//! Shared Ryu sidecar data-directory seam.

use std::path::PathBuf;

/// Clip-ingest scratch space: `<data dir>/tmp/clips`.
pub fn tmp_dir() -> PathBuf {
    ryu_sidecar_runtime::layout::tmp_dir_for(&ryu_sidecar_runtime::ryu_dir(), "clips")
}
