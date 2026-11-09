//! Inbox, toast and push copy as localizable messages.
//!
//! Core renders a notify hint's `i18n` text per recipient (the inbox row and
//! toast in the node's `ui-language`, each push in the locale its device
//! registered with); the English rendering still goes out as the plain
//! `title`/`body`, which an older Core delivers as is.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`name = arg("name")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence. Core's renderer has no plural selector,
//! so counts are written as `label: {count}`.
#![allow(clippy::useless_format)]

use ryu_app_events::{NotifyHint, NotifyMessage, NotifyTemplate as Template};

pub(crate) const CLIP_READY_MESSAGE: Template =
    |arg| format!("Clip ready: {title}", title = arg("title"));
pub(crate) const CLIP_LENGTH_MESSAGE: Template =
    |arg| format!("Recording length: {seconds}s", seconds = arg("seconds"));

/// The Inbox notification for a finished clip.
pub(crate) fn clip_ready(title: &str, duration_ms: u64) -> NotifyHint {
    let seconds = i64::try_from(duration_ms.saturating_add(500) / 1000).unwrap_or(i64::MAX);
    NotifyHint::localized(
        NotifyMessage::new(CLIP_READY_MESSAGE).text("title", title),
        vec![NotifyMessage::new(CLIP_LENGTH_MESSAGE).count("seconds", seconds)],
    )
}
