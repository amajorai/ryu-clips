# ryu-clips

Clips for Ryu — agent-native Loom/Jam: capture and browse screen/timeline clips via the Shadow capture proxy.

> **Read-only mirror.** Developed in https://github.com/amajorai/ryu —
> please open issues and pull requests there, not on this repository.

## Install

- Binary: `ryu-clips` from the [Ryu releases](https://github.com/amajorai/ryu/releases).
- Crate: `cargo install ryu-clips`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).

---

# Clips

Agent-native Loom / Jam for Ryu: capture and browse screen/timeline clips. Core owns *what
runs* — the clip session the desktop drives — and Shadow owns the sensor half (screen +
audio capture, ffmpeg mux, the agent-context.json bundle).

## Parts

- **`backend/` (`ryu-clips`)** — an extracted Core capability crate: the stable
  `/api/clips/*` HTTP surface that **proxies each call to the Shadow sidecar over loopback**.
  **Now served OUT-OF-PROCESS** by the `ryu-clips` bin (`[[bin]]`, `kind:local`, `public_mount`,
  `RYU_CLIPS_BIN`/`RYU_CLIPS_PORT`, default `:7992`) via the generic ext-proxy loader — Core links
  **zero clips code** (no path-dep, no `clips` cargo feature, no in-process `clips_routes`).
  It also owns `framesEndpoint` rewriting, the ingest orchestration (URL vs local-file
  resolution), and the summary render. Because the crate now runs standalone, its two former
  kernel couplings **degrade cleanly in the sidecar** rather than inverting through a Core
  `clips_host` shim (the Shadow record/browse half is fully live — the sidecar reads
  `RYU_SHADOW_URL` itself):
  - **yt-dlp ingest** — resolving a watched URL to a local video over Core's `DownloadCenter`
    (kernel binary management);
  - **auto-file into the `Clips` Space** — a finished clip's mp4 + summary stored in the
    `Clips` system Space (a Core store).
- **No companion UI here.** Clips surface through Core-side desktop/extension views.

## Fail-soft

When Shadow is down, handlers return `{ available: false, reason }` (the same shape as the
Shadow MCP provider) rather than a 5xx, so a stopped sidecar degrades gracefully in the UI.

## Manifest (Core fixture)

- **id** `com.ryu.clips`, no runnables, no `permission_grants`.
- **requires** app `shadow` (>=1.0.0) — it is a Core→Shadow proxy and depends on the Shadow
  capture app for its recordings.

## Surface

`/api/clips` (list) · `ingest` · `sources` · `recent-activity` · `start`, per-clip
`:id/{stop,pause,resume,context,frame,file,diagnostics}`.

## Core-vs-Gateway

Capture + bundle is "what runs" (Core/Shadow). Redacting diagnostics on egress is a Gateway
concern; v1 redacts client-side in the extension, so nothing here enforces policy.
