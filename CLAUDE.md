# CLAUDE.md — dmr_decoder

Headless Python app that decodes a conventional (non-trunked) DMR channel via RTL-SDR and uploads completed calls to the dispatcher app (`mostlychris/dispatcher`).

## Running the app

```bash
python3 dmr_decoder.py                          # default port 8081
python3 dmr_decoder.py --config /path/to/cfg.json
python3 dmr_decoder.py --listen-port 8082
python3 dmr_decoder.py --debug                  # prints meaningful dsd-fme stderr lines
```

## Dependencies

```bash
sudo apt install rtl-sdr ffmpeg
pip install fastapi "uvicorn[standard]" numpy scipy
# dsd-fme (lwvmobile/dsd-fme) must be compiled and on PATH as `dsd-fme`
```

## Configuration

Copy `decoder_config.example.json` → `decoder_config.json` and edit. Key fields:

| Key | Description |
|-----|-------------|
| `frequency` | Center frequency in MHz, e.g. `"464.0875"` |
| `gain` | RTL-SDR gain in dB, e.g. `"40"` |
| `ppm` | Frequency correction (this dongle: **-5** at 464 MHz) |
| `device` | RTL-SDR device index (0 = rtl-scanner, **1 = DMR decoder**) |
| `decoder` | Decoder binary name, e.g. `"dsd-fme"` |
| `decoder_format` | dsd-fme format flag value: `"g"` = force DMR, `"gf"` = auto/DMR-preferred (default), `"a"` = full auto, `"d"` = D-STAR |
| `decoder_args` | Extra args passed to decoder, e.g. `[]` |
| `rtlfm_rate` | rtl_fm sample rate, default `48000` |
| `rtlfm_mode` | rtl_fm mode, default `"fm"` |
| `rtlfm_args` | Extra args passed to rtl_fm |
| `activity_threshold` | RMS floor for signal detection (0–32767); default `300` |
| `squelch_hold` | Seconds to hold call open after signal drops; default `0.5` |
| `min_call_length` | Minimum call duration in seconds to trigger upload; default `1.0` |
| `audio_lp_hz` | LP filter cutoff for post-processing (removes AMBE harshness); default `3500` |
| `audio_gain` | Output gain multiplier; default `0.80` |
| `audio_crossfade_ms` | Fade duration (ms) applied at AMBE frame-erasure boundaries; `0` to disable; default `8.0` |

### Call detection tuning

- **`activity_threshold`** — RMS energy level (16-bit scale, 0–32767) above which a chunk counts as "signal." Default 300 (~0.9% of full scale). Raise if noise triggers false calls; lower if real audio is being missed.
- **`squelch_hold`** — How long (seconds) the detector stays open after signal drops below threshold. Bridges inter-word silence in DMR AMBE decoding. Raise to 1.0–1.5 if transmissions are being split in two.
- **`min_call_length`** — Calls shorter than this (seconds) are silently discarded. Filters noise bursts and sync-only frames. Lower to 0.5 to catch very short calls; raise if garbage is being uploaded.

Flow: signal above threshold → ACTIVE → signal drops → HOLD for `squelch_hold` → FINALIZE → if duration ≥ `min_call_length` → upload.

### Dispatcher section

```json
"dispatcher": {
  "url":             "http://127.0.0.1:9090",
  "api_key":         "",
  "system":          "Local DMR",
  "talkgroup":       1,
  "talkgroup_tag":   "DMR",
  "talkgroup_name":  "Local DMR Channel",
  "talkgroup_group": "DMR"
}
```

Upload goes to `POST {url}/api/call-upload` as multipart (Trunk Recorder / rdio-scanner compatible). The `talkgroup` field here is the fallback; if dsd-fme decodes a live TGT from stderr that value is used instead.

### Broadcastify section (optional)

```json
"broadcastify": {
  "enabled":        false,
  "server":         "audio2.broadcastify.com",
  "port":           80,
  "password":       "your_stream_password",
  "mountpoint":     "/your_mountpoint",
  "bitrate":        32,
  "lp_cutoff":      3500,
  "admin_user":     "admin",
  "admin_password": "",
  "metadata_delay": 0.0
}
```

## Architecture

### Pipeline

```
rtl_fm -d {device} -f {freq} -s {rate} -g {gain} -p {ppm} -M fm -
    → (stdin)
dsd-fme -f{format} -i - -o -
    → stdout: raw 16-bit mono PCM at 16 kHz
    → stderr: TGT=NNNN SRC=NNNN lines (parsed for live talkgroup)
```

**Important:** `rtl:N` inside dsd-fme has an off-by-one bug (rtl:1 opens device 2). Always use `rtl_fm -d N | dsd-fme -i -` as the pipeline instead of letting dsd-fme open the device.

### Key classes

**`AudioProcessor`** — 2nd-order Butterworth LP filter at `audio_lp_hz` + gain trim. Runs on every PCM chunk to remove AMBE vocoder harshness and prevent clipping. Falls back to passthrough if scipy is not available.

**`CallDetector`** — State machine (IDLE → ACTIVE → HOLD → IDLE) that buffers PCM during transmissions and fires `CallUploader.upload()` in a background thread when a call ends.

**`CallUploader`** — POSTs completed calls as multipart WAV to the dispatcher. Captures live TGT/SRC at call-start via `_get_tg()` callback from `DMRDecoder`.

**`AudioBroadcaster`** — Thread-safe distributor; pushes PCM chunks to asyncio queues for `/stream` and `/ws/audio` consumers.

**`IcecastFeeder`** — ffmpeg subprocess that encodes PCM → MP3 and pushes to Icecast/Broadcastify. Auto-reconnects on disconnect. Generates silence between transmissions to keep the stream alive.

**`DMRDecoder`** — Main controller. Launches `rtl_fm` and `dsd-fme` subprocesses. Uses `select`-based read loop on dsd-fme stdout with `CHUNK_MS` (120 ms) timeout — injects `SILENCE` chunks when dsd-fme produces no output (between transmissions) so `CallDetector` ticks continuously and calls finalize promptly. Reads dsd-fme stderr in a background thread to extract live TGT/SRC.

### `_read_stderr` filtering

Only lines that are NOT matched by `_NOISE_RE` are printed in `--debug` mode:

```python
_NOISE_RE = re.compile(r'Sync:|Activity Update|SLCO NULL|^\s*$')
```

Lines like `IDLE`, `SLCO NULL`, `Hash`, and `TLC` are normal DMR idle/control frames — not errors.

### Audio constants

| Constant | Value | Notes |
|----------|-------|-------|
| `AUDIO_RATE` | 16000 | dsd-fme actual PCM output rate (Hz) |
| `CHUNK_MS` | 120 | Distribution granularity (ms) |
| `CHUNK_SIZE` | 3840 | Bytes per chunk (16-bit mono × 120 ms) |

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /status` | JSON: name, frequency, label, active, rx_count, talkgroup, source, icecast state |
| `GET /stream` | Live WAV stream (infinite-length header), 16 kHz mono 16-bit |
| `WS /ws/audio` | Raw 16-bit PCM WebSocket at `AUDIO_RATE` Hz |
| `GET /debug` | Extended live state dump |

## Systemd service

```bash
bash install-service.sh
```

Creates `/etc/systemd/system/dmr-decoder.service`, enables and starts it. Uses `After=network-online.target` + `Wants=network-online.target` so the service waits for the network before starting (avoids "connection refused" at boot).

```bash
sudo systemctl status dmr-decoder
sudo journalctl -u dmr-decoder -f
sudo systemctl restart dmr-decoder
sudo systemctl stop dmr-decoder
```

## Monitoring decode quality (ncurses UI)

dsd-fme has a built-in ncurses display. Because the RTL device index bug requires piping through rtl_fm, and ncurses needs an interactive terminal, you need PulseAudio running for dsd-fme's audio subsystem to initialize:

```bash
sudo apt install pulseaudio -y
pulseaudio --start
```

Then in a terminal with a proper TTY:

```bash
rtl_fm -d 1 -f 464087500 -s 48000 -g 40 -p -5 -M fm - 2>/dev/null \
  | dsd-fme -fs -i - -o /dev/null -N 2>/tmp/dsd.log
```

Or tail the log file for decode quality lines:

```bash
tail -f /tmp/dsd.log
```

## Known hardware notes

- **RTL-SDR device 0**: used by rtl-scanner (`app.py`)
- **RTL-SDR device 1**: used by dmr_decoder
- **PPM offset**: **-5** for this dongle at 464 MHz (verified empirically)
- **dsd-fme RTL device bug**: `rtl:1` opens device index 2; always use the `rtl_fm | dsd-fme` pipeline

## Repository

GitHub: https://github.com/mostlychris/dmr_decoder
Companion app: https://github.com/mostlychris/dispatcher
