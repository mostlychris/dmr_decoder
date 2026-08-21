#!/usr/bin/env python3
"""
dmr_decoder.py — Headless conventional DMR channel decoder.

Pipeline:  rtl_fm → dsd / dsd-fme → 16-bit 8 kHz mono PCM

Outputs
-------
1. Call upload   POST completed call WAVs to the dispatcher's
                 /api/call-upload endpoint (Trunk Recorder / rdio-scanner
                 uploader compatible).  One POST per discrete transmission.
2. /stream       Live WAV stream for real-time monitoring (VLC, ffplay …)
3. /ws/audio     Raw 16-bit mono PCM WebSocket at AUDIO_RATE Hz
4. Icecast       Optional continuous MP3 stream via ffmpeg
5. /status       JSON health / state

Usage
-----
  python3 dmr_decoder.py
  python3 dmr_decoder.py --config /path/to/config.json
  python3 dmr_decoder.py --listen-port 8081
  python3 dmr_decoder.py --debug
"""

import argparse
import asyncio
import json
import os
import queue as _q_mod
import re
import select as _select
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

_TG_RE    = re.compile(r'\bTGT=(\d+)')
_SRC_RE   = re.compile(r'\bSRC=(\d+)')
_NOISE_RE = re.compile(r'Sync:|Activity Update|SLCO NULL|^\s*$')

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import Response, StreamingResponse
import uvicorn

# ── Audio constants ────────────────────────────────────────────────────────────
AUDIO_RATE = 16_000                               # dsd-fme actual output sample rate
CHUNK_MS   = 120                                  # PCM distribution granularity
CHUNK_SIZE = AUDIO_RATE * 2 * CHUNK_MS // 1000   # bytes — 16-bit mono
SILENCE    = bytes(CHUNK_SIZE)

DEFAULT_CONFIG      = "decoder_config.json"
ACTIVITY_THRESHOLD  = 300    # default RMS floor for signal detection

# ── WAV helpers ────────────────────────────────────────────────────────────────

def _wav_stream_header(rate: int = AUDIO_RATE) -> bytes:
    """Infinite-length WAV header for the /stream endpoint."""
    data_size = 0xFFFFFFF0
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", data_size + 36, b"WAVE",
        b"fmt ", 16, 1, 1, rate, rate * 2, 2, 16,
        b"data", data_size,
    )

def _wav_file(pcm: bytes, rate: int = AUDIO_RATE) -> bytes:
    """Finite WAV file for call uploads."""
    n = len(pcm)
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", n + 36, b"WAVE",
        b"fmt ", 16, 1, 1, rate, rate * 2, 2, 16,
        b"data", n,
    ) + pcm

# ── RMS helper ─────────────────────────────────────────────────────────────────

def _rms(data: bytes) -> float:
    n = len(data) // 2
    if not n:
        return 0.0
    samples = struct.unpack_from(f"<{n}h", data)
    return (sum(s * s for s in samples) / n) ** 0.5

# ── Call uploader (dispatcher /api/call-upload) ────────────────────────────────

class CallUploader:
    """POSTs completed call WAV files to the dispatcher's Trunk Recorder endpoint.

    The dispatcher's /api/call-upload is compatible with the rdio-scanner
    uploader plugin used by trunk-recorder.  One multipart POST per call.
    """

    def __init__(self, cfg: dict, freq_mhz: float):
        self._url      = cfg["url"].rstrip("/") + "/api/call-upload"
        self._api_key  = cfg.get("api_key", "")
        self._system   = cfg.get("system", "DMR")
        self._tg       = str(cfg.get("talkgroup", 1))
        self._tg_tag   = cfg.get("talkgroup_tag", "")
        self._tg_name  = cfg.get("talkgroup_name", "")
        self._tg_group = cfg.get("talkgroup_group", "")
        self._freq_hz  = str(int(freq_mhz * 1_000_000))

    def probe(self):
        """Send a harmless test probe to verify dispatcher connectivity."""
        try:
            import urllib.request
            data = urllib.parse.urlencode(
                {"system": self._system, "test": "1"}
            ).encode()
            req  = urllib.request.Request(self._url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=5):
                pass
            print(f"[Upload] Dispatcher reachable at {self._url}")
        except Exception as e:
            print(f"[Upload] Warning — cannot reach dispatcher: {e}", file=sys.stderr)

    def upload(self, pcm: bytes, start_time: float, duration: float,
               talkgroup: Optional[int] = None, source: Optional[int] = None):
        """Encode PCM → WAV and POST to dispatcher.  Runs in a background thread."""
        import urllib.request
        import urllib.parse

        tg    = str(talkgroup) if talkgroup is not None else self._tg
        wav   = _wav_file(pcm)
        fname = f"{self._system}_{tg}_{int(start_time)}.wav"

        fields = {
            "key":            self._api_key,
            "systemLabel":    self._system,
            "talkgroup":      tg,
            "dateTime":       str(int(start_time)),
            "frequency":      self._freq_hz,
            "talkgroupTag":   self._tg_tag,
            "talkgroupName":  self._tg_name,
            "talkgroupGroup": self._tg_group,
            "sources":        f'[{{"src":{source}}}]' if source else "[]",
        }

        boundary = b"----DmrDecoderBoundary"
        body     = bytearray()

        for k, v in fields.items():
            body += (
                b"--" + boundary + b"\r\n"
                b'Content-Disposition: form-data; name="' + k.encode() + b'"\r\n'
                b"\r\n" + v.encode() + b"\r\n"
            )

        body += (
            b"--" + boundary + b"\r\n"
            b'Content-Disposition: form-data; name="audio"; filename="'
            + fname.encode() + b'"\r\n'
            b"Content-Type: audio/wav\r\n"
            b"\r\n" + wav + b"\r\n"
            b"--" + boundary + b"--\r\n"
        )

        print(f"[Upload] → {self._url}  tg={tg}  src={source}  system={self._system}  file={fname}  dur={duration:.1f}s")
        req = urllib.request.Request(
            self._url,
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary.decode()}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.getcode()
            print(f"[Upload] ✓ HTTP {status}")
        except Exception as e:
            print(f"[Upload] Error posting {fname}: {e}", file=sys.stderr)


# ── Call detector (squelch state machine) ─────────────────────────────────────

class CallDetector:
    """Buffers PCM during active transmissions and triggers uploads when silent.

    States: IDLE → ACTIVE → HOLD → IDLE

    IDLE   Signal below threshold — nothing buffered.
    ACTIVE Signal above threshold — buffering call audio.
    HOLD   Signal dropped; waiting squelch_hold seconds before finalising.
           A new burst returns to ACTIVE, extending the same call.
    """

    _IDLE   = 0
    _ACTIVE = 1
    _HOLD   = 2

    def __init__(self, cfg: dict, uploader: Optional[CallUploader], get_tg=None):
        self._uploader     = uploader
        self._get_tg       = get_tg   # callable → (tg, src) or (None, None)
        self._threshold    = cfg.get("activity_threshold", ACTIVITY_THRESHOLD)
        self._hold_secs    = float(cfg.get("squelch_hold", 0.5))
        self._min_length   = float(cfg.get("min_call_length", 1.0))
        self._state        = self._IDLE
        self._buf          = bytearray()
        self._start        = 0.0
        self._hold_until   = 0.0
        self._call_tg:     Optional[int] = None
        self._call_src:    Optional[int] = None

        # Public state
        self.active    = False
        self.rx_count  = 0

    def feed(self, pcm: bytes):
        rms    = _rms(pcm)
        signal = rms > self._threshold
        now    = time.time()

        if self._state == self._IDLE:
            if signal:
                self._state = self._ACTIVE
                self._start = now
                self._buf   = bytearray(pcm)
                self.active = True
                self.rx_count += 1
                if self._get_tg:
                    self._call_tg, self._call_src = self._get_tg()
                else:
                    self._call_tg = self._call_src = None

        elif self._state == self._ACTIVE:
            self._buf.extend(pcm)
            if not signal:
                self._state      = self._HOLD
                self._hold_until = now + self._hold_secs

        elif self._state == self._HOLD:
            self._buf.extend(pcm)
            if signal:
                self._state = self._ACTIVE  # burst resumed — same call
            elif now >= self._hold_until:
                self._finalize()
                self._state = self._IDLE
                self.active = False

    def _finalize(self):
        pcm      = bytes(self._buf)
        duration = len(pcm) / (AUDIO_RATE * 2)
        if duration < self._min_length:
            return
        if self._uploader:
            threading.Thread(
                target=self._uploader.upload,
                args=(pcm, self._start, duration,
                      self._call_tg, self._call_src),
                daemon=True,
                name="call-upload",
            ).start()


# ── Audio post-processor ──────────────────────────────────────────────────────

class AudioProcessor:
    """Low-pass filter + gain trim for AMBE-decoded PCM.

    AMBE vocoder artefacts concentrate above the speech band (~3.5 kHz).
    A gentle LP removes the harshness; a gain trim gives headroom so peaks
    don't rail at 0 dBFS.
    """

    def __init__(self, rate: int = AUDIO_RATE, lp_hz: float = 3500.0,
                 gain: float = 0.80):
        self._gain       = float(gain)
        self._use_filter = False
        try:
            import numpy as np
            from scipy.signal import butter, lfilter_zi
            nyq          = rate / 2.0
            cutoff       = min(float(lp_hz), nyq - 1.0)
            b, a         = butter(2, cutoff / nyq, btype="low")
            self._b      = b
            self._a      = a
            self._zi     = lfilter_zi(b, a) * 0.0
            self._np     = np
            self._use_filter = True
        except ImportError:
            pass

    def process(self, data: bytes) -> bytes:
        if not self._use_filter:
            return data
        from scipy.signal import lfilter
        np      = self._np
        samples = np.frombuffer(data, dtype=np.int16).astype(np.float32) / 32768.0
        filt, self._zi = lfilter(self._b, self._a, samples, zi=self._zi)
        out     = np.clip(filt * self._gain, -1.0, 1.0)
        return (out * 32767.0).astype(np.int16).tobytes()


# ── Audio broadcaster (decoder thread → async consumers) ──────────────────────

class AudioBroadcaster:
    """Distributes real-time PCM from the decoder thread to asyncio consumers."""

    def __init__(self):
        self._clients: dict[int, asyncio.Queue] = {}
        self._lock    = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._next_id = 0

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def add(self) -> tuple[int, "asyncio.Queue[bytes]"]:
        q: asyncio.Queue = asyncio.Queue(maxsize=60)
        with self._lock:
            cid = self._next_id
            self._next_id += 1
            self._clients[cid] = q
        return cid, q

    def remove(self, cid: int):
        with self._lock:
            self._clients.pop(cid, None)

    def broadcast(self, pcm: bytes):
        if not self._loop:
            return
        with self._lock:
            queues = list(self._clients.values())
        for q in queues:
            try:
                self._loop.call_soon_threadsafe(q.put_nowait, pcm)
            except Exception:
                pass


# ── Icecast / Broadcastify feeder (optional real-time stream) ──────────────────

class IcecastFeeder:
    """Encodes decoded 16-bit mono PCM to MP3 and pushes to an Icecast server.

    Mirrors the scanner's BroadcastifyFeeder — self-generating silence between
    transmissions and auto-reconnecting on disconnect.
    """

    _RECONNECT_DELAY = 5.0

    def __init__(self, cfg: dict, label: str, on_status=None):
        server   = cfg["server"]
        port     = cfg.get("port", 80)
        password = cfg["password"]
        mount    = cfg["mountpoint"]
        self.url          = f"icecast://source:{password}@{server}:{port}{mount}"
        self.bitrate      = cfg.get("bitrate", 32)
        self.lp_cutoff    = cfg.get("lp_cutoff", 3500)
        self._label       = label
        self._meta_base   = f"http://{server}:{port}/admin/metadata"
        self._meta_mount  = mount
        self._admin_user  = cfg.get("admin_user", "admin")
        self._admin_pass  = cfg.get("admin_password", "")
        self._meta_delay  = cfg.get("metadata_delay", 0.0)
        self._on_status   = on_status
        self._q: _q_mod.Queue = _q_mod.Queue(maxsize=200)
        self._running     = False
        self._thread: Optional[threading.Thread] = None
        self.connected    = False
        self.last_error: Optional[str] = None

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True,
                                         name="icecast-feeder")
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=8.0)

    def send(self, pcm: bytes):
        """Queue a PCM chunk; drop oldest if the queue is full."""
        try:
            self._q.put_nowait(pcm)
        except _q_mod.Full:
            try:
                self._q.get_nowait()
            except _q_mod.Empty:
                pass
            try:
                self._q.put_nowait(pcm)
            except _q_mod.Full:
                pass

    def _notify(self, connected: bool, error: Optional[str] = None):
        self.connected  = connected
        self.last_error = error
        if self._on_status:
            try:
                self._on_status(connected, error)
            except Exception:
                pass

    def _run(self):
        try:
            import numpy as np
            from scipy.signal import butter, lfilter, lfilter_zi
            nyq    = AUDIO_RATE / 2.0
            cutoff = min(self.lp_cutoff, nyq - 1)
            lp_b, lp_a  = butter(2, cutoff / nyq, btype="low")
            _lp_zi_init = lfilter_zi(lp_b, lp_a) * 0.0

            def _filter(data: bytes, zi):
                samples = np.frombuffer(data, dtype=np.int16).astype(np.float32) / 32768.0
                out, zi = lfilter(lp_b, lp_a, samples, zi=zi)
                return (np.clip(out, -1.0, 1.0) * 32767).astype(np.int16).tobytes(), zi

        except ImportError:
            _lp_zi_init = None

            def _filter(data: bytes, zi):
                return data, zi

        chunk_secs = CHUNK_SIZE / (AUDIO_RATE * 2)

        while self._running:
            proc  = None
            lp_zi = _lp_zi_init
            try:
                cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-f", "s16le", "-ar", str(AUDIO_RATE), "-ac", "1",
                    "-i", "pipe:0",
                    "-c:a", "libmp3lame", "-b:a", f"{self.bitrate}k",
                    "-ice_name",        self._label,
                    "-ice_description", "DMR channel decoder",
                    "-ice_genre",       "Scanner",
                    "-content_type",    "audio/mpeg",
                    "-f", "mp3", self.url,
                ]
                proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                self._notify(True)
                print(f"[Icecast] Connected → {self.url.split('@')[-1]}")

                next_tick = time.monotonic()
                while self._running:
                    if proc.poll() is not None:
                        stderr = proc.stderr.read().decode(errors="replace").strip()
                        raise RuntimeError(f"ffmpeg exited: {stderr or '(no output)'}")
                    try:
                        chunk = self._q.get_nowait()
                    except _q_mod.Empty:
                        chunk = SILENCE
                    chunk, lp_zi = _filter(chunk, lp_zi)
                    try:
                        proc.stdin.write(chunk)
                        proc.stdin.flush()
                    except BrokenPipeError:
                        break
                    next_tick += chunk_secs
                    sleep = next_tick - time.monotonic()
                    if sleep > 0:
                        time.sleep(sleep)
                    elif sleep < -0.5:
                        next_tick = time.monotonic()

            except Exception as exc:
                err = str(exc)
                print(f"[Icecast] Error: {err}", file=sys.stderr)
                self._notify(False, err)
            else:
                self._notify(False)

            if proc is not None:
                try:
                    proc.stdin.close()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            if self._running:
                print(f"[Icecast] Reconnecting in {self._RECONNECT_DELAY:.0f} s…")
                time.sleep(self._RECONNECT_DELAY)

        self._notify(False)
        print("[Icecast] Feeder stopped.")


# ── DMR decoder pipeline ───────────────────────────────────────────────────────

class DMRDecoder:
    """Manages the rtl_fm | dsd subprocess pipeline.

    Each PCM chunk is forwarded to:
      - CallDetector  (squelch / call buffering / upload)
      - AudioBroadcaster  (real-time WebSocket / HTTP streaming)
      - IcecastFeeder  (optional Icecast stream)
    """

    def __init__(
        self,
        cfg: dict,
        broadcaster: AudioBroadcaster,
        detector: CallDetector,
        feeder: Optional[IcecastFeeder],
        debug: bool = False,
    ):
        self._cfg     = cfg
        self._bcast   = broadcaster
        self._detect  = detector
        self._feeder  = feeder
        self._debug   = debug
        self._proc    = AudioProcessor(
            lp_hz=cfg.get("audio_lp_hz", 3500.0),
            gain=cfg.get("audio_gain",   0.80),
        )
        self._rtlfm: Optional[subprocess.Popen] = None
        self._dsd:   Optional[subprocess.Popen] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_tg:  Optional[int] = None
        self._current_src: Optional[int] = None

    def get_tg(self) -> tuple[Optional[int], Optional[int]]:
        """Return the most recently decoded (talkgroup, source) pair."""
        return self._current_tg, self._current_src

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True,
                                         name="dmr-decoder")
        self._thread.start()

    def stop(self):
        self._running = False
        self._kill()
        if self._thread:
            self._thread.join(timeout=5)

    # ── command builders ───────────────────────────────────────────────────────

    def _rtlfm_cmd(self) -> list[str]:
        c    = self._cfg
        freq = int(float(c["frequency"]) * 1_000_000)
        cmd  = [
            "rtl_fm",
            "-f", str(freq),
            "-s", str(c.get("rtlfm_rate", 48000)),
            "-g", str(c.get("gain", "40")),
            "-p", str(c.get("ppm", 0)),
            "-M", c.get("rtlfm_mode", "fm"),
        ]
        dev = c.get("device", 0)
        if dev:
            cmd += ["-d", str(dev)]
        cmd += c.get("rtlfm_args", [])
        cmd.append("-")
        return cmd

    def _dsd_cmd(self) -> list[str]:
        c      = self._cfg
        binary = c.get("decoder", "dsd")
        fmt    = c.get("decoder_format", "gf")   # "gf" = guess, "d" = force DMR
        cmd    = [
            binary,
            "-f", fmt,
            "-i", "-",
            "-o", "-",
        ]
        cmd += c.get("decoder_args", [])
        return cmd

    # ── process management ─────────────────────────────────────────────────────

    def _kill(self):
        for proc in (self._dsd, self._rtlfm):
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        self._dsd = self._rtlfm = None

    def _read_stderr(self, proc: subprocess.Popen):
        """Parse dsd-fme stderr for TG/SRC; print to terminal only in debug."""
        try:
            for raw in proc.stderr:
                line = raw.decode(errors="replace").rstrip()
                m = _TG_RE.search(line)
                if m:
                    self._current_tg = int(m.group(1))
                m = _SRC_RE.search(line)
                if m:
                    self._current_src = int(m.group(1))
                if self._debug and not _NOISE_RE.search(line):
                    print(line, file=sys.stderr)
        except Exception:
            pass

    def _loop(self):
        while self._running:
            try:
                self._run()
            except Exception as e:
                print(f"[DMR] Pipeline error: {e}", file=sys.stderr)
            if self._running:
                print("[DMR] Restarting pipeline in 2 s…")
                time.sleep(2)

    def _run(self):
        rtlfm_cmd = self._rtlfm_cmd()
        dsd_cmd   = self._dsd_cmd()
        freq      = self._cfg.get("frequency", "?")
        label     = self._cfg.get("label", "")

        if self._debug:
            print(f"[DMR] rtl_fm: {' '.join(rtlfm_cmd)}")
            print(f"[DMR] dsd:    {' '.join(dsd_cmd)}")

        self._rtlfm = subprocess.Popen(
            rtlfm_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        self._dsd = subprocess.Popen(
            dsd_cmd,
            stdin=self._rtlfm.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,  # always capture — parse TG/SRC
        )
        self._rtlfm.stdout.close()   # propagate SIGPIPE when dsd dies

        threading.Thread(
            target=self._read_stderr,
            args=(self._dsd,),
            daemon=True,
            name="dsd-stderr",
        ).start()

        print(f"[DMR] Decoding {freq} MHz  {label}")

        try:
            fd      = self._dsd.stdout.fileno()
            buf     = bytearray()
            timeout = CHUNK_MS / 1000  # seconds

            while self._running:
                ready, _, _ = _select.select([fd], [], [], timeout)
                if ready:
                    got = os.read(fd, CHUNK_SIZE)
                    if not got:
                        break
                    buf.extend(got)
                else:
                    # dsd has no output between transmissions — tick the detector
                    buf.extend(SILENCE)

                while len(buf) >= CHUNK_SIZE:
                    chunk = self._proc.process(bytes(buf[:CHUNK_SIZE]))
                    del buf[:CHUNK_SIZE]
                    self._detect.feed(chunk)
                    self._bcast.broadcast(chunk)
                    if self._feeder:
                        self._feeder.send(chunk)

        finally:
            self._kill()
            print("[DMR] Pipeline stopped.")


# ── FastAPI application ────────────────────────────────────────────────────────

app      = FastAPI(title="DMR Decoder", docs_url=None, redoc_url=None)
_bcast   = AudioBroadcaster()
_dec:    Optional[DMRDecoder]    = None
_detect: Optional[CallDetector]  = None
_feeder: Optional[IcecastFeeder] = None
_cfg:    dict = {}

@app.on_event("startup")
async def _startup():
    _bcast.set_loop(asyncio.get_event_loop())

@app.get("/health")
def health():
    return {"ok": True, "active": bool(_detect and _detect.active)}

@app.get("/status")
def status():
    ice = _cfg.get("broadcastify", {})
    tg, src = _dec.get_tg() if _dec else (None, None)
    return {
        "name":        _cfg.get("name", "DMR Decoder"),
        "frequency":   _cfg.get("frequency"),
        "label":       _cfg.get("label"),
        "active":      bool(_detect and _detect.active),
        "rx_count":    _detect.rx_count if _detect else 0,
        "talkgroup":   tg,
        "source":      src,
        "audio_rate":  AUDIO_RATE,
        "icecast": {
            "enabled":   ice.get("enabled", False),
            "connected": _feeder.connected if _feeder else False,
            "error":     _feeder.last_error if _feeder else None,
        },
    }

@app.get("/stream")
async def wav_stream():
    """Live WAV stream.  Open with:  ffplay http://host:8081/stream"""
    async def generate():
        yield _wav_stream_header()
        cid, q = _bcast.add()
        try:
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    data = SILENCE
                yield data
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            _bcast.remove(cid)

    return StreamingResponse(
        generate(),
        media_type="audio/wav",
        headers={"Cache-Control": "no-cache", "Transfer-Encoding": "chunked"},
    )

@app.websocket("/ws/audio")
async def ws_audio(ws: WebSocket):
    """Raw 16-bit signed mono PCM at AUDIO_RATE Hz."""
    await ws.accept()
    cid, q = _bcast.add()
    try:
        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                data = SILENCE
            await ws.send_bytes(data)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _bcast.remove(cid)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    global _dec, _detect, _feeder, _cfg
    import urllib.parse  # ensure available for CallUploader

    parser = argparse.ArgumentParser(
        description="Headless conventional DMR channel decoder"
    )
    parser.add_argument("--config",       default=DEFAULT_CONFIG)
    parser.add_argument("--listen-port",  type=int)
    parser.add_argument("--debug",        action="store_true")
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        sys.exit(
            f"Config not found: {cfg_path}\n"
            f"Copy decoder_config.example.json → {cfg_path} and edit it."
        )

    with cfg_path.open() as f:
        _cfg = json.load(f)

    if args.listen_port:
        _cfg["port"] = args.listen_port

    freq_mhz = float(_cfg.get("frequency", 0))

    # Build dispatcher uploader
    uploader: Optional[CallUploader] = None
    disp_cfg = _cfg.get("dispatcher", {})
    if disp_cfg.get("url"):
        uploader = CallUploader(disp_cfg, freq_mhz)
        uploader.probe()

    # Build call detector (get_tg wired below after decoder is created)
    _detect = CallDetector(_cfg, uploader)

    # Start optional Icecast feeder
    ice_cfg = _cfg.get("broadcastify", {})
    if ice_cfg.get("enabled"):
        label   = _cfg.get("label", _cfg.get("name", "DMR"))
        _feeder = IcecastFeeder(ice_cfg, label)
        _feeder.start()

    # Start DMR decoder pipeline
    _dec = DMRDecoder(_cfg, _bcast, _detect, _feeder, debug=args.debug)
    _detect._get_tg = _dec.get_tg   # wire live TG into call detector
    _dec.start()

    host = _cfg.get("host", "0.0.0.0")
    port = int(_cfg.get("port", 8081))
    label_str = f"{freq_mhz} MHz  ({_cfg.get('label', '')})"

    print(f"[DMR Decoder] {_cfg.get('name', 'DMR')}  →  {label_str}")
    print(f"[DMR Decoder] Listening on {host}:{port}")
    print(f"[DMR Decoder]   /stream     live WAV stream")
    print(f"[DMR Decoder]   /ws/audio   raw PCM WebSocket")
    print(f"[DMR Decoder]   /status     JSON state")
    if uploader:
        print(f"[DMR Decoder]   Uploading calls → {disp_cfg['url']}")

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
