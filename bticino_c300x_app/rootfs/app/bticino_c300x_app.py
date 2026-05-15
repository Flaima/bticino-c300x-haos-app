#!/usr/bin/env python3
"""
BTicino C300X App for Home Assistant OS.

This app connects Home Assistant to a BTicino/Legrand Classe 300X/C300X
installation prepared with the custom firmware. It publishes MQTT Discovery
entities, translates the firmware MQTT frames into Home Assistant states,
provides safe command handling, and exposes a local web page
for live video, status and diagnostics.
"""

from __future__ import annotations

import json
import html
import mimetypes
import logging
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import paho.mqtt.client as mqtt

try:
    import paramiko
except Exception:  # pragma: no cover - handled at runtime
    paramiko = None  # type: ignore[assignment]

APP_NAME = "BTicino C300X App"
APP_VERSION = "1.0.0"
APP_ID = "bticino_c300x_app"

DEVICE_IDENTIFIERS = ["bticino_c300x"]
DEVICE_NAME = "BTicino C300X"
DEVICE_MODEL = "Classe 300X / C300X"
DEVICE_MANUFACTURER = "BTicino / Legrand"

DEFAULT_FIRMWARE_TX_TOPIC = "Bticino/tx"
DEFAULT_FIRMWARE_RX_TOPIC = "Bticino/rx"
DEFAULT_FIRMWARE_LWT_TOPIC = "Bticino/LastWillT"
DEFAULT_FIRMWARE_AVAILABILITY_TOPIC = "Bticino/status/availability"
DEFAULT_FIRMWARE_COMMAND_PREFIX = "Bticino/command"
DEFAULT_FIRMWARE_ACK_PREFIX = "Bticino/ack"
DEFAULT_DISCOVERY_PREFIX = "homeassistant"
DEFAULT_APP_TOPIC_PREFIX = "bticino_c300x_app"

DEFAULT_MQTT_PORT = 1883
DEFAULT_HTTP_PORT = 8099
DEFAULT_COMMAND_DELAY_SECONDS = 0.35
DEFAULT_RELEASE_DELAY_SECONDS = 0.75
DEFAULT_COMMAND_COOLDOWN_SECONDS = 10.0
DEFAULT_RING_HOLD_SECONDS = 5.0
DEFAULT_CALL_HOLD_SECONDS = 35.0
DEFAULT_SHORT_PULSE_SECONDS = 5.0
DEFAULT_STREAM_PORT = 5007
DEFAULT_STREAM_FPS = 25
DEFAULT_STREAM_WIDTH = 688
DEFAULT_STREAM_QUALITY = 6
DEFAULT_STREAM_START_COOLDOWN_SECONDS = 8.0
DEFAULT_STREAM_DIAGNOSTIC_INTERVAL_SECONDS = 5.0
DEFAULT_SSH_PORT = 22
DEFAULT_MEDIA_DIR = "/media/bticino/messages"
DEFAULT_AB_REMOTE_DIR = "/home/bticino/cfg/extra/47/messages"
DEFAULT_AB_SYNC_INTERVAL_SECONDS = 60


OPTIONS_PATH = Path("/data/options.json")
START_CODE = b"\x00\x00\x00\x01"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
_LOGGER = logging.getLogger(APP_ID)


@dataclass(frozen=True)
class AppConfig:
    bticino_host: str
    mqtt_host: str
    mqtt_username: str
    mqtt_password: str
    mqtt_port: int = DEFAULT_MQTT_PORT
    firmware_tx_topic: str = DEFAULT_FIRMWARE_TX_TOPIC
    firmware_rx_topic: str = DEFAULT_FIRMWARE_RX_TOPIC
    firmware_lwt_topic: str = DEFAULT_FIRMWARE_LWT_TOPIC
    firmware_availability_topic: str = DEFAULT_FIRMWARE_AVAILABILITY_TOPIC
    firmware_command_prefix: str = DEFAULT_FIRMWARE_COMMAND_PREFIX
    firmware_ack_prefix: str = DEFAULT_FIRMWARE_ACK_PREFIX
    discovery_prefix: str = DEFAULT_DISCOVERY_PREFIX
    app_topic_prefix: str = DEFAULT_APP_TOPIC_PREFIX
    http_port: int = DEFAULT_HTTP_PORT
    stream_udp_port: int = DEFAULT_STREAM_PORT
    log_level: str = "info"
    video_mode: str = "ssh_loopback"
    ssh_username: str = ""
    ssh_password: str = ""
    ssh_port: int = DEFAULT_SSH_PORT
    media_path: str = DEFAULT_MEDIA_DIR
    ab_remote_path: str = DEFAULT_AB_REMOTE_DIR
    ab_sync_interval: int = DEFAULT_AB_SYNC_INTERVAL_SECONDS


def _read_options() -> Dict[str, Any]:
    if OPTIONS_PATH.exists():
        with OPTIONS_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
        raise ValueError("/data/options.json is not a JSON object")

    return {
        "bticino_host": os.environ.get("BTICINO_HOST", ""),
        "mqtt_host": os.environ.get("MQTT_HOST", ""),
        "mqtt_username": os.environ.get("MQTT_USERNAME", ""),
        "mqtt_password": os.environ.get("MQTT_PASSWORD", ""),
        "log_level": os.environ.get("LOG_LEVEL", "info"),
        "video_mode": os.environ.get("VIDEO_MODE", "ssh_loopback"),
        "ssh_username": os.environ.get("BTICINO_SSH_USERNAME", "root2"),
        "ssh_password": os.environ.get("BTICINO_SSH_PASSWORD", "pwned123"),
        "media_path": os.environ.get("BTICINO_MEDIA_PATH", DEFAULT_MEDIA_DIR),
        "ab_remote_path": os.environ.get("BTICINO_AB_REMOTE_PATH", DEFAULT_AB_REMOTE_DIR),
        "ab_sync_interval": os.environ.get("BTICINO_AB_SYNC_INTERVAL", str(DEFAULT_AB_SYNC_INTERVAL_SECONDS)),
    }


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_from_options(options: Dict[str, Any], key: str, default: int) -> int:
    value = options.get(key, default)
    try:
        return int(value)
    except Exception:
        return default


def load_config() -> AppConfig:
    options = _read_options()

    bticino_host = _clean_text(options.get("bticino_host"))
    mqtt_host = _clean_text(options.get("mqtt_host"))
    mqtt_username = _clean_text(options.get("mqtt_username"))
    mqtt_password = _clean_text(options.get("mqtt_password"))
    log_level = _clean_text(options.get("log_level", "info")).lower()
    if log_level not in {"info", "debug"}:
        log_level = "info"

    video_mode = _clean_text(options.get("video_mode", "ssh_loopback")).lower()
    if video_mode not in {"ssh_loopback", "mqtt_direct", "off"}:
        video_mode = "ssh_loopback"

    ssh_username = _clean_text(options.get("ssh_username", "root2")) or "root2"
    ssh_password = _clean_text(options.get("ssh_password", "pwned123")) or "pwned123"
    media_path = _clean_text(options.get("media_path", DEFAULT_MEDIA_DIR)) or DEFAULT_MEDIA_DIR
    ab_remote_path = _clean_text(options.get("ab_remote_path", DEFAULT_AB_REMOTE_DIR)) or DEFAULT_AB_REMOTE_DIR

    missing: List[str] = []
    if not bticino_host:
        missing.append("BTicino-IP")
    if not mqtt_host:
        missing.append("MQTT-Host/IP")
    if not mqtt_username:
        missing.append("MQTT-Benutzername")
    if not mqtt_password:
        missing.append("MQTT-Passwort")

    if missing:
        raise ValueError(
            "Pflichtfelder fehlen: "
            + ", ".join(missing)
            + ". Bitte App-Konfiguration öffnen und BTicino-IP, MQTT-Host/IP, MQTT-Benutzername und MQTT-Passwort eintragen."
        )

    return AppConfig(
        bticino_host=bticino_host,
        mqtt_host=mqtt_host,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_port=_int_from_options(options, "mqtt_port", DEFAULT_MQTT_PORT),
        http_port=_int_from_options(options, "http_port", DEFAULT_HTTP_PORT),
        stream_udp_port=_int_from_options(options, "stream_udp_port", DEFAULT_STREAM_PORT),
        log_level=log_level,
        video_mode=video_mode,
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        ssh_port=_int_from_options(options, "ssh_port", DEFAULT_SSH_PORT),
        media_path=media_path,
        ab_remote_path=ab_remote_path,
        ab_sync_interval=_int_from_options(options, "ab_sync_interval", DEFAULT_AB_SYNC_INTERVAL_SECONDS),
    )


class TimerState:
    def __init__(self, callback: Any) -> None:
        self._callback = callback
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def start(self, delay_seconds: float) -> None:
        self.cancel()
        with self._lock:
            self._timer = threading.Timer(delay_seconds, self._callback)
            self._timer.daemon = True
            self._timer.start()

    def cancel(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None


def read_exact(stream: Any, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = stream.read(size - len(data))
        if not chunk:
            return b""
        data.extend(chunk)
    return bytes(data)


def parse_pcap_header(stream: Any) -> Tuple[str, int]:
    header = read_exact(stream, 24)
    if len(header) != 24:
        raise RuntimeError("No valid PCAP header received")

    magic = header[:4]
    if magic == b"\xd4\xc3\xb2\xa1":
        endian = "<"
    elif magic == b"\xa1\xb2\xc3\xd4":
        endian = ">"
    elif magic == b"\x4d\x3c\xb2\xa1":
        endian = "<"
    elif magic == b"\xa1\xb2\x3c\x4d":
        endian = ">"
    else:
        raise RuntimeError(f"Unknown PCAP magic header: {magic.hex()}")

    _version_major, _version_minor, _thiszone, _sigfigs, _snaplen, network = struct.unpack(
        endian + "HHiiII", header[4:24]
    )
    return endian, network


def iter_pcap_packets(stream: Any) -> Iterable[Tuple[int, bytes]]:
    endian, linktype = parse_pcap_header(stream)

    while True:
        packet_header = read_exact(stream, 16)
        if not packet_header:
            return
        if len(packet_header) != 16:
            return

        _ts_sec, _ts_usec, incl_len, _orig_len = struct.unpack(endian + "IIII", packet_header)
        if incl_len <= 0 or incl_len > 10_000_000:
            return

        packet = read_exact(stream, incl_len)
        if len(packet) != incl_len:
            return

        yield linktype, packet


def extract_udp_payload(linktype: int, frame: bytes) -> Optional[Tuple[int, int, bytes]]:
    offset: Optional[int] = None

    if linktype == 1:
        if len(frame) < 14:
            return None
        ethertype = int.from_bytes(frame[12:14], "big")
        offset = 14
        while ethertype in (0x8100, 0x88A8) and len(frame) >= offset + 4:
            ethertype = int.from_bytes(frame[offset + 2 : offset + 4], "big")
            offset += 4
        if ethertype != 0x0800:
            return None
    elif linktype == 113:
        if len(frame) < 16:
            return None
        protocol = int.from_bytes(frame[14:16], "big")
        if protocol != 0x0800:
            return None
        offset = 16
    elif linktype == 101:
        offset = 0
    else:
        return None

    if offset is None or len(frame) < offset + 20:
        return None

    ip = frame[offset:]
    version = ip[0] >> 4
    if version != 4:
        return None

    ihl = (ip[0] & 0x0F) * 4
    if ihl < 20 or len(ip) < ihl + 8:
        return None

    protocol = ip[9]
    if protocol != 17:
        return None

    udp = ip[ihl:]
    if len(udp) < 8:
        return None

    src_port = int.from_bytes(udp[0:2], "big")
    dst_port = int.from_bytes(udp[2:4], "big")
    udp_len = int.from_bytes(udp[4:6], "big")
    if udp_len < 8:
        return None

    payload = udp[8:udp_len]
    return src_port, dst_port, payload


def parse_rtp_payload(packet: bytes) -> Optional[Dict[str, Any]]:
    if len(packet) < 12:
        return None

    version = packet[0] >> 6
    if version != 2:
        return None

    padding = bool(packet[0] & 0x20)
    extension = bool(packet[0] & 0x10)
    csrc_count = packet[0] & 0x0F
    header_len = 12 + (4 * csrc_count)

    if len(packet) < header_len:
        return None

    if extension:
        if len(packet) < header_len + 4:
            return None
        ext_len_words = int.from_bytes(packet[header_len + 2 : header_len + 4], "big")
        header_len += 4 + (ext_len_words * 4)
        if len(packet) < header_len:
            return None

    payload_end = len(packet)
    if padding:
        pad_len = packet[-1]
        if 0 < pad_len < len(packet):
            payload_end -= pad_len

    if payload_end <= header_len:
        return None

    return {
        "payload_type": packet[1] & 0x7F,
        "sequence": int.from_bytes(packet[2:4], "big"),
        "timestamp": int.from_bytes(packet[4:8], "big"),
        "ssrc": int.from_bytes(packet[8:12], "big"),
        "payload": packet[header_len:payload_end],
    }


class H264RtpDepay:
    def __init__(self) -> None:
        self.in_fu = False
        self.nal_count = 0

    def process(self, rtp_payload: bytes) -> List[bytes]:
        output: List[bytes] = []
        if not rtp_payload:
            return output

        nal_header = rtp_payload[0]
        nal_type = nal_header & 0x1F

        if 1 <= nal_type <= 23:
            output.append(START_CODE + rtp_payload)
            self.in_fu = False
            self.nal_count += 1
            return output

        if nal_type == 24:
            pos = 1
            while pos + 2 <= len(rtp_payload):
                size = int.from_bytes(rtp_payload[pos : pos + 2], "big")
                pos += 2
                if size <= 0:
                    break
                if pos + size > len(rtp_payload):
                    break
                output.append(START_CODE + rtp_payload[pos : pos + size])
                self.nal_count += 1
                pos += size
            self.in_fu = False
            return output

        if nal_type == 28 and len(rtp_payload) >= 2:
            fu_indicator = rtp_payload[0]
            fu_header = rtp_payload[1]
            start = bool(fu_header & 0x80)
            end = bool(fu_header & 0x40)
            reconstructed_type = fu_header & 0x1F
            reconstructed_header = bytes([(fu_indicator & 0xE0) | reconstructed_type])

            if start:
                output.append(START_CODE + reconstructed_header + rtp_payload[2:])
                self.in_fu = True
                self.nal_count += 1
            else:
                if self.in_fu:
                    output.append(rtp_payload[2:])

            if end:
                self.in_fu = False
            return output

        self.in_fu = False
        return output


def iter_jpeg_frames(stream: Any, stop_event: threading.Event) -> Iterable[bytes]:
    buffer = bytearray()
    while not stop_event.is_set():
        chunk = stream.read(4096)
        if not chunk:
            return
        buffer.extend(chunk)

        while True:
            start = buffer.find(b"\xff\xd8")
            if start < 0:
                if len(buffer) > 1024 * 1024:
                    del buffer[:-2]
                break
            end = buffer.find(b"\xff\xd9", start + 2)
            if end < 0:
                if start > 0:
                    del buffer[:start]
                break
            frame = bytes(buffer[start : end + 2])
            del buffer[: end + 2]
            yield frame


class MQTTFirmwareVideoRelay:
    def __init__(self, config: AppConfig, mqtt_client: mqtt.Client, topic_prefix: str) -> None:
        self.config = config
        self.mqtt_client = mqtt_client
        self.topic_prefix = topic_prefix
        self.lock = threading.RLock()
        self.frame_condition = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.active_clients = 0
        self.started = False
        self.last_jpeg: Optional[bytes] = None
        self.last_error = "not started"
        self.last_camera_start = 0.0
        self.udp_thread: Optional[threading.Thread] = None
        self.jpeg_thread: Optional[threading.Thread] = None
        self.ffmpeg_proc: Optional[subprocess.Popen[bytes]] = None
        self.depay = H264RtpDepay()
        self.udp_packets = 0
        self.rtp_packets = 0
        self.h264_bytes = 0
        self.jpeg_frames = 0
        self.last_target_ip = ""
        self.last_video_command = ""
        self.camera_start_count = 0
        self.last_camera_start_result = "not started"
        self.last_publish_mid = 0
        self.last_publish_rc = -1
        self.last_stream_start_reason = ""
        self.non_rtp_packets = 0
        self.rtp_payload_type_96_packets = 0
        self.last_udp_source = ""
        self.last_udp_packet_size = 0
        self.last_rtp_payload_type = -1
        self.last_rtp_sequence = -1
        self.last_rtp_timestamp = -1
        self.first_udp_seen_at = 0.0
        self.first_rtp_seen_at = 0.0
        self.last_udp_seen_at = 0.0
        self.last_jpeg_seen_at = 0.0
        self.ffmpeg_stderr_tail: List[str] = []
        self.ffmpeg_stderr_thread: Optional[threading.Thread] = None
        self.last_diagnostic_log = 0.0
        self.ssh_client: Any = None
        self.ssh_channel: Any = None
        self.ssh_capture_packets = 0
        self.last_ssh_status = "not connected"

    def topic(self, suffix: str) -> str:
        return f"{self.topic_prefix}/{suffix.strip('/')}"

    def status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "active_clients": self.active_clients,
                "started": self.started,
                "last_error": self.last_error,
                "stream_udp_port": self.config.stream_udp_port,
                "udp_packets": self.udp_packets,
                "rtp_packets": self.rtp_packets,
                "h264_bytes": self.h264_bytes,
                "jpeg_frames": self.jpeg_frames,
                "mode": self.config.video_mode,
                "last_target_ip": self.last_target_ip,
                "last_video_command": self.last_video_command,
                "camera_start_count": self.camera_start_count,
                "last_camera_start_result": self.last_camera_start_result,
                "last_publish_mid": self.last_publish_mid,
                "last_publish_rc": self.last_publish_rc,
                "last_stream_start_reason": self.last_stream_start_reason,
                "non_rtp_packets": self.non_rtp_packets,
                "rtp_payload_type_96_packets": self.rtp_payload_type_96_packets,
                "last_udp_source": self.last_udp_source,
                "last_udp_packet_size": self.last_udp_packet_size,
                "last_rtp_payload_type": self.last_rtp_payload_type,
                "last_rtp_sequence": self.last_rtp_sequence,
                "last_rtp_timestamp": self.last_rtp_timestamp,
                "seconds_since_first_udp": round(time.time() - self.first_udp_seen_at, 1) if self.first_udp_seen_at else None,
                "seconds_since_last_udp": round(time.time() - self.last_udp_seen_at, 1) if self.last_udp_seen_at else None,
                "seconds_since_last_jpeg": round(time.time() - self.last_jpeg_seen_at, 1) if self.last_jpeg_seen_at else None,
                "ffmpeg_stderr_tail": list(self.ffmpeg_stderr_tail),
                "ssh_capture_packets": self.ssh_capture_packets,
                "last_ssh_status": self.last_ssh_status,
            }

    def autodetect_target_ip(self) -> str:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect((self.config.bticino_host, 20000))
            address = sock.getsockname()[0]
            if not address or address.startswith("127."):
                raise RuntimeError("detected loopback address")
            return address
        finally:
            sock.close()

    @staticmethod
    def read_openwebnet_response(sock: socket.socket, timeout_seconds: float = 2.0) -> str:
        sock.settimeout(timeout_seconds)
        data = bytearray()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(1024)
            except socket.timeout:
                break
            if not chunk:
                break
            data.extend(chunk)
            if b"##" in data:
                break
        return data.decode("utf-8", errors="replace")

    def send_video_start_command(self, frame: str) -> None:
        """Request the video stream through the firmware MQTT command channel."""
        _LOGGER.info("Sending live video request through firmware MQTT topic %s", self.config.firmware_rx_topic)
        _LOGGER.info("Live video request frame: %s", frame)
        info = self.mqtt_client.publish(self.config.firmware_rx_topic, frame, qos=0, retain=False)
        with self.lock:
            self.last_publish_mid = int(getattr(info, "mid", 0) or 0)
            self.last_publish_rc = int(getattr(info, "rc", -1))
        _LOGGER.debug(
            "Live video MQTT publish result: mid=%s rc=%s",
            getattr(info, "mid", None),
            getattr(info, "rc", None),
        )

    def internal_loopback_video_frame(self) -> str:
        port = int(self.config.stream_udp_port)
        if port == 5002:
            return "*7*300#127#0#0#1#5002#1*##"
        return "*7*300#127#0#0#1#5007#0*##"

    def profile_from_port(self) -> str:
        return "low" if int(self.config.stream_udp_port) == 5002 else "high"

    def send_livestream_start_command(self, profile: str) -> None:
        request_id = f"ha-stream-{uuid.uuid4().hex}"
        payload = {"request_id": request_id, "profile": profile}
        topic = f"{self.config.firmware_command_prefix}/livestream/start"
        _LOGGER.info("Sending firmware livestream start command: topic=%s payload=%s", topic, payload)
        info = self.mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), qos=0, retain=False)
        with self.lock:
            self.last_publish_mid = int(getattr(info, "mid", 0) or 0)
            self.last_publish_rc = int(getattr(info, "rc", -1))

    def start_camera_session(self, reason: str = "manual") -> None:
        now = time.monotonic()
        with self.lock:
            if now - self.last_camera_start < DEFAULT_STREAM_START_COOLDOWN_SECONDS:
                remaining = DEFAULT_STREAM_START_COOLDOWN_SECONDS - (now - self.last_camera_start)
                _LOGGER.debug("Livestream start suppressed by cooldown, remaining %.1fs, reason=%s", remaining, reason)
                return
            self.last_camera_start = now
            self.last_stream_start_reason = reason
            self.camera_start_count += 1

        if self.config.video_mode == "off":
            with self.lock:
                self.last_camera_start_result = "video mode off"
                self.last_error = "video mode off"
            _LOGGER.info("Livestream start skipped: video_mode=off")
            return

        if self.config.video_mode == "ssh_loopback":
            profile = self.profile_from_port()
            command = self.internal_loopback_video_frame()
            with self.lock:
                self.last_target_ip = "127.0.0.1"
                self.last_video_command = command
                self.last_error = "sending firmware loopback livestream request"
            _LOGGER.info(
                "Starting SSH loopback video stream: bticino=%s tcpdump_dst_port=%s profile=%s reason=%s",
                self.config.bticino_host,
                self.config.stream_udp_port,
                profile,
                reason,
            )
            self.send_livestream_start_command(profile)
            # Extra local fallback: exact same loopback frame the firmware command sends.
            self.send_video_start_command_over_ssh(command)
            with self.lock:
                self.last_camera_start_result = "Firmware MQTT loopback video request sent"
                self.last_error = "waiting for SSH/tcpdump loopback RTP/H264 video"
            return

        target_ip = self.autodetect_target_ip()
        octets = target_ip.split(".")
        if len(octets) != 4 or not all(part.isdigit() and 0 <= int(part) <= 255 for part in octets):
            raise RuntimeError(f"invalid detected target IP: {target_ip}")

        command = f"*6*32#{octets[0]}#{octets[1]}#{octets[2]}#{octets[3]}#{self.config.stream_udp_port}*4000##"
        with self.lock:
            self.last_target_ip = target_ip
            self.last_video_command = command
            self.last_error = "sending camera start request"

        _LOGGER.info(
            "Starting MQTT-controlled direct video stream: target_ip=%s udp_port=%s reason=%s",
            target_ip,
            self.config.stream_udp_port,
            reason,
        )
        self.send_video_start_command(command)
        with self.lock:
            self.last_camera_start_result = "MQTT direct video request sent"
            self.last_error = "waiting for RTP/H264 video"

    def require_ssh_config(self) -> None:
        if paramiko is None:
            raise RuntimeError("paramiko is not available in the app image")
        if not self.config.ssh_username or not self.config.ssh_password:
            raise RuntimeError("SSH username/password missing in app configuration")

    def open_ssh_client(self) -> Any:
        self.require_ssh_config()
        assert paramiko is not None
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        _LOGGER.debug(
            "Opening SSH connection to BTicino: host=%s port=%s user=%s",
            self.config.bticino_host,
            self.config.ssh_port,
            self.config.ssh_username,
        )
        client.connect(
            hostname=self.config.bticino_host,
            port=self.config.ssh_port,
            username=self.config.ssh_username,
            password=self.config.ssh_password,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
            banner_timeout=10,
            auth_timeout=10,
        )
        return client

    @staticmethod
    def shell_quote_single(value: str) -> str:
        return "'" + value.replace("'", "'\''") + "'"

    def send_video_start_command_over_ssh(self, frame: str) -> None:
        """Send one video request locally on the BTicino using netcat when SSH is available."""
        try:
            client = self.open_ssh_client()
        except Exception as exc:
            with self.lock:
                self.last_ssh_status = f"SSH connect failed: {exc}"
                self.last_camera_start_result = self.last_ssh_status
            _LOGGER.warning("SSH video start failed before command: %s", exc)
            return

        quoted = self.shell_quote_single(frame)
        remote_script = (
            "set +e; "
            "if command -v nc >/dev/null 2>&1; then "
            f"printf %s {quoted} | nc 0 30006 >/dev/null 2>&1 || "
            f"printf %s {quoted} | nc 127.0.0.1 30006 >/dev/null 2>&1 || true; "
            "else echo __NO_NC__; fi"
        )
        try:
            _LOGGER.info("Sending local BTicino video request through SSH/nc: %s", frame)
            _stdin, stdout, stderr = client.exec_command(remote_script, timeout=8)
            out = stdout.read().decode("utf-8", errors="replace").strip()
            err = stderr.read().decode("utf-8", errors="replace").strip()
            with self.lock:
                self.last_ssh_status = f"video start command sent; stdout={out!r}; stderr={err!r}"
            if out or err:
                _LOGGER.debug("SSH video start output: stdout=%r stderr=%r", out, err)
        except Exception as exc:
            with self.lock:
                self.last_ssh_status = f"SSH video command failed: {exc}"
            _LOGGER.warning("SSH video start command failed: %s", exc)
        finally:
            try:
                client.close()
            except Exception:
                pass

    def ensure_started(self) -> None:
        with self.lock:
            if self.started:
                return
            self.started = True
            self.stop_event.clear()
            self.last_error = "waiting for RTP/H264 video"
            self.udp_packets = 0
            self.rtp_packets = 0
            self.h264_bytes = 0
            self.jpeg_frames = 0
            self.non_rtp_packets = 0
            self.rtp_payload_type_96_packets = 0
            self.first_udp_seen_at = 0.0
            self.first_rtp_seen_at = 0.0
            self.last_udp_seen_at = 0.0
            self.last_jpeg_seen_at = 0.0
            self.ffmpeg_stderr_tail = []
            self.depay = H264RtpDepay()
            _LOGGER.debug("Starting video relay threads for mode=%s port=%s", self.config.video_mode, self.config.stream_udp_port)
            if self.config.video_mode == "ssh_loopback":
                video_target = self.ssh_pcap_to_ffmpeg_loop
                thread_name = "bticino-ssh-loopback-video"
            else:
                video_target = self.udp_to_ffmpeg_loop
                thread_name = "bticino-udp-video"
            self.udp_thread = threading.Thread(target=video_target, name=thread_name, daemon=True)
            self.jpeg_thread = threading.Thread(target=self.ffmpeg_to_jpeg_loop, name="bticino-jpeg-video", daemon=True)
            self.udp_thread.start()
            self.jpeg_thread.start()

    def start_ffmpeg(self) -> subprocess.Popen[bytes]:
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "nobuffer",
            "-flags",
            "low_delay",
            "-f",
            "h264",
            "-framerate",
            str(DEFAULT_STREAM_FPS),
            "-i",
            "pipe:0",
            "-an",
            "-vf",
            "fps=5",
            "-q:v",
            str(DEFAULT_STREAM_QUALITY),
            "-f",
            "mjpeg",
            "pipe:1",
        ]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        return proc

    def ffmpeg_stderr_loop(self, proc: subprocess.Popen[bytes]) -> None:
        if proc.stderr is None:
            return
        try:
            for raw_line in iter(proc.stderr.readline, b""):
                if not raw_line:
                    break
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                with self.lock:
                    self.ffmpeg_stderr_tail.append(line)
                    self.ffmpeg_stderr_tail = self.ffmpeg_stderr_tail[-12:]
                _LOGGER.debug("ffmpeg: %s", line)
                if self.stop_event.is_set():
                    break
        except Exception as exc:
            _LOGGER.debug("ffmpeg stderr reader stopped: %s", exc)

    def log_video_diagnostics(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_diagnostic_log < DEFAULT_STREAM_DIAGNOSTIC_INTERVAL_SECONDS:
            return
        self.last_diagnostic_log = now
        status = self.status()
        _LOGGER.debug("Video diagnostics: %s", json.dumps(status, ensure_ascii=False, separators=(",", ":")))

    def ssh_pcap_to_ffmpeg_loop(self) -> None:
        """Read the known working internal RTP/H264 loopback stream through SSH/tcpdump."""
        if paramiko is None:
            with self.lock:
                self.last_error = "paramiko is not available"
                self.last_ssh_status = "paramiko is not available"
            _LOGGER.warning("SSH loopback stream cannot start: paramiko is not available")
            with self.lock:
                self.started = False
                self.frame_condition.notify_all()
            return

        client: Any = None
        channel: Any = None
        proc: Optional[subprocess.Popen[bytes]] = None
        try:
            client = self.open_ssh_client()
            with self.lock:
                self.ssh_client = client
                self.last_ssh_status = "SSH connected"
            transport = client.get_transport()
            if transport is None:
                raise RuntimeError("SSH transport not available")
            channel = transport.open_session()
            with self.lock:
                self.ssh_channel = channel
            tcpdump_cmd = f"/usr/sbin/tcpdump -i lo -U -s 0 -w - 'udp dst port {int(self.config.stream_udp_port)}'"
            _LOGGER.info("Starting SSH/tcpdump loopback capture: %s", tcpdump_cmd)
            channel.exec_command(tcpdump_cmd)
            stream = channel.makefile("rb", 0)
            proc = self.start_ffmpeg()
            with self.lock:
                self.ffmpeg_proc = proc
                self.ffmpeg_stderr_thread = threading.Thread(
                    target=self.ffmpeg_stderr_loop,
                    args=(proc,),
                    name="bticino-ffmpeg-stderr",
                    daemon=True,
                )
                self.ffmpeg_stderr_thread.start()
                self.last_error = "SSH/tcpdump running, waiting for RTP/H264"
            _LOGGER.debug("FFmpeg process started for SSH loopback H264->MJPEG conversion")
            assert proc.stdin is not None

            for linktype, frame in iter_pcap_packets(stream):
                if self.stop_event.is_set():
                    break
                extracted = extract_udp_payload(linktype, frame)
                if extracted is None:
                    continue
                src_port, dst_port, udp_payload = extracted
                if int(dst_port) != int(self.config.stream_udp_port):
                    continue
                now_wall = time.time()
                with self.lock:
                    self.ssh_capture_packets += 1
                    self.udp_packets += 1
                    self.last_udp_source = f"127.0.0.1:{src_port}"
                    self.last_udp_packet_size = len(udp_payload)
                    self.last_udp_seen_at = now_wall
                    if not self.first_udp_seen_at:
                        self.first_udp_seen_at = now_wall
                        _LOGGER.info("First SSH/tcpdump UDP packet received: src=%s dst=%s size=%s", src_port, dst_port, len(udp_payload))

                rtp = parse_rtp_payload(udp_payload)
                if rtp is None:
                    with self.lock:
                        self.non_rtp_packets += 1
                    self.log_video_diagnostics()
                    continue

                with self.lock:
                    self.rtp_packets += 1
                    self.last_rtp_payload_type = int(rtp["payload_type"])
                    self.last_rtp_sequence = int(rtp["sequence"])
                    self.last_rtp_timestamp = int(rtp["timestamp"])
                    if int(rtp["payload_type"]) == 96:
                        self.rtp_payload_type_96_packets += 1
                    if not self.first_rtp_seen_at:
                        self.first_rtp_seen_at = now_wall
                        _LOGGER.info(
                            "First SSH/tcpdump RTP packet received: payload_type=%s sequence=%s",
                            rtp["payload_type"],
                            rtp["sequence"],
                        )

                chunks = self.depay.process(rtp["payload"])
                if not chunks:
                    self.log_video_diagnostics()
                    continue
                try:
                    for chunk in chunks:
                        proc.stdin.write(chunk)
                        with self.lock:
                            self.h264_bytes += len(chunk)
                    proc.stdin.flush()
                    with self.lock:
                        self.last_error = "receiving SSH loopback video"
                        self.last_ssh_status = "receiving SSH/tcpdump video"
                    self.log_video_diagnostics()
                except Exception as exc:
                    with self.lock:
                        self.last_error = f"ffmpeg input failed: {exc}"
                    break
        except Exception as exc:
            with self.lock:
                self.last_error = f"SSH loopback video failed: {exc}"
                self.last_ssh_status = f"failed: {exc}"
            _LOGGER.warning("SSH loopback video failed: %s", exc)
        finally:
            try:
                if channel is not None:
                    channel.close()
            except Exception:
                pass
            try:
                if client is not None:
                    client.close()
            except Exception:
                pass
            self.stop_process()
            with self.lock:
                self.ssh_client = None
                self.ssh_channel = None
                self.started = False
                self.frame_condition.notify_all()

    def udp_to_ffmpeg_loop(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)
        try:
            sock.bind(("0.0.0.0", self.config.stream_udp_port))
            _LOGGER.info("UDP video receiver bound on 0.0.0.0:%s", self.config.stream_udp_port)
            proc = self.start_ffmpeg()
            with self.lock:
                self.ffmpeg_proc = proc
                self.ffmpeg_stderr_thread = threading.Thread(target=self.ffmpeg_stderr_loop, args=(proc,), name="bticino-ffmpeg-stderr", daemon=True)
                self.ffmpeg_stderr_thread.start()
            _LOGGER.debug("FFmpeg process started for H264->MJPEG conversion")
            assert proc.stdin is not None

            while not self.stop_event.is_set():
                try:
                    packet, addr = sock.recvfrom(65535)
                except socket.timeout:
                    self.log_video_diagnostics()
                    continue
                except OSError as exc:
                    with self.lock:
                        self.last_error = f"UDP receive failed: {exc}"
                    break

                now_wall = time.time()
                with self.lock:
                    self.udp_packets += 1
                    self.last_udp_source = f"{addr[0]}:{addr[1]}"
                    self.last_udp_packet_size = len(packet)
                    self.last_udp_seen_at = now_wall
                    if not self.first_udp_seen_at:
                        self.first_udp_seen_at = now_wall
                        _LOGGER.info("First UDP packet received from %s:%s size=%s", addr[0], addr[1], len(packet))

                rtp = parse_rtp_payload(packet)
                if rtp is None:
                    with self.lock:
                        self.non_rtp_packets += 1
                    self.log_video_diagnostics()
                    continue

                with self.lock:
                    self.rtp_packets += 1
                    self.last_rtp_payload_type = int(rtp["payload_type"])
                    self.last_rtp_sequence = int(rtp["sequence"])
                    self.last_rtp_timestamp = int(rtp["timestamp"])
                    if int(rtp["payload_type"]) == 96:
                        self.rtp_payload_type_96_packets += 1
                    if not self.first_rtp_seen_at:
                        self.first_rtp_seen_at = now_wall
                        _LOGGER.info(
                            "First RTP packet received: payload_type=%s sequence=%s source=%s:%s",
                            rtp["payload_type"],
                            rtp["sequence"],
                            addr[0],
                            addr[1],
                        )

                chunks = self.depay.process(rtp["payload"])
                if not chunks:
                    self.log_video_diagnostics()
                    continue

                try:
                    for chunk in chunks:
                        proc.stdin.write(chunk)
                        with self.lock:
                            self.h264_bytes += len(chunk)
                    proc.stdin.flush()
                    with self.lock:
                        self.last_error = "receiving video"
                    self.log_video_diagnostics()
                except Exception as exc:
                    with self.lock:
                        self.last_error = f"ffmpeg input failed: {exc}"
                    break
        except Exception as exc:
            with self.lock:
                self.last_error = str(exc)
            _LOGGER.warning("Direct video relay failed: %s", exc)
        finally:
            try:
                sock.close()
            except Exception:
                pass
            self.stop_process()
            with self.lock:
                self.started = False
                self.frame_condition.notify_all()

    def ffmpeg_to_jpeg_loop(self) -> None:
        while not self.stop_event.is_set():
            proc: Optional[subprocess.Popen[bytes]]
            with self.lock:
                proc = self.ffmpeg_proc
            if proc is None or proc.stdout is None:
                time.sleep(0.1)
                continue
            try:
                for frame in iter_jpeg_frames(proc.stdout, self.stop_event):
                    if not frame:
                        continue
                    with self.lock:
                        self.last_jpeg = frame
                        self.jpeg_frames += 1
                        self.last_jpeg_seen_at = time.time()
                        self.last_error = "streaming"
                        self.frame_condition.notify_all()
                    try:
                        self.mqtt_client.publish(self.topic("camera/latest_jpeg"), frame, qos=0, retain=False)
                    except Exception:
                        pass
                    if self.stop_event.is_set():
                        break
            except Exception as exc:
                with self.lock:
                    self.last_error = f"ffmpeg output failed: {exc}"
            time.sleep(0.2)

    def stop_process(self) -> None:
        proc: Optional[subprocess.Popen[bytes]]
        with self.lock:
            proc = self.ffmpeg_proc
            self.ffmpeg_proc = None
        if proc is None:
            return
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def stream_mjpeg(self, handler: BaseHTTPRequestHandler) -> None:
        self.ensure_started()
        try:
            self.start_camera_session()
        except Exception as exc:
            with self.lock:
                self.last_error = f"camera start failed: {exc}"
            _LOGGER.warning("Camera start failed: %s", exc)

        boundary = "bticino-c300x-frame"
        with self.lock:
            self.active_clients += 1

        try:
            handler.send_response(HTTPStatus.OK)
            handler.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={boundary}")
            handler.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            handler.send_header("Pragma", "no-cache")
            handler.send_header("Connection", "close")
            handler.end_headers()

            last_sent: Optional[bytes] = None
            while not self.stop_event.is_set():
                with self.lock:
                    if self.last_jpeg is last_sent or self.last_jpeg is None:
                        self.frame_condition.wait(timeout=3.0)
                    frame = self.last_jpeg

                if frame is None:
                    try:
                        self.start_camera_session("mjpeg-client")
                    except Exception:
                        pass
                    continue

                last_sent = frame
                header = (
                    f"--{boundary}\r\n"
                    "Content-Type: image/jpeg\r\n"
                    f"Content-Length: {len(frame)}\r\n\r\n"
                ).encode("ascii")
                handler.wfile.write(header)
                handler.wfile.write(frame)
                handler.wfile.write(b"\r\n")
                handler.wfile.flush()
        except Exception:
            pass
        finally:
            with self.lock:
                self.active_clients = max(0, self.active_clients - 1)

    def shutdown(self) -> None:
        self.stop_event.set()
        try:
            if self.ssh_channel is not None:
                self.ssh_channel.close()
        except Exception:
            pass
        try:
            if self.ssh_client is not None:
                self.ssh_client.close()
        except Exception:
            pass
        self.stop_process()
        with self.lock:
            self.frame_condition.notify_all()


class AnsweringMachineClipSync:
    """Copies C300X answering-machine clips to Home Assistant /media and publishes MQTT state."""

    FILES_TO_COPY = ("msg_info.ini", "aswm.jpg", "aswm.avi")

    def __init__(self, config: AppConfig, mqtt_client: mqtt.Client, topic_prefix: str) -> None:
        self.config = config
        self.mqtt_client = mqtt_client
        self.topic_prefix = topic_prefix
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        self.messages: List[Dict[str, Any]] = []
        self.last_error = "not started"
        self.last_sync_at = 0.0
        self.sync_count = 0

    def topic(self, suffix: str) -> str:
        return f"{self.topic_prefix}/{suffix.strip('/')}"

    def publish_text(self, suffix: str, payload: str, retain: bool = True) -> None:
        self.mqtt_client.publish(self.topic(suffix), payload, qos=1, retain=retain)

    def publish_json(self, suffix: str, payload: Dict[str, Any], retain: bool = True) -> None:
        self.mqtt_client.publish(self.topic(suffix), json.dumps(payload, ensure_ascii=False, separators=(",", ":")), qos=1, retain=retain)

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self.loop, name="bticino-ab-clip-sync", daemon=True)
        self.thread.start()

    def shutdown(self) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=3)

    def loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.refresh_once()
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
                _LOGGER.warning("Answering-machine clip sync failed: %s", exc)
                self.publish_text("answering_machine/sync_error", str(exc), retain=True)
            wait_seconds = max(15, int(self.config.ab_sync_interval or DEFAULT_AB_SYNC_INTERVAL_SECONDS))
            self.stop_event.wait(wait_seconds)

    def open_sftp(self) -> Tuple[Any, Any]:
        if paramiko is None:
            raise RuntimeError("paramiko is not available in the app image")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.config.bticino_host,
            port=self.config.ssh_port,
            username=self.config.ssh_username,
            password=self.config.ssh_password,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
            banner_timeout=10,
            auth_timeout=10,
        )
        return client, client.open_sftp()

    @staticmethod
    def parse_msg_info(path: Path) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if not path.exists():
            return result
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            result[key.strip().lower()] = value.strip()
        return result

    @staticmethod
    def is_unread(info: Dict[str, str]) -> bool:
        value = str(info.get("read", info.get("status", "1"))).strip().lower()
        return value in {"0", "false", "unread", "no", "nein"}

    def remote_file_exists(self, sftp: Any, path: str) -> bool:
        try:
            sftp.stat(path)
            return True
        except Exception:
            return False

    def download_if_needed(self, sftp: Any, remote_file: str, local_file: Path) -> bool:
        try:
            st = sftp.stat(remote_file)
        except Exception:
            return False
        local_file.parent.mkdir(parents=True, exist_ok=True)
        if local_file.exists() and local_file.stat().st_size == int(getattr(st, "st_size", -1)):
            return True
        tmp = local_file.with_suffix(local_file.suffix + ".tmp")
        try:
            sftp.get(remote_file, str(tmp))
            tmp.replace(local_file)
            return True
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    def refresh_once(self) -> Dict[str, Any]:
        media_root = Path(self.config.media_path)
        media_root.mkdir(parents=True, exist_ok=True)
        client: Any = None
        sftp: Any = None
        messages: List[Dict[str, Any]] = []
        try:
            client, sftp = self.open_sftp()
            entries = sorted(sftp.listdir_attr(self.config.ab_remote_path), key=lambda attr: attr.filename)
            for entry in entries:
                name = entry.filename
                if not name.startswith("message_"):
                    continue
                remote_dir = f"{self.config.ab_remote_path.rstrip('/')}/{name}"
                local_dir = media_root / name
                local_dir.mkdir(parents=True, exist_ok=True)
                copied: Dict[str, bool] = {}
                for filename in self.FILES_TO_COPY:
                    copied[filename] = self.download_if_needed(sftp, f"{remote_dir}/{filename}", local_dir / filename)
                info = self.parse_msg_info(local_dir / "msg_info.ini")
                item: Dict[str, Any] = {
                    "id": name,
                    "remote_path": remote_dir,
                    "has_thumbnail": bool(copied.get("aswm.jpg")),
                    "has_video": bool(copied.get("aswm.avi")),
                    "thumbnail_url": f"/media/local/bticino/messages/{name}/aswm.jpg" if copied.get("aswm.jpg") else "",
                    "video_url": f"/media/local/bticino/messages/{name}/aswm.avi" if copied.get("aswm.avi") else "",
                    "app_thumbnail_url": f"/media/local/bticino/messages/{name}/aswm.jpg" if copied.get("aswm.jpg") else "",
                    "app_video_url": f"/media/local/bticino/messages/{name}/aswm.avi" if copied.get("aswm.avi") else "",
                    "info": info,
                    "unread": self.is_unread(info),
                }
                try:
                    item["mtime"] = int((local_dir / "aswm.avi").stat().st_mtime if copied.get("aswm.avi") else local_dir.stat().st_mtime)
                except Exception:
                    item["mtime"] = 0
                messages.append(item)
        finally:
            try:
                if sftp is not None:
                    sftp.close()
            except Exception:
                pass
            try:
                if client is not None:
                    client.close()
            except Exception:
                pass

        messages.sort(key=lambda item: (int(item.get("mtime", 0)), str(item.get("id", ""))))
        unread_count = sum(1 for item in messages if item.get("unread"))
        payload = {
            "count": len(messages),
            "unread_count": unread_count,
            "messages": messages,
            "ids": [item["id"] for item in messages],
            "last_sync_at": int(time.time()),
        }
        last = messages[-1] if messages else {}
        with self.lock:
            self.messages = messages
            self.last_error = "ok"
            self.last_sync_at = time.time()
            self.sync_count += 1
        self.publish_json("answering_machine/messages", payload, retain=True)
        self.publish_text("answering_machine/message_count", str(len(messages)), retain=True)
        self.publish_text("answering_machine/unread_count", str(unread_count), retain=True)
        self.publish_json("answering_machine/last_message", last, retain=True)
        if last.get("has_thumbnail"):
            thumb_path = media_root / str(last.get("id")) / "aswm.jpg"
            try:
                self.mqtt_client.publish(self.topic("answering_machine/last_thumbnail_jpeg"), thumb_path.read_bytes(), qos=0, retain=False)
            except Exception:
                pass
        self.publish_text("answering_machine/sync_error", "", retain=True)
        return payload

    def status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "count": len(self.messages),
                "unread_count": sum(1 for item in self.messages if item.get("unread")),
                "messages": list(self.messages),
                "last_error": self.last_error,
                "last_sync_at": int(self.last_sync_at or 0),
                "sync_count": self.sync_count,
                "media_path": self.config.media_path,
                "remote_path": self.config.ab_remote_path,
            }


class BTicinoC300XApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.lock = threading.RLock()
        self.http_server: Optional[ThreadingHTTPServer] = None
        self.http_thread: Optional[threading.Thread] = None

        client_id = f"{APP_ID}_{socket.gethostname()}_{os.getpid()}"
        self.client = mqtt.Client(client_id=client_id, clean_session=True)
        self.client.username_pw_set(config.mqtt_username, config.mqtt_password)
        self.client.will_set(self.topic("status"), payload="offline", qos=1, retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.live_stream = MQTTFirmwareVideoRelay(config, self.client, config.app_topic_prefix)
        self.ab_clip_sync = AnsweringMachineClipSync(config, self.client, config.app_topic_prefix)

        self.ring_timer = TimerState(lambda: self.set_doorbell_state("ring", False))
        self.doorbell_upper_timer = TimerState(lambda: self.set_doorbell_state("doorbell_upper", False))
        self.doorbell_lower_timer = TimerState(lambda: self.set_doorbell_state("doorbell_lower", False))
        self.external_call_timer = TimerState(lambda: self.publish_binary_state("external_call", False))
        self.internal_call_timer = TimerState(lambda: self.publish_binary_state("internal_call", False))
        self.stairs_light_timer = TimerState(lambda: self.publish_binary_state("stairs_light_seen", False))
        self.door_seen_timer = TimerState(lambda: self.set_main_door_seen(False))
        self.stream_timer = TimerState(lambda: self.set_app_streaming(False))
        self.ab_video_timer = TimerState(lambda: self.publish_binary_state("answering_machine_video", False))
        self.last_command_times: Dict[str, float] = {}

    def topic(self, suffix: str) -> str:
        suffix_clean = suffix.strip("/")
        return f"{self.config.app_topic_prefix}/{suffix_clean}"

    def discovery_topic(self, component: str, object_id: str) -> str:
        return f"{self.config.discovery_prefix}/{component}/{APP_ID}/{object_id}/config"

    def device(self) -> Dict[str, Any]:
        return {
            "identifiers": DEVICE_IDENTIFIERS,
            "name": DEVICE_NAME,
            "model": DEVICE_MODEL,
            "manufacturer": DEVICE_MANUFACTURER,
            "sw_version": APP_VERSION,
        }

    def base_entity_payload(self, name: str, unique_suffix: str) -> Dict[str, Any]:
        return {
            "name": name,
            "unique_id": f"{APP_ID}_{unique_suffix}",
            "device": self.device(),
            "availability_topic": self.topic("status"),
            "payload_available": "online",
            "payload_not_available": "offline",
        }

    def publish_discovery(self) -> None:
        discovery_payloads: List[Tuple[str, str, Dict[str, Any]]] = []

        sensors = [
            ("last_frame", "Letzter OpenWebNet/MQTT-Frame", "mdi:code-string", self.topic("state/last_frame")),
            ("last_event", "Letztes Ereignis", "mdi:timeline-text-outline", self.topic("state/last_event")),
            (
                "firmware_availability_raw",
                "Firmware MQTT Status roh",
                "mdi:lan-connect",
                self.topic("state/firmware_availability_raw"),
            ),
            ("app_version", "App Version", "mdi:information-outline", self.topic("state/app_version")),
            ("stream_url", "Livestream URL", "mdi:video-wireless", self.topic("state/stream_url")),
            ("openwebnet_monitor_raw", "OpenWebNet Monitor Antwort", "mdi:lan-check", self.topic("state/openwebnet_monitor_raw")),
            ("answering_machine_message_count", "AB Nachrichten", "mdi:message-video", self.topic("answering_machine/message_count")),
            ("answering_machine_unread_count", "AB ungelesen", "mdi:message-badge", self.topic("answering_machine/unread_count")),
            ("answering_machine_clip_page", "AB Clip-Seite", "mdi:video-box", self.topic("state/answering_machine_clip_page")),
            ("last_ack", "Letzte Firmware-Bestätigung", "mdi:check-network", self.topic("state/last_ack")),
        ]

        for object_id, name, icon, state_topic in sensors:
            discovery_payloads.append(
                (
                    "sensor",
                    object_id,
                    {
                        **self.base_entity_payload(name, object_id),
                        "state_topic": state_topic,
                        "icon": icon,
                    },
                )
            )

        binary_sensors: List[Tuple[str, str, Optional[str]]] = [
            ("firmware_online", "Firmware MQTT online", "connectivity"),
            ("ring", "Klingel", "sound"),
            ("doorbell_upper", "Klingel obere Wohnung", "sound"),
            ("doorbell_lower", "Klingel untere Wohnung", "sound"),
            ("external_call", "Externer Ruf", None),
            ("internal_call", "Interner Ruf", None),
            ("camera_active", "Kamera aktiv", None),
            ("speech_active", "Sprechverbindung aktiv", None),
            ("app_streaming", "App-Livestream aktiv", None),
            ("answering_machine_video", "Anrufbeantworter Video aktiv", None),
            ("door_open_command_seen", "Türöffner-Befehl gesehen", None),
            ("stairs_light_seen", "Treppenlicht-Befehl gesehen", None),
            ("openwebnet_monitor", "OpenWebNet Monitor", "connectivity"),
        ]

        for object_id, name, device_class in binary_sensors:
            payload = {
                **self.base_entity_payload(name, object_id),
                "state_topic": self.topic(f"state/{object_id}"),
                "payload_on": "ON",
                "payload_off": "OFF",
            }
            if device_class:
                payload["device_class"] = device_class
            discovery_payloads.append(("binary_sensor", object_id, payload))

        discovery_payloads.append(
            (
                "button",
                "open_main_door",
                {
                    **self.base_entity_payload("Türöffner", "open_main_door"),
                    "command_topic": self.topic("command/open_main_door"),
                    "payload_press": "PRESS",
                    "icon": "mdi:door-open",
                    "qos": 0,
                    "retain": False,
                },
            )
        )

        discovery_payloads.append(
            (
                "button",
                "start_livestream",
                {
                    **self.base_entity_payload("Livebild starten", "start_livestream"),
                    "command_topic": self.topic("command/start_livestream"),
                    "payload_press": "START",
                    "icon": "mdi:video",
                    "qos": 0,
                    "retain": False,
                },
            )
        )

        discovery_payloads.append(
            (
                "switch",
                "answering_machine",
                {
                    **self.base_entity_payload("Anrufbeantworter", "answering_machine"),
                    "state_topic": self.topic("state/answering_machine"),
                    "command_topic": self.topic("command/answering_machine/set"),
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "state_on": "ON",
                    "state_off": "OFF",
                    "optimistic": False,
                    "icon": "mdi:voicemail",
                    "qos": 0,
                    "retain": False,
                },
            )
        )

        discovery_payloads.append(
            (
                "camera",
                "live_video",
                {
                    **self.base_entity_payload("Livestream", "live_video"),
                    "topic": self.topic("camera/latest_jpeg"),
                    "icon": "mdi:cctv",
                },
            )
        )

        discovery_payloads.append(
            (
                "camera",
                "answering_machine_last_thumbnail",
                {
                    **self.base_entity_payload("AB letztes Vorschaubild", "answering_machine_last_thumbnail"),
                    "topic": self.topic("answering_machine/last_thumbnail_jpeg"),
                    "icon": "mdi:image-frame",
                },
            )
        )

        discovery_payloads.append(
            (
                "button",
                "refresh_answering_machine_clips",
                {
                    **self.base_entity_payload("AB Clips aktualisieren", "refresh_answering_machine_clips"),
                    "command_topic": self.topic("command/answering_machine/refresh"),
                    "payload_press": "REFRESH",
                    "icon": "mdi:sync",
                    "qos": 0,
                    "retain": False,
                },
            )
        )

        discovery_payloads.append(
            (
                "button",
                "stop_livestream",
                {
                    **self.base_entity_payload("Livebild stoppen", "stop_livestream"),
                    "command_topic": self.topic("command/stop_livestream"),
                    "payload_press": "STOP",
                    "icon": "mdi:video-off",
                    "qos": 0,
                    "retain": False,
                },
            )
        )

        for component, object_id, payload in discovery_payloads:
            self.client.publish(
                self.discovery_topic(component, object_id),
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                qos=1,
                retain=True,
            )

    def publish_text_state(self, key: str, value: str, retain: bool = True) -> None:
        self.client.publish(self.topic(f"state/{key}"), value, qos=1, retain=retain)

    def publish_binary_state(self, key: str, is_on: bool, retain: bool = True) -> None:
        self.publish_text_state(key, "ON" if is_on else "OFF", retain=retain)

    def publish_legacy(self, topic: str, payload: str, retain: bool = True) -> None:
        if topic == self.config.firmware_rx_topic:
            return
        self.client.publish(topic, payload, qos=1, retain=retain)

    def set_doorbell_state(self, key: str, is_on: bool) -> None:
        self.publish_binary_state(key, is_on)
        if key == "doorbell_upper":
            self.publish_legacy("Bticino/doorbell_upper/state", "Läutet" if is_on else "Frei")
        elif key == "doorbell_lower":
            self.publish_legacy("Bticino/doorbell_lower/state", "Läutet" if is_on else "Frei")

    def set_app_streaming(self, is_on: bool) -> None:
        self.publish_binary_state("app_streaming", is_on)
        self.publish_legacy("Bticino/app_livestream/state", "Stream aktiv" if is_on else "Stream inaktiv")

    def set_main_door_seen(self, is_on: bool) -> None:
        self.publish_binary_state("door_open_command_seen", is_on)
        self.publish_legacy("Bticino/main_door/state", "ON" if is_on else "OFF")

    def set_answering_machine_state(self, is_on: bool) -> None:
        self.publish_binary_state("answering_machine", is_on)
        self.publish_legacy("Bticino/answering_machine/state", "ON" if is_on else "OFF")

    def publish_initial_states(self) -> None:
        self.client.publish(self.topic("status"), "online", qos=1, retain=True)
        self.publish_text_state("app_version", APP_VERSION)
        self.publish_text_state("last_event", "App gestartet")
        self.publish_text_state("last_frame", "")
        self.publish_text_state("firmware_availability_raw", "unknown")
        self.publish_text_state("stream_url", "Open the BTicino C300X App page in Home Assistant")
        self.publish_text_state("answering_machine_clip_page", "Open the BTicino C300X App page > Anrufbeantworter")
        self.publish_text_state("last_ack", "")
        self.publish_text_state("openwebnet_monitor_raw", "unknown")
        self.publish_binary_state("firmware_online", False)
        self.publish_binary_state("ring", False)
        self.publish_binary_state("doorbell_upper", False)
        self.publish_binary_state("doorbell_lower", False)
        self.publish_binary_state("external_call", False)
        self.publish_binary_state("internal_call", False)
        self.publish_binary_state("camera_active", False)
        self.publish_binary_state("speech_active", False)
        self.publish_binary_state("app_streaming", False)
        self.publish_binary_state("answering_machine_video", False)
        self.publish_binary_state("door_open_command_seen", False)
        self.publish_binary_state("stairs_light_seen", False)
        self.publish_binary_state("openwebnet_monitor", False)
        self.publish_legacy("Bticino/bridge/status", "online")
        self.publish_legacy("Bticino/bridge/version", APP_VERSION)
        self.publish_legacy("Bticino/bridge/connection_mode", "mqtt_firmware")

    def check_openwebnet_monitor_once(self) -> None:
        response = ""
        ok = False
        try:
            with socket.create_connection((self.config.bticino_host, 20000), timeout=5.0) as sock:
                sock.settimeout(5.0)
                sock.sendall(b"*99*1##")
                chunks: List[bytes] = []
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    try:
                        chunk = sock.recv(4096)
                    except socket.timeout:
                        break
                    if not chunk:
                        break
                    chunks.append(chunk)
                    joined = b"".join(chunks)
                    if b"##" in joined:
                        break
                response = b"".join(chunks).decode("utf-8", errors="replace").strip()
            ok = "*#*1##" in response
            if ok:
                _LOGGER.info("OpenWebNet-Monitor bestätigt: %s", response)
            else:
                _LOGGER.warning("OpenWebNet-Monitor nicht bestätigt, Antwort: %s", response or "leer")
        except Exception as exc:
            response = f"ERROR: {exc}"
            _LOGGER.warning("OpenWebNet-Monitor-Test fehlgeschlagen: %s", exc)

        self.publish_binary_state("openwebnet_monitor", ok)
        self.publish_text_state("openwebnet_monitor_raw", response or "empty")
        self.publish_legacy("Bticino/openwebnet_monitor/state", "ON" if ok else "OFF")
        self.publish_legacy("Bticino/openwebnet_monitor/raw", response or "empty")

    def clear_retained_firmware_command(self) -> None:
        self.client.publish(self.config.firmware_rx_topic, payload=b"", qos=0, retain=True)

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
        if rc != 0:
            _LOGGER.error("MQTT-Verbindung fehlgeschlagen, rc=%s", rc)
            return

        _LOGGER.info("MQTT verbunden")
        _LOGGER.debug(
            "Runtime configuration: bticino_host=%s mqtt_host=%s mqtt_port=%s app_topic_prefix=%s firmware_tx=%s firmware_rx=%s firmware_lwt=%s http_port=%s stream_udp_port=%s host_network_expected=true",
            self.config.bticino_host,
            self.config.mqtt_host,
            self.config.mqtt_port,
            self.config.app_topic_prefix,
            self.config.firmware_tx_topic,
            self.config.firmware_rx_topic,
            self.config.firmware_lwt_topic,
            self.config.http_port,
            self.config.stream_udp_port,
        )
        self.connected_event.set()

        self.clear_retained_firmware_command()
        self.publish_discovery()
        self.publish_initial_states()
        for command_topic in [
            self.topic("command/open_main_door"),
            self.topic("command/answering_machine/set"),
            self.topic("command/start_livestream"),
            self.topic("command/stop_livestream"),
            self.topic("command/answering_machine/refresh"),
        ]:
            self.client.publish(command_topic, payload=b"", qos=0, retain=True)

        topics = [
            (self.config.firmware_tx_topic, 0),
            (self.config.firmware_lwt_topic, 0),
            (self.config.firmware_availability_topic, 0),
            (f"{self.config.firmware_ack_prefix}/#", 0),
            ("Bticino/state/#", 0),
            ("Bticino/livestream/#", 0),
            ("Bticino/answering_machine/#", 0),
            (self.topic("command/open_main_door"), 0),
            (self.topic("command/answering_machine/set"), 0),
            (self.topic("command/start_livestream"), 0),
            (self.topic("command/stop_livestream"), 0),
            (self.topic("command/answering_machine/refresh"), 0),
        ]
        if self.config.log_level == "debug":
            topics.append(("Bticino/#", 0))
        client.subscribe(topics)
        _LOGGER.info("MQTT topics subscribed: %s", ", ".join(topic for topic, _qos in topics))

    def on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        self.connected_event.clear()
        if self.stop_event.is_set():
            _LOGGER.info("MQTT getrennt")
        else:
            _LOGGER.warning("MQTT getrennt, rc=%s; Wiederverbindung folgt automatisch", rc)

    def on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = msg.payload.decode("utf-8", errors="replace").strip()
        except Exception:
            payload = repr(msg.payload)

        topic = msg.topic
        _LOGGER.debug(
            "MQTT Nachricht: topic=%s retain=%s qos=%s payload_len=%s payload=%s",
            topic,
            getattr(msg, "retain", False),
            getattr(msg, "qos", "?"),
            len(msg.payload or b""),
            payload,
        )

        if topic == self.config.firmware_tx_topic:
            self.handle_firmware_frame(payload)
            return

        if topic == self.config.firmware_lwt_topic or topic == self.config.firmware_availability_topic:
            self.handle_firmware_lwt(payload)
            return

        if topic.startswith(f"{self.config.firmware_ack_prefix}/"):
            self.publish_text_state("last_ack", payload, retain=True)
            return

        if topic.startswith("Bticino/state/") or topic.startswith("Bticino/livestream/"):
            self.handle_firmware_state_topic(topic, payload)
            return

        if topic.startswith("Bticino/answering_machine/"):
            self.handle_firmware_answering_machine_topic(topic, payload)
            return

        door_command_topics = {self.topic("command/open_main_door")}
        ab_command_topics = {self.topic("command/answering_machine/set")}
        livestream_command_topics = {self.topic("command/start_livestream")}
        stop_livestream_command_topics = {self.topic("command/stop_livestream")}
        ab_refresh_topics = {self.topic("command/answering_machine/refresh")}

        if topic in door_command_topics:
            if getattr(msg, "retain", False):
                _LOGGER.warning("Retained Türöffner-Kommando ignoriert und gelöscht: %s", topic)
                self.client.publish(topic, payload=b"", qos=0, retain=True)
                return
            if payload.upper() in {"PRESS", "OPEN", "ON", "1", "TRUE"}:
                self.open_main_door()
            return

        if topic in livestream_command_topics:
            if getattr(msg, "retain", False):
                _LOGGER.warning("Retained Livestream-Kommando ignoriert und gelöscht: %s", topic)
                self.client.publish(topic, payload=b"", qos=0, retain=True)
                return
            if payload.upper() in {"START", "ON", "1", "TRUE"}:
                self.start_livestream_from_command()
            return

        if topic in stop_livestream_command_topics:
            if getattr(msg, "retain", False):
                self.client.publish(topic, payload=b"", qos=0, retain=True)
                return
            if payload.upper() in {"STOP", "OFF", "0", "FALSE"}:
                self.stop_livestream_from_command()
            return

        if topic in ab_refresh_topics:
            if getattr(msg, "retain", False):
                self.client.publish(topic, payload=b"", qos=0, retain=True)
                return
            threading.Thread(target=self.refresh_answering_machine_clips, name="bticino-ab-refresh-command", daemon=True).start()
            return

        if topic in ab_command_topics:
            if getattr(msg, "retain", False):
                _LOGGER.warning("Retained AB-Kommando ignoriert und gelöscht: %s", topic)
                self.client.publish(topic, payload=b"", qos=0, retain=True)
                return
            payload_upper = payload.upper()
            if payload_upper in {"ON", "1", "TRUE", "EIN", "JA"}:
                self.set_answering_machine(True)
            elif payload_upper in {"OFF", "0", "FALSE", "AUS", "NEIN"}:
                self.set_answering_machine(False)
            else:
                _LOGGER.warning("Unbekannter AB-Schaltwert: %r", payload)
            return

    def start_livestream_from_command(self) -> None:
        try:
            self.live_stream.ensure_started()
            self.live_stream.start_camera_session("mqtt-command")
            self.publish_binary_state("camera_active", True)
            self.set_app_streaming(True)
            self.stream_timer.start(DEFAULT_CALL_HOLD_SECONDS)
            self.publish_text_state("last_event", "Livebild angefordert")
            self.publish_legacy("Bticino/last_event", "Livebild angefordert")
        except Exception as exc:
            _LOGGER.warning("Livestream konnte nicht gestartet werden: %s", exc)
            self.publish_text_state("last_event", f"Livebild Start fehlgeschlagen: {exc}")

    def stop_livestream_from_command(self) -> None:
        try:
            self.send_firmware_json_command("livestream/stop", {})
        except Exception as exc:
            _LOGGER.debug("Firmware livestream stop command failed: %s", exc)
        self.live_stream.shutdown()
        self.publish_binary_state("camera_active", False)
        self.set_app_streaming(False)
        self.stream_timer.cancel()
        self.publish_text_state("last_event", "Livebild gestoppt")

    def handle_firmware_lwt(self, payload: str) -> None:
        normalized = payload.strip().lower()
        online = normalized in {"online", "on", "1", "true", "connected"}
        self.publish_text_state("firmware_availability_raw", payload or "empty")
        self.publish_binary_state("firmware_online", online)
        self.publish_text_state("last_event", "Firmware MQTT online" if online else "Firmware MQTT offline")

    def firmware_command_topic(self, suffix: str) -> str:
        return f"{self.config.firmware_command_prefix.rstrip('/')}/{suffix.strip('/')}"

    def send_firmware_json_command(self, suffix: str, payload: Dict[str, Any]) -> None:
        if "request_id" not in payload:
            payload["request_id"] = f"ha-{uuid.uuid4().hex}"
        topic = self.firmware_command_topic(suffix)
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        _LOGGER.info("Sende Firmware-Command: %s %s", topic, text)
        self.client.publish(topic, text, qos=0, retain=False)

    def handle_firmware_state_topic(self, topic: str, payload: str) -> None:
        value = payload.strip().upper()
        if topic == "Bticino/state/doorbell_upper":
            self.set_doorbell_state("doorbell_upper", value in {"RINGING", "ON", "TRUE", "1"})
        elif topic == "Bticino/state/doorbell_lower":
            self.set_doorbell_state("doorbell_lower", value in {"RINGING", "ON", "TRUE", "1"})
        elif topic == "Bticino/state/main_door":
            self.set_main_door_seen(value in {"ON", "OPEN", "TRUE", "1"})
        elif topic == "Bticino/state/answering_machine":
            self.set_answering_machine_state(value in {"ON", "TRUE", "1"})
        elif topic in {"Bticino/state/app_livestream", "Bticino/livestream/status"}:
            self.set_app_streaming(value in {"ACTIVE", "ON", "TRUE", "1"})

    def handle_firmware_answering_machine_topic(self, topic: str, payload: str) -> None:
        # The firmware already publishes metadata; the app mirrors the most useful
        # parts and the SSH sync provides actual playable files under /media.
        if topic == "Bticino/answering_machine/unread_count":
            self.client.publish(self.topic("answering_machine/unread_count"), payload, qos=1, retain=True)
        elif topic == "Bticino/answering_machine/messages":
            try:
                data = json.loads(payload)
                count = data.get("count")
                unread = data.get("unread_count")
                if count is not None:
                    self.client.publish(self.topic("answering_machine/message_count"), str(count), qos=1, retain=True)
                if unread is not None:
                    self.client.publish(self.topic("answering_machine/unread_count"), str(unread), qos=1, retain=True)
            except Exception:
                pass

    def refresh_answering_machine_clips(self) -> None:
        try:
            self.send_firmware_json_command("answering_machine/refresh", {})
        except Exception:
            pass
        try:
            self.ab_clip_sync.refresh_once()
            self.publish_text_state("last_event", "AB Clips aktualisiert")
        except Exception as exc:
            self.publish_text_state("last_event", f"AB Clip-Aktualisierung fehlgeschlagen: {exc}")
            _LOGGER.warning("AB Clip refresh failed: %s", exc)

    def handle_firmware_frame(self, frame: str) -> None:
        self.publish_text_state("last_frame", frame)
        self.publish_legacy("Bticino/technical/last", frame)
        event = self.classify_frame(frame)
        _LOGGER.debug("Frame classified: %s -> %s", frame, event)
        self.publish_text_state("last_event", event)
        self.publish_legacy("Bticino/last_event", event)
        self.publish_legacy("Bticino/frame_log/event", json.dumps({"frame": frame, "event": event}, ensure_ascii=False))
        if event.startswith("Unbekannter Frame:"):
            self.publish_legacy("Bticino/unknown/last", frame)

        if frame == "*#*1##":
            self.publish_binary_state("openwebnet_monitor", True)
            self.publish_text_state("openwebnet_monitor_raw", frame)
            self.publish_legacy("Bticino/openwebnet_monitor/state", "ON")
            self.publish_legacy("Bticino/openwebnet_monitor/raw", frame)

        upper_ring_frames = {
            "*#7**31#2*100##",
            "*#8**35*2*0*0##",
            "*8*9#1#4*20##",
            "*#8**41*100##",
            "*#8**35*4*0*0##",
            "*7*58#8#0#0#1*##",
        }
        lower_ring_frames = {
            "*8*9#2#4*20##",
            "*#7**31#1*100##",
        }
        stream_active_frames = {
            "*7*53#4000#0*##",
            "*7*300#127#0#0#1#5000#2*##",
            "*7*72#4*20##",
            "*8*100#5#4*20##",
            "*#7**31#5*80##",
            "*8*2#5#4*11##",
            "*7*300#127#0#0#1#5007#0*##",
            "*7*300#127#0#0#1#5002#1*##",
            "*#7**20*40*30*90*75*50##",
            "*8*3#5#4*411##",
            "*#8**35*6*0*0##",
            "*7*77#800#480#2500#148#83#0#800#180#10#15#400#288#0#4000*##",
            "*8*9#5#4*20##",
            "*#8**35*3*0*0##",
            "*#8**35*1*0*0##",
        }
        stream_inactive_frames = {
            "*7*0*##",
            "*7*219*##",
            "*7*55*##",
            "*#8**35*0*0*0##",
        }
        speech_active_frames = {
            "*7*72*20##",
            "*8*100#5#4*20##",
            "*#7**31#0*90##",
            "*8*2#5#4*11##",
            "*#8**35*6*0*0##",
        }

        if frame in upper_ring_frames:
            _LOGGER.info("Klingel obere Wohnung erkannt: %s", frame)
            self.publish_binary_state("ring", True)
            self.set_doorbell_state("doorbell_upper", True)
            self.publish_binary_state("external_call", True)
            self.ring_timer.start(DEFAULT_RING_HOLD_SECONDS)
            self.doorbell_upper_timer.start(DEFAULT_RING_HOLD_SECONDS)
            self.external_call_timer.start(DEFAULT_CALL_HOLD_SECONDS)

        if frame in lower_ring_frames:
            _LOGGER.info("Klingel untere Wohnung erkannt: %s", frame)
            self.publish_binary_state("ring", True)
            self.set_doorbell_state("doorbell_lower", True)
            self.publish_binary_state("external_call", True)
            self.ring_timer.start(DEFAULT_RING_HOLD_SECONDS)
            self.doorbell_lower_timer.start(DEFAULT_RING_HOLD_SECONDS)
            self.external_call_timer.start(DEFAULT_CALL_HOLD_SECONDS)

        if frame in stream_active_frames:
            _LOGGER.info("Livestream/Kamera aktiv erkannt: %s", frame)
            self.publish_binary_state("camera_active", True)
            self.set_app_streaming(True)
            self.stream_timer.start(DEFAULT_CALL_HOLD_SECONDS)

        if frame in stream_inactive_frames:
            _LOGGER.info("Livestream/Kamera inaktiv erkannt: %s", frame)
            self.publish_binary_state("camera_active", False)
            self.publish_binary_state("speech_active", False)
            self.set_app_streaming(False)
            self.stream_timer.cancel()

        if frame in speech_active_frames:
            self.publish_binary_state("speech_active", True)
            self.publish_binary_state("camera_active", True)

        if frame == "*#8**40*1*0*9982*1*25##":
            self.set_answering_machine_state(True)
            return

        if frame == "*#8**40*0*0*9982*1*25##":
            self.set_answering_machine_state(False)
            return

        if frame in {"*8*1#16#4*11##"}:
            self.publish_binary_state("answering_machine_video", True)
            self.ab_video_timer.start(DEFAULT_RING_HOLD_SECONDS)
            return

        if frame in {"*8*3#16#4*411##"}:
            self.publish_binary_state("answering_machine_video", False)
            return

        if frame == "*8*19*20##":
            self.set_main_door_seen(True)
            self.door_seen_timer.start(DEFAULT_SHORT_PULSE_SECONDS)
            return

        if frame == "*8*20*20##":
            self.set_main_door_seen(False)
            self.door_seen_timer.cancel()
            return

        if frame == "*8*21*10##":
            self.publish_binary_state("stairs_light_seen", True)
            self.stairs_light_timer.start(DEFAULT_SHORT_PULSE_SECONDS)
            return

    @staticmethod
    def classify_frame(frame: str) -> str:
        known = {
            "*#*1##": "OpenWebNet Monitor bestätigt",
            "*8*19*20##": "Türöffner gedrückt",
            "*8*20*20##": "Türöffner losgelassen",
            "*#130**1*1*0*1*7*19##": "Türöffnung über Innenstelle/App erkannt",
            "*8*19*21##": "Sekundärer Türöffner gedrückt",
            "*8*20*21##": "Sekundärer Türöffner losgelassen",
            "*8*21*10##": "Treppenlicht aktiviert",
            "*#7**31#2*100##": "Klingel obere Wohnung",
            "*#8**35*2*0*0##": "Klingel obere Wohnung",
            "*8*9#1#4*20##": "Klingel obere Wohnung",
            "*#8**41*100##": "Klingel obere Wohnung",
            "*#8**35*4*0*0##": "Klingel obere Wohnung",
            "*7*58#8#0#0#1*##": "Klingel obere Wohnung",
            "*8*9#2#4*20##": "Klingel untere Wohnung Kandidat",
            "*#7**31#1*100##": "Klingel untere Wohnung Kandidat",
            "*7*53#4000#0*##": "App-Livestream aktiv",
            "*7*300#127#0#0#1#5000#2*##": "App-Livestream aktiv",
            "*7*72#4*20##": "App-Livestream aktiv",
            "*8*100#5#4*20##": "Kamera/Sprechen aktiv",
            "*#7**31#5*80##": "App-Livestream aktiv",
            "*8*2#5#4*11##": "Kamera/Sprechen aktiv",
            "*7*300#127#0#0#1#5007#0*##": "High-Res Videoport 5007 aktiv",
            "*7*300#127#0#0#1#5002#1*##": "Low-Res Videoport 5002 aktiv",
            "*#7**20*40*30*90*75*50##": "Kamera aktiv",
            "*8*3#5#4*411##": "Video-Session aktiv",
            "*#8**35*6*0*0##": "Sprechverbindung aktiv",
            "*7*77#800#480#2500#148#83#0#800#180#10#15#400#288#0#4000*##": "Videoparameter empfangen",
            "*8*9#5#4*20##": "Kamera aktiv",
            "*#8**35*3*0*0##": "Kamera aktiv",
            "*#8**35*1*0*0##": "Kamera aktiv",
            "*7*0*##": "App-Livestream beendet",
            "*7*219*##": "App-Livestream beendet",
            "*7*55*##": "App-Livestream beendet",
            "*#8**35*0*0*0##": "App-Livestream beendet",
            "*7*72*20##": "Sprechverbindung aktiv",
            "*#7**31#0*90##": "Sprechverbindung aktiv",
            "*8*91##": "Anrufbeantworter Einschaltsequenz",
            "*8*91*##": "Anrufbeantworter Einschaltsequenz abgeschlossen",
            "*8*92##": "Anrufbeantworter Ausschaltsequenz",
            "*8*92*##": "Anrufbeantworter Ausschaltsequenz abgeschlossen",
            "*#8**40*1*0*9815*1*25##": "Anrufbeantworter Einschaltbefehl",
            "*#8**40*0*0*9815*1*25##": "Anrufbeantworter Ausschaltbefehl",
            "*#8**40*1*0*9982*1*25##": "Anrufbeantworter eingeschaltet",
            "*#8**40*0*0*9982*1*25##": "Anrufbeantworter ausgeschaltet",
            "*8*1#16#4*11##": "Anrufbeantworter Video gestartet",
            "*8*3#16#4*411##": "Anrufbeantworter Video beendet",
        }
        return known.get(frame, f"Unbekannter Frame: {frame}")

    def command_allowed(self, key: str) -> bool:
        now = time.monotonic()
        with self.lock:
            previous = self.last_command_times.get(key, 0.0)
            if now - previous < DEFAULT_COMMAND_COOLDOWN_SECONDS:
                _LOGGER.warning("Kommando %s ignoriert: Sicherheits-Cooldown aktiv", key)
                return False
            self.last_command_times[key] = now
            return True

    def send_firmware_frames(self, frames: List[str], delay: float = DEFAULT_COMMAND_DELAY_SECONDS) -> None:
        self.clear_retained_firmware_command()
        time.sleep(0.05)
        for frame in frames:
            _LOGGER.info("Sende Firmware-Frame: %s", frame)
            self.client.publish(self.config.firmware_rx_topic, frame, qos=0, retain=False)
            time.sleep(delay)
        self.clear_retained_firmware_command()

    def open_main_door(self) -> None:
        if not self.command_allowed("open_main_door"):
            return
        _LOGGER.info("Türöffner-Befehl wird als JSON-Command an die Firmware gesendet")
        self.publish_text_state("last_event", "Türöffner-Befehl gesendet")
        self.publish_legacy("Bticino/last_event", "Türöffner-Befehl gesendet")
        self.send_firmware_json_command("main_door/open", {"raw": "OPEN"})

    @staticmethod
    def answering_machine_sequence(enabled: bool) -> List[str]:
        if enabled:
            return [
                "*7*73#1#100*##",
                "*8*91##",
                "*#8**40*1*0*9815*1*25##",
                "*8*91*##",
            ]
        return [
            "*7*73#1#100*##",
            "*8*92##",
            "*#8**40*0*0*9815*1*25##",
            "*8*92*##",
        ]

    def set_answering_machine(self, enabled: bool) -> None:
        key = "answering_machine_on" if enabled else "answering_machine_off"
        if not self.command_allowed(key):
            return
        label = "Anrufbeantworter einschalten" if enabled else "Anrufbeantworter ausschalten"
        state = "ON" if enabled else "OFF"
        _LOGGER.info("%s", label)
        self.publish_text_state("last_event", f"{label}: JSON-Command gesendet, warte auf 9982-Bestätigung")
        self.publish_legacy("Bticino/last_event", f"{label}: JSON-Command gesendet, warte auf 9982-Bestätigung")
        self.send_firmware_json_command("answering_machine/set", {"state": state})

    def start_http_server(self) -> None:
        app = self

        class RequestHandler(BaseHTTPRequestHandler):
            server_version = "BTicinoC300XApp/1.0"

            def log_message(self, fmt: str, *args: Any) -> None:
                _LOGGER.debug("HTTP: " + fmt, *args)

            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path in {"/", "", "/answering_machine", "/ab"}:
                    self.send_index()
                    return
                if parsed.path == "/healthz":
                    self.send_text("OK\n", "text/plain")
                    return
                if parsed.path == "/api/start":
                    query = parse_qs(parsed.query)
                    port_value = query.get("port", [str(app.config.stream_udp_port)])[0]
                    try:
                        port = int(port_value)
                    except Exception:
                        port = app.config.stream_udp_port
                    if int(app.config.stream_udp_port) != int(port):
                        app.live_stream.shutdown()
                        time.sleep(0.2)
                    object.__setattr__(app.config, "stream_udp_port", port)
                    app.live_stream.ensure_started()
                    try:
                        app.live_stream.start_camera_session("webui-api-start")
                        app.publish_binary_state("camera_active", True)
                        app.set_app_streaming(True)
                        app.stream_timer.start(DEFAULT_CALL_HOLD_SECONDS)
                        app.publish_text_state("last_event", "Livebild angefordert")
                        app.publish_legacy("Bticino/last_event", "Livebild angefordert")
                        self.send_json({"ok": True, **app.live_stream.status()})
                    except Exception as exc:
                        with app.live_stream.lock:
                            app.live_stream.last_error = f"camera start failed: {exc}"
                            app.live_stream.last_camera_start_result = f"failed: {exc}"
                        _LOGGER.warning("Camera start failed: %s", exc)
                        self.send_json({"ok": False, "error": str(exc), **app.live_stream.status()})
                    return
                if parsed.path == "/status.json":
                    self.send_json(app.live_stream.status())
                    return
                if parsed.path == "/api/answering_machine":
                    self.send_json(app.ab_clip_sync.status())
                    return
                if parsed.path == "/api/answering_machine/refresh":
                    try:
                        data = app.ab_clip_sync.refresh_once()
                        self.send_json({"ok": True, **data})
                    except Exception as exc:
                        self.send_json({"ok": False, "error": str(exc), **app.ab_clip_sync.status()})
                    return
                if parsed.path.startswith("/media/local/bticino/messages/"):
                    self.send_media_file(parsed.path)
                    return
                if parsed.path == "/stream.mjpeg":
                    query = parse_qs(parsed.query)
                    port_value = query.get("port", [str(app.config.stream_udp_port)])[0]
                    try:
                        port = int(port_value)
                    except Exception:
                        port = app.config.stream_udp_port
                    object.__setattr__(app.config, "stream_udp_port", port)
                    app.publish_binary_state("camera_active", True)
                    app.set_app_streaming(True)
                    app.stream_timer.start(DEFAULT_CALL_HOLD_SECONDS)
                    app.live_stream.stream_mjpeg(self)
                    return
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")

            def send_text(self, text: str, content_type: str = "text/html; charset=utf-8") -> None:
                body = text.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(body)

            def send_json(self, data: Dict[str, Any]) -> None:
                self.send_text(json.dumps(data, ensure_ascii=False, indent=2), "application/json; charset=utf-8")

            def send_media_file(self, request_path: str) -> None:
                prefix = "/media/local/bticino/messages/"
                relative = unquote(request_path[len(prefix):])
                parts = [part for part in relative.split("/") if part]
                if len(parts) != 2 or parts[0].startswith(".") or parts[1].startswith("."):
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                filename = parts[1]
                if filename not in {"aswm.jpg", "aswm.avi", "msg_info.ini"}:
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                path = Path(app.config.media_path) / parts[0] / filename
                if not path.exists() or not path.is_file():
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
                body = path.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(body)

            def send_index(self) -> None:
                html = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BTicino C300X App</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; padding: 24px; background: Canvas; color: CanvasText; }
    main { max-width: 980px; margin: 0 auto; }
    .card { border: 1px solid color-mix(in srgb, CanvasText 18%, transparent); border-radius: 18px; padding: 18px; margin: 0 0 16px; background: color-mix(in srgb, Canvas 96%, CanvasText 4%); }
    h1 { margin: 0 0 12px; font-size: 1.55rem; }
    h2 { margin: 0 0 10px; font-size: 1.15rem; }
    p { line-height: 1.5; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0; }
    button { border: 0; border-radius: 12px; padding: 11px 14px; cursor: pointer; font-weight: 650; }
    button.primary { background: #03a9f4; color: white; }
    button.secondary { background: color-mix(in srgb, CanvasText 12%, transparent); color: CanvasText; }
    img { width: 100%; max-width: 688px; min-height: 260px; object-fit: contain; background: #111; border-radius: 14px; display: block; }
    code { background: color-mix(in srgb, CanvasText 10%, transparent); border-radius: 6px; padding: 2px 5px; }
    pre { white-space: pre-wrap; background: color-mix(in srgb, CanvasText 8%, transparent); border-radius: 12px; padding: 12px; overflow: auto; }
    .small { opacity: .82; font-size: .92rem; }
  </style>
</head>
<body>
  <main>
    <section class="card">
      <h1>BTicino C300X App</h1>
      <p>Diese Seite startet das Livebild über die Firmware-MQTT-Anbindung und zeigt es in Home Assistant an.</p>
      <p class="small">This page starts live video through the firmware MQTT connection and displays it in Home Assistant.</p>
    </section>

    <section class="card">
      <h2>Kamera / Live video</h2>
      <p>Beim Öffnen dieser Seite wird automatisch eine Videosehung über die Firmware angefordert. Es ist kein zusätzlicher Klick in der BTicino-App nötig.</p>
      <p class="small">When this page is opened, a video session is requested through the firmware automatically. No additional action in the BTicino mobile app is required.</p>
      <div class="actions">
        <button class="primary" onclick="startStream(5007)">High-Res starten / Start high-res</button>
        <button class="secondary" onclick="startStream(5002)">Low-Res starten / Start low-res</button>
        <button class="secondary" onclick="reloadStream()">Neu verbinden / Reconnect</button>
      </div>
      <img id="video" alt="BTicino live video">
      <p id="status" class="small">Livebild wird gestartet...</p>
      <pre id="details" class="small"></pre>
    </section>

    <section class="card">
      <h2>Anrufbeantworter / Clips</h2>
      <p>Die App kopiert Clips per SSH von der BTicino nach <code>/media/bticino/messages</code> und zeigt sie hier direkt an.</p>
      <div class="actions">
        <button class="primary" onclick="refreshClips()">Clips aktualisieren</button>
      </div>
      <p id="abStatus" class="small">AB-Clips werden geladen...</p>
      <div id="clips"></div>
    </section>
  </main>

<script>
  const video = document.getElementById('video');
  const status = document.getElementById('status');
  const details = document.getElementById('details');
  let currentPort = 5007;
  let reconnectTimer = null;

  function streamUrl() {
    return './stream.mjpeg?port=' + currentPort + '&t=' + Date.now();
  }

  async function requestStart() {
    status.textContent = 'Livebild wird angefordert...';
    try {
      const response = await fetch('./api/start?port=' + currentPort + '&t=' + Date.now(), { cache: 'no-store' });
      const data = await response.json();
      status.textContent = data.ok ? 'Livebild angefordert. Warte auf RTP/H264 von der BTicino...' : ('Start fehlgeschlagen: ' + data.error);
      updateDetails(data);
    } catch (err) {
      status.textContent = 'Start-Anforderung fehlgeschlagen: ' + err;
    }
  }

  async function startStream(port) {
    currentPort = port || currentPort;
    await requestStart();
    video.src = streamUrl();
    scheduleReconnect();
  }

  function reloadStream() {
    video.src = '';
    setTimeout(() => startStream(currentPort), 400);
  }

  function scheduleReconnect() {
    if (reconnectTimer) {
      clearInterval(reconnectTimer);
    }
    reconnectTimer = setInterval(async () => {
      try {
        const data = await getStatus();
        if ((data.jpeg_frames || 0) === 0) {
          await requestStart();
          video.src = streamUrl();
        }
      } catch (err) {
        status.textContent = 'Status konnte nicht gelesen werden: ' + err;
      }
    }, 7000);
  }

  async function getStatus() {
    const response = await fetch('./status.json?t=' + Date.now(), { cache: 'no-store' });
    return await response.json();
  }

  function updateDetails(data) {
    details.textContent = JSON.stringify({
      status: data.last_error,
      port: data.stream_udp_port,
      active_clients: data.active_clients,
      udp_packets: data.udp_packets,
      rtp_packets: data.rtp_packets,
      h264_bytes: data.h264_bytes,
      jpeg_frames: data.jpeg_frames,
      camera_start_count: data.camera_start_count,
      camera_start_result: data.last_camera_start_result,
      target_ip: data.last_target_ip,
      video_command: data.last_video_command,
      publish_mid: data.last_publish_mid,
      publish_rc: data.last_publish_rc,
      start_reason: data.last_stream_start_reason,
      non_rtp_packets: data.non_rtp_packets,
      rtp_payload_type_96_packets: data.rtp_payload_type_96_packets,
      last_udp_source: data.last_udp_source,
      last_udp_packet_size: data.last_udp_packet_size,
      last_rtp_payload_type: data.last_rtp_payload_type,
      last_rtp_sequence: data.last_rtp_sequence,
      seconds_since_last_udp: data.seconds_since_last_udp,
      seconds_since_last_jpeg: data.seconds_since_last_jpeg,
      ffmpeg_stderr_tail: data.ffmpeg_stderr_tail
    }, null, 2);
  }

  async function refreshClips() {
    const statusEl = document.getElementById('abStatus');
    const clipsEl = document.getElementById('clips');
    statusEl.textContent = 'AB-Clips werden aktualisiert...';
    try {
      const response = await fetch('./api/answering_machine/refresh?t=' + Date.now(), { cache: 'no-store' });
      const data = await response.json();
      renderClips(data);
    } catch (err) {
      statusEl.textContent = 'AB-Clips konnten nicht aktualisiert werden: ' + err;
    }
  }

  async function loadClips() {
    try {
      const response = await fetch('./api/answering_machine?t=' + Date.now(), { cache: 'no-store' });
      const data = await response.json();
      renderClips(data);
    } catch (err) {
      document.getElementById('abStatus').textContent = 'AB-Clips konnten nicht geladen werden: ' + err;
    }
  }

  function renderClips(data) {
    const statusEl = document.getElementById('abStatus');
    const clipsEl = document.getElementById('clips');
    const messages = data.messages || [];
    statusEl.textContent = 'Clips: ' + (data.count ?? messages.length) + ' | ungelesen: ' + (data.unread_count ?? 0) + ' | Status: ' + (data.last_error || (data.ok === false ? data.error : 'ok'));
    if (!messages.length) {
      clipsEl.innerHTML = '<p class="small">Keine AB-Clips gefunden.</p>';
      return;
    }
    clipsEl.innerHTML = messages.slice().reverse().map((item) => {
      const id = String(item.id || 'message');
      const thumb = item.app_thumbnail_url || item.thumbnail_url || '';
      const video = item.app_video_url || item.video_url || '';
      const info = item.info || {};
      const title = id.replace(/[&<>"']/g, '');
      return '<div class="card">' +
        '<h2>' + title + (item.unread ? ' · ungelesen' : '') + '</h2>' +
        (thumb ? '<img src=".' + thumb + '?t=' + Date.now() + '" alt="' + title + '">' : '') +
        (video ? '<video controls style="width:100%;max-width:688px;border-radius:14px;background:#111" src=".' + video + '?t=' + Date.now() + '"></video>' : '') +
        '<pre class="small">' + JSON.stringify(info, null, 2) + '</pre>' +
      '</div>';
    }).join('');
  }

  async function refreshStatus() {
    try {
      const data = await getStatus();
      if ((data.jpeg_frames || 0) > 0) {
        status.textContent = 'Livebild aktiv | RTP: ' + data.rtp_packets + ' | JPEG: ' + data.jpeg_frames;
      } else {
        status.textContent = 'Warte auf Livebild | ' + data.last_error + ' | RTP: ' + data.rtp_packets + ' | JPEG: ' + data.jpeg_frames;
      }
      updateDetails(data);
    } catch (err) {
      status.textContent = 'Status konnte nicht gelesen werden.';
    }
  }

  startStream(5007);
  loadClips();
  setInterval(refreshStatus, 2000);
  setInterval(loadClips, 30000);
</script>
</body>
</html>
"""
                self.send_text(html)

        self.http_server = ThreadingHTTPServer(("0.0.0.0", self.config.http_port), RequestHandler)
        self.http_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
        self.http_thread.start()
        _LOGGER.info("HTTP/Ingress server started on port %s", self.config.http_port)

    def stop_http_server(self) -> None:
        if self.http_server is not None:
            self.http_server.shutdown()
            self.http_server.server_close()
            self.http_server = None
        if self.http_thread is not None and self.http_thread.is_alive():
            self.http_thread.join(timeout=2)
            self.http_thread = None

    def run(self) -> int:
        _LOGGER.info("Starting %s v%s", APP_NAME, APP_VERSION)
        _LOGGER.info(
            "Firmware MQTT topics: tx=%s rx=%s lwt=%s availability=%s command=%s",
            self.config.firmware_tx_topic,
            self.config.firmware_rx_topic,
            self.config.firmware_lwt_topic,
            self.config.firmware_availability_topic,
            self.config.firmware_command_prefix,
        )

        self.start_http_server()
        self.client.connect_async(self.config.mqtt_host, self.config.mqtt_port, keepalive=60)
        self.client.loop_start()
        self.ab_clip_sync.start()

        def handle_signal(signum: int, frame: Any) -> None:
            _LOGGER.info("Stop-Signal erhalten: %s", signum)
            self.stop_event.set()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        try:
            while not self.stop_event.is_set():
                time.sleep(1.0)
        finally:
            self.ring_timer.cancel()
            self.doorbell_upper_timer.cancel()
            self.doorbell_lower_timer.cancel()
            self.external_call_timer.cancel()
            self.internal_call_timer.cancel()
            self.stairs_light_timer.cancel()
            self.door_seen_timer.cancel()
            self.stream_timer.cancel()
            self.ab_video_timer.cancel()
            try:
                self.client.publish(self.topic("status"), "offline", qos=1, retain=True)
                self.publish_legacy("Bticino/bridge/status", "offline")
            except Exception:
                pass
            try:
                self.live_stream.shutdown()
            except Exception:
                pass
            try:
                self.ab_clip_sync.shutdown()
            except Exception:
                pass
            self.client.loop_stop()
            self.client.disconnect()
            self.stop_http_server()

        return 0


def main() -> int:
    try:
        config = load_config()
    except Exception as exc:
        _LOGGER.error("Konfiguration ungültig: %s", exc)
        while True:
            time.sleep(30)

    logging.getLogger().setLevel(logging.DEBUG if config.log_level == "debug" else logging.INFO)
    _LOGGER.setLevel(logging.DEBUG if config.log_level == "debug" else logging.INFO)
    _LOGGER.info("Log level: %s", config.log_level)

    app = BTicinoC300XApp(config)
    return app.run()


if __name__ == "__main__":
    sys.exit(main())
