"""
Echo Bot — Avaya Real-Time Contextual Media (RTCM) WebSocket Protocol
======================================================================

FLUJO DE MENSAJES:
  cliente → servidor  session.start       # negociación de codec y transporte
  servidor → cliente  session.started     # confirma parámetros seleccionados
  cliente → servidor  echo.start          # activa el eco para un endpoint
  servidor → cliente  echo.started        # confirma inicio del eco
  cliente → servidor  [frames de audio]   # flujo continuo (binario o base64 JSON)
  servidor → cliente  [frames de audio]   # el servidor devuelve exactamente lo mismo
  cliente → servidor  echo.end            # detiene el eco
  servidor → cliente  echo.ended
  cliente → servidor  session.end
  servidor → cliente  session.ended

FORMATOS DE AUDIO:
  Binario  — frame compacto de 16 bytes de cabecera + payload de PCM crudo
  Base64   — mensaje JSON {"type":"media", "bid":N, "asn":N, "ts":N, "audio":"<base64>"}
"""

from __future__ import annotations

import asyncio
import base64
import http
import json
import logging
import ssl
import struct
import time
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Set

import websockets
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 1: Constantes del protocolo binario
# ─────────────────────────────────────────────────────────────────────────────
#
# El protocolo RTCM usa frames binarios compactos con un header de 16 bytes.
# Cada frame lleva un "bid" (stream identifier) y un "source" que indica
# si el audio viene del tx (transmisor/caller) o rx (receptor/agent).
#
# Los frames que el SERVIDOR envía al cliente siempre usan source=none (0).

SOURCE_NONE = 0  # Usado en frames de ingress (servidor → cliente)
SOURCE_TX   = 1  # Transmisor (audio del caller)
SOURCE_RX   = 2  # Receptor  (audio del agent)

SOURCE_TO_INT: Dict[str, int] = {"none": SOURCE_NONE, "tx": SOURCE_TX, "rx": SOURCE_RX}
INT_TO_SOURCE: Dict[int, str] = {v: k for k, v in SOURCE_TO_INT.items()}

# Bits de flags en el header del frame binario
FLAG_LAST_FRAME = 0x0001  # Último frame del segmento de audio actual
FLAG_EXTENSION  = 0x0002  # Hay bytes de extensión después del header de 16 bytes


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 2: Parseo y construcción de frames binarios
# ─────────────────────────────────────────────────────────────────────────────
#
# LAYOUT del frame compacto (big-endian):
#
#   Offset  Tamaño  Campo
#   ──────  ──────  ──────────────────────────────────────────────
#   0       2 B     Flags     (uint16)  — FLAG_LAST_FRAME, FLAG_EXTENSION
#   2       1 B     Bid       (uint8)   — identificador del stream (0-255)
#   3       1 B     Source    (uint8)   — 0=none, 1=tx, 2=rx
#   4       4 B     Sequence  (uint32)  — número de secuencia del stream
#   8       8 B     Timestamp (uint64)  — microsegundos NTP
#   16+     N B     [Extensión opcional si FLAG_EXTENSION] + payload de audio

def parse_compact_binary_frame(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parsea un frame binario compacto del protocolo RTCM.
    Retorna un dict con los campos o None si el frame es inválido.
    """
    if len(data) < 16:
        logger.warning("Frame binario demasiado corto: %d bytes (mínimo 16)", len(data))
        return None

    flags  = struct.unpack(">H", data[0:2])[0]
    bid    = data[2]
    source = INT_TO_SOURCE.get(data[3], "none")
    seq    = struct.unpack(">I", data[4:8])[0]
    ts_us  = struct.unpack(">Q", data[8:16])[0]

    offset    = 16
    extension = b""
    if flags & FLAG_EXTENSION:
        if len(data) < offset + 4:
            logger.warning("Frame demasiado corto para leer longitud de extensión")
            return None
        ext_len   = struct.unpack(">I", data[offset:offset + 4])[0]
        offset   += 4
        extension  = data[offset:offset + ext_len]
        offset    += ext_len

    return {
        "flags":     flags,
        "bid":       bid,
        "source":    source,
        "seq":       seq,
        "ts_us":     ts_us,
        "extension": extension,
        "payload":   data[offset:],   # audio PCM crudo
    }


def build_compact_binary_frame(
    bid: int,
    source: str,
    seq: int,
    ts_us: int,
    flags: int,
    payload: bytes,
) -> bytes:
    """
    Construye un frame binario compacto del protocolo RTCM.
    Los frames de ingress (servidor→cliente) siempre usan source="none".
    """
    source_byte = SOURCE_TO_INT.get(source, SOURCE_NONE)
    header  = struct.pack(">H", flags)          # flags   (2 bytes)
    header += bytes([bid, source_byte])          # bid+src (2 bytes)
    header += struct.pack(">I", seq)             # seq     (4 bytes)
    header += struct.pack(">Q", ts_us)           # ts_us   (8 bytes)
    return header + payload


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 3: Estado de sesión por conexión WebSocket
# ─────────────────────────────────────────────────────────────────────────────
#
# Cada conexión WebSocket tiene su propia EchoSession.
# La sesión guarda:
#   - Los parámetros negociados (codec, sample_rate, transport)
#   - La tabla de streams: bid+source → endpoint_id + dirección
#   - El bid de ingress por endpoint (para saber en qué canal responder)
#   - Cuáles endpoints tienen eco activo
#   - El número de secuencia e timestamp del stream de ingress

class EchoSession:
    """Estado completo de una sesión RTCM (una por conexión WebSocket)."""

    def __init__(self, session_id: str, client_id: str) -> None:
        self.session_id  = session_id
        self.client_id   = client_id

        # Parámetros negociados en session.start / session.started
        self.codec        = "L16"
        self.sample_rate  = 8000
        self.transport    = "binary"  # "binary" | "base64"

        # Mapa de streams de egress (cliente→servidor):
        # clave = "{bid}:{source_int}"  →  info del endpoint
        self.stream_map: Dict[str, Dict[str, Any]] = {}

        # Canal de respuesta (ingress, servidor→cliente):
        # endpoint_id → bid asignado para enviar audio de vuelta
        self.ingress_bid: Dict[str, int] = {}

        # Conjunto de endpoint_ids con eco activo
        self.echo_active: Set[str] = set()

        # Contadores de secuencia y timestamp para cada stream de ingress
        self._ingress_seq: Dict[str, int] = {}
        self._ingress_ts:  Dict[str, int] = {}

    def init_ingress(self, endpoint_id: str) -> None:
        """Inicializa contadores de secuencia/timestamp para un endpoint."""
        self._ingress_seq[endpoint_id] = 0
        self._ingress_ts[endpoint_id]  = int(time.time() * 1_000_000)

    def next_ingress_seq_ts(self, endpoint_id: str) -> tuple[int, int]:
        """
        Devuelve (seq, ts_us) para el próximo frame de ingress y avanza los contadores.
        El timestamp avanza 100 ms (100 000 μs) por frame, que es el tamaño de chunk estándar.
        """
        seq   = self._ingress_seq.get(endpoint_id, 0)
        ts_us = self._ingress_ts.get(endpoint_id, int(time.time() * 1_000_000))
        self._ingress_seq[endpoint_id] = seq + 1
        self._ingress_ts[endpoint_id]  = ts_us + 100_000   # avanza 100 ms
        return seq, ts_us


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 4: Servidor Echo
# ─────────────────────────────────────────────────────────────────────────────

class EchoServer:
    """
    Servidor WebSocket que implementa el protocolo RTCM de Avaya para eco de audio.

    Para cada frame de audio que llega del cliente (egress), el servidor lo
    devuelve inmediatamente por el canal de ingress del mismo endpoint.
    No hay buffering ni pacing: el eco es instantáneo paquete a paquete.
    """

    PREFERRED_CODEC     = "L16"    # Prioriza PCM lineal 16-bit
    PREFERRED_TRANSPORT = "binary" # Prioriza frames binarios (menor overhead)

    def __init__(
        self,
        host:     str            = "0.0.0.0",
        port:     int            = 8765,
        ssl_cert: Optional[str]  = None,
        ssl_key:  Optional[str]  = None,
    ) -> None:
        self.host     = host
        self.port     = port
        self.ssl_cert = ssl_cert
        self.ssl_key  = ssl_key

        # Sesiones activas: session_id → EchoSession
        self.sessions: Dict[str, EchoSession] = {}
        # Número de secuencia de los mensajes JSON enviados al cliente
        self._seq: Dict[str, int] = {}

    # ── Helpers ────────────────────────────────────────────────────────────

    def _next_seq(self, client_id: str) -> int:
        n = self._seq.get(client_id, 0)
        self._seq[client_id] = n + 1
        return n

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    def _session_for_client(self, client_id: str) -> Optional[EchoSession]:
        """Busca la sesión activa de un client_id dado."""
        for s in self.sessions.values():
            if s.client_id == client_id:
                return s
        return None

    # ── Gestión de conexiones ──────────────────────────────────────────────

    async def handle_connection(self, websocket: WebSocketServerProtocol) -> None:
        """
        Punto de entrada para cada nueva conexión WebSocket.

        Cada conexión es una coroutine independiente. El loop de mensajes
        despacha texto (JSON) y bytes (frames binarios) al manejador correcto.
        Al desconectarse, se limpia todo el estado de esa conexión.
        """
        client_id = "{}:{}".format(*websocket.remote_address)
        self._seq[client_id] = 0
        logger.info("[%s] Nueva conexión", client_id)

        try:
            async for message in websocket:
                try:
                    if isinstance(message, bytes):
                        await self._handle_binary(websocket, client_id, message)
                    else:
                        await self._handle_text(websocket, client_id, message)
                except Exception:
                    logger.exception("[%s] Error procesando mensaje", client_id)
        except websockets.exceptions.ConnectionClosed:
            logger.info("[%s] Conexión cerrada", client_id)
        except Exception:
            logger.exception("[%s] Error en conexión", client_id)
        finally:
            # Eliminar todas las sesiones de este cliente
            stale = [sid for sid, s in self.sessions.items() if s.client_id == client_id]
            for sid in stale:
                self.sessions.pop(sid, None)
                logger.debug("[%s] Sesión %s eliminada", client_id, sid)
            self._seq.pop(client_id, None)

    # ── Mensajes de texto (JSON) ───────────────────────────────────────────

    async def _handle_text(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        raw:       str,
    ) -> None:
        """
        Parsea el JSON y despacha al manejador correspondiente según el campo "type".
        También soporta múltiples mensajes JSON concatenados en un mismo frame
        (batching que hace la plataforma Avaya).
        """
        # Detectar batching (varios JSON en un frame separados por \\n)
        messages = self._split_batched_json(raw)

        for raw_msg in messages:
            try:
                data = json.loads(raw_msg)
            except json.JSONDecodeError as e:
                logger.error("[%s] JSON inválido: %s", client_id, e)
                continue

            msg_type = data.get("type", "")
            logger.info("[%s] ← %s", client_id, msg_type)

            if msg_type == "session.start":
                await self._on_session_start(ws, client_id, data)
            elif msg_type == "echo.start":
                await self._on_echo_start(ws, client_id, data)
            elif msg_type == "echo.end":
                await self._on_echo_end(ws, client_id, data)
            elif msg_type == "media":
                await self._on_media_json(ws, client_id, data)
            elif msg_type in ("session.end", "session.stop"):
                await self._on_session_end(ws, client_id, data)
            elif msg_type == "session.ping":
                await self._on_session_ping(ws, client_id, data)
            else:
                logger.warning("[%s] Tipo de mensaje no manejado: %s", client_id, msg_type)

    @staticmethod
    def _split_batched_json(raw: str) -> list[str]:
        """
        Separa mensajes JSON concatenados en un solo frame de texto.
        La plataforma puede enviar p.ej. {session.start}\\n{echo.start} juntos.
        Usa JSONDecoder.raw_decode para extraer objetos completos sin ambigüedad.
        """
        decoder = json.JSONDecoder()
        results: list[str] = []
        idx = 0
        while idx < len(raw):
            while idx < len(raw) and raw[idx] in " \t\r\n":
                idx += 1
            if idx >= len(raw):
                break
            try:
                _, end = decoder.raw_decode(raw, idx)
                results.append(raw[idx:end])
                idx = end
            except json.JSONDecodeError:
                break
        return results or [raw]

    # ── Frames binarios de audio ───────────────────────────────────────────

    async def _handle_binary(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      bytes,
    ) -> None:
        """
        Parsea el frame binario compacto (16-byte header) y, si el eco está activo
        para ese endpoint, devuelve el audio exactamente igual por el bid de ingress.
        """
        frame = parse_compact_binary_frame(data)
        if not frame:
            return

        session = self._session_for_client(client_id)
        if not session:
            return

        # Construir la clave del stream para buscar el endpoint
        stream_key = "{bid}:{src}".format(bid=frame["bid"], src=SOURCE_TO_INT.get(frame["source"], SOURCE_NONE))
        ep_info    = session.stream_map.get(stream_key)
        if not ep_info or ep_info["direction"] != "egress":
            return  # Solo ecoamos streams de egress (audio que llega del cliente)

        endpoint_id      = ep_info["endpoint_id"]
        supports_ingress = ep_info["supports_ingress"]

        if not supports_ingress:
            return  # Este endpoint no acepta audio de vuelta
        if endpoint_id not in session.echo_active:
            return  # El eco no ha sido activado para este endpoint

        ingress_bid = session.ingress_bid.get(endpoint_id)
        if ingress_bid is None:
            return

        # Preparar y enviar el frame de eco
        seq, ts_us = session.next_ingress_seq_ts(endpoint_id)
        is_last    = bool(frame["flags"] & FLAG_LAST_FRAME)
        out_flags  = FLAG_LAST_FRAME if is_last else 0

        out_frame = build_compact_binary_frame(
            bid     = ingress_bid,
            source  = "none",    # ingress siempre usa source=none
            seq     = seq,
            ts_us   = ts_us,
            flags   = out_flags,
            payload = frame["payload"],
        )
        try:
            await ws.send(out_frame)
        except Exception as e:
            logger.warning("[%s] Error enviando eco binario: %s", client_id, e)

    # ─────────────────────────────────────────────────────────────────────
    # SECCIÓN 5: Manejadores de mensajes del protocolo de señalización
    # ─────────────────────────────────────────────────────────────────────

    async def _on_session_start(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """
        Maneja 'session.start': negocia codec y transporte, registra endpoints
        con sus bids y responde con 'session.started'.

        El cliente ofrece una lista de codecs y encodings. El servidor selecciona
        su preferido y construye la tabla de stream IDs para rutear audio.

        Estructura del payload de session.start:
        {
          "mediaTransports": [{
            "type": "avaya-wss",
            "transportEncodings": ["binary", "base64"],
            "mediaCodecs": [["audio","L16",8000,1], ["audio","PCMU",8000,1]]
          }],
          "mediaEndpoints": [{
            "endpointId": "<uuid>",
            "tag": "customer",
            "flows": {
              "audio": {
                "egress":  { "sources": ["tx"], "bid": 0 },
                "ingress": { "target": ["auto"], "bid": 1 }
              }
            }
          }]
        }
        """
        session_id = data.get("sessionId", "")
        payload    = data.get("payload", {})

        # ── Paso 1: Negociar encoding de transporte ──────────────────────
        transport_blocks   = payload.get("mediaTransports", [{}])
        transport_block    = transport_blocks[0] if transport_blocks else {}
        offered_encodings  = transport_block.get(
            "transportEncodings",
            [transport_block.get("transportEncoding", "base64")]
        )
        if not isinstance(offered_encodings, list):
            offered_encodings = [offered_encodings]

        selected_encoding = (
            self.PREFERRED_TRANSPORT
            if self.PREFERRED_TRANSPORT in offered_encodings
            else (offered_encodings[0] if offered_encodings else "base64")
        )

        # ── Paso 2: Negociar codec ───────────────────────────────────────
        offered_codecs = transport_block.get("mediaCodecs", [])
        selected_codec = next(
            (c for c in offered_codecs if isinstance(c, list) and len(c) >= 2 and c[1] == self.PREFERRED_CODEC),
            offered_codecs[0] if offered_codecs else ["audio", "L16", 8000, 1],
        )

        codec_name  = selected_codec[1] if len(selected_codec) > 1 else "L16"
        sample_rate = selected_codec[2] if len(selected_codec) > 2 else 8000

        logger.info(
            "[%s] Negociación — encoding: %s (ofrecidos: %s)  codec: %s@%dHz",
            client_id, selected_encoding, offered_encodings, codec_name, sample_rate,
        )

        # ── Paso 3: Crear la sesión con los parámetros negociados ────────
        session             = EchoSession(session_id, client_id)
        session.codec       = codec_name
        session.sample_rate = sample_rate
        session.transport   = selected_encoding

        # ── Paso 4: Mapear endpoints y asignar bids ──────────────────────
        #
        # La plataforma Avaya asigna bids por flujo. Nosotros los reasignamos
        # secuencialmente para saber, cuando llega un frame, a qué endpoint
        # pertenece y si debemos/podemos hacer eco.
        bid_counter = 0
        for ep in payload.get("mediaEndpoints", []):
            endpoint_id = ep.get("endpointId", "")
            flows       = ep.get("flows", {}).get("audio", {})
            egress_flow = flows.get("egress", {})
            ingress_flow = flows.get("ingress", {})

            # Fuentes del stream egress (audio que viene del cliente)
            sources          = [s for s in egress_flow.get("sources", []) if s != "none"]
            ingress_targets  = ingress_flow.get("target", [])
            supports_ingress = bool(ingress_targets and ingress_targets != ["none"])

            # Registrar cada fuente del stream egress
            if sources:
                egress_bid = bid_counter
                bid_counter += 1
                for src in sources:
                    key = "{bid}:{src}".format(bid=egress_bid, src=SOURCE_TO_INT.get(src, SOURCE_NONE))
                    session.stream_map[key] = {
                        "endpoint_id":      endpoint_id,
                        "direction":        "egress",
                        "supports_ingress": supports_ingress,
                    }
                    logger.debug(
                        "[%s] Stream egress  bid=%d src=%s → endpoint=%s ingress=%s",
                        client_id, egress_bid, src, endpoint_id[:8], supports_ingress,
                    )

            # Registrar el bid de ingress (canal de respuesta del servidor)
            if supports_ingress:
                ingress_bid = bid_counter
                bid_counter += 1
                session.ingress_bid[endpoint_id] = ingress_bid
                key = "{bid}:{src}".format(bid=ingress_bid, src=SOURCE_NONE)
                session.stream_map[key] = {
                    "endpoint_id":      endpoint_id,
                    "direction":        "ingress",
                    "supports_ingress": True,
                }
                logger.debug(
                    "[%s] Stream ingress bid=%d src=none → endpoint=%s",
                    client_id, ingress_bid, endpoint_id[:8],
                )

        self.sessions[session_id] = session

        # ── Paso 5: Enviar session.started ───────────────────────────────
        response = {
            "version":     "1.0.0",
            "type":        "session.started",
            "sessionId":   session_id,
            "sequenceNum": self._next_seq(client_id),
            "timestamp":   self._now(),
            "payload": {
                "mediaTransport": {
                    "type":              transport_block.get("type", "avaya-wss"),
                    "transportEncoding": selected_encoding,
                    "mediaCodecs":       [selected_codec],
                }
            },
        }
        logger.info("[%s] → session.started (codec=%s encoding=%s)", client_id, codec_name, selected_encoding)
        await ws.send(json.dumps(response))

    async def _on_echo_start(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """
        Maneja 'echo.start': activa el eco para el endpointId indicado.
        A partir de este momento, todo frame de audio egress de ese endpoint
        se devolverá inmediatamente como frame de ingress.
        """
        session_id  = data.get("sessionId", "")
        payload     = data.get("payload", {})
        service     = data.get("service", "streaming")
        endpoint_id = payload.get("endpointId", "")

        session = self.sessions.get(session_id)
        if not session:
            logger.warning("[%s] echo.start para sesión desconocida %s", client_id, session_id)
            return

        session.echo_active.add(endpoint_id)
        session.init_ingress(endpoint_id)

        response = {
            "version":     "1.0.0",
            "type":        "echo.started",
            "sessionId":   session_id,
            "sequenceNum": self._next_seq(client_id),
            "timestamp":   self._now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        }
        logger.info("[%s] → echo.started (endpoint=%s)", client_id, endpoint_id)
        await ws.send(json.dumps(response))

    async def _on_echo_end(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """Maneja 'echo.end': desactiva el eco para el endpointId indicado."""
        session_id  = data.get("sessionId", "")
        payload     = data.get("payload", {})
        service     = data.get("service", "streaming")
        endpoint_id = payload.get("endpointId", "")

        session = self.sessions.get(session_id)
        if session:
            session.echo_active.discard(endpoint_id)

        response = {
            "version":     "1.0.0",
            "type":        "echo.ended",
            "sessionId":   session_id,
            "sequenceNum": self._next_seq(client_id),
            "timestamp":   self._now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        }
        logger.info("[%s] → echo.ended (endpoint=%s)", client_id, endpoint_id)
        await ws.send(json.dumps(response))

    async def _on_media_json(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """
        Maneja frames de audio en formato JSON/base64.

        Formato del mensaje entrante:
        {
          "type": "media",
          "bid":  0,
          "src":  "tx",
          "asn":  42,
          "ts":   1234567890,
          "audio": "<base64 PCM>",
          "lastf": false
        }

        El servidor responde con el mismo audio en el bid de ingress del endpoint.
        """
        bid       = data.get("bid", 0)
        source    = data.get("src", "none")
        lastf     = data.get("lastf", False)
        audio_b64 = data.get("audio", "")

        session = self._session_for_client(client_id)
        if not session or not audio_b64:
            return

        stream_key = "{bid}:{src}".format(bid=bid, src=SOURCE_TO_INT.get(source, SOURCE_NONE))
        ep_info    = session.stream_map.get(stream_key)
        if not ep_info or ep_info["direction"] != "egress":
            return
        if not ep_info["supports_ingress"]:
            return

        endpoint_id = ep_info["endpoint_id"]
        if endpoint_id not in session.echo_active:
            return

        ingress_bid = session.ingress_bid.get(endpoint_id)
        if ingress_bid is None:
            return

        seq, ts_us = session.next_ingress_seq_ts(endpoint_id)
        reply: Dict[str, Any] = {
            "type":  "media",
            "bid":   ingress_bid,
            "asn":   seq,
            "ts":    ts_us,
            "audio": audio_b64,   # devolvemos el mismo base64
        }
        if lastf:
            reply["lastf"] = True

        try:
            await ws.send(json.dumps(reply))
        except Exception as e:
            logger.warning("[%s] Error enviando eco JSON: %s", client_id, e)

    async def _on_session_end(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """Maneja 'session.end': elimina la sesión y confirma con 'session.ended'."""
        session_id = data.get("sessionId", "")
        service    = data.get("service", "streaming")

        self.sessions.pop(session_id, None)

        response = {
            "version":     "1.0.0",
            "type":        "session.ended",
            "sessionId":   session_id,
            "sequenceNum": self._next_seq(client_id),
            "timestamp":   self._now(),
            "service":     service,
            "payload":     {"status": {"code": 200, "reason": "NORMAL"}},
        }
        logger.info("[%s] → session.ended", client_id)
        await ws.send(json.dumps(response))

    async def _on_session_ping(
        self,
        ws:        WebSocketServerProtocol,
        client_id: str,
        data:      dict,
    ) -> None:
        """Responde a pings de keepalive con un pong."""
        pong = {
            "version":     "1.0.0",
            "type":        "session.pong",
            "sessionId":   data.get("sessionId", ""),
            "sequenceNum": self._next_seq(client_id),
            "timestamp":   self._now(),
        }
        await ws.send(json.dumps(pong))

    # ─────────────────────────────────────────────────────────────────────
    # SECCIÓN 6: Arranque del servidor
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    async def _process_request(path: str, request_headers) -> Optional[tuple]:
        """
        Intercepta peticiones HTTP antes del handshake WebSocket.

        Render.com (y cualquier load balancer) hace GET /healthz para saber
        si el servicio está vivo. Este handler responde con HTTP 200 OK
        sin necesidad de un servidor HTTP separado.

        Si la ruta NO es /healthz, retorna None para que websockets continúe
        con el handshake WebSocket normal.
        """
        if path == "/healthz":
            return (http.HTTPStatus.OK, [], b"OK\n")
        return None  # Continúa con el upgrade a WebSocket

    async def start(self) -> None:
        """
        Arranca el servidor WebSocket.
        Si se proveen ssl_cert y ssl_key, usa WSS (WebSocket Secure).
        En Render, TLS lo maneja el proxy de la plataforma — no hace falta cert.
        Se ejecuta indefinidamente hasta recibir KeyboardInterrupt.
        """
        ssl_context: Optional[ssl.SSLContext] = None
        if self.ssl_cert and self.ssl_key:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(self.ssl_cert, self.ssl_key)
            logger.info("TLS habilitado — usando WSS")

        scheme = "wss" if ssl_context else "ws"
        logger.info("Echo bot escuchando en %s://%s:%d", scheme, self.host, self.port)

        async with websockets.serve(
            self.handle_connection,
            self.host,
            self.port,
            ssl=ssl_context,
            process_request=self._process_request,  # habilita GET /healthz
        ):
            await asyncio.Future()  # corre indefinidamente
