#!/usr/bin/env python3
"""
Punto de entrada del Echo Bot — Avaya RTCM.

Uso básico (WebSocket sin TLS):
    python main.py

Uso con TLS (WebSocket Secure):
    python main.py --ssl-cert cert/cert.pem --ssl-key cert/key.pem

Opciones completas:
    python main.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from echo_server import EchoServer


# ─────────────────────────────────────────────────────────────────────────────
# Configuración de logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_file: str, verbose: bool) -> None:
    """
    Configura logging a consola y a archivo.
    Si el archivo de log ya existe lo rota añadiendo la extensión .bak.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt   = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    # Salida a consola
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Salida a archivo (con rotación al inicio)
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if os.path.isfile(log_file):
            bak = log_file + ".bak"
            try:
                os.replace(log_file, bak)
            except OSError as e:
                print(f"Advertencia: no se pudo rotar {log_file}: {e}", file=sys.stderr)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)


# ─────────────────────────────────────────────────────────────────────────────
# Punto de entrada principal
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Avaya RTCM Echo Bot — refleja el audio del cliente de vuelta.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Interfaz de red en la que escuchar (default: 0.0.0.0)",
    )
    # Render.com inyecta la variable de entorno PORT automáticamente.
    # --port la sobreescribe si se pasa explícitamente.
    render_port = int(os.environ.get("PORT", 8765))
    parser.add_argument(
        "--port",
        type=int,
        default=render_port,
        help="Puerto TCP (default: variable $PORT o 8765)",
    )
    parser.add_argument(
        "--ssl-cert",
        default=None,
        metavar="CERT",
        help="Ruta al certificado TLS (.pem). Habilita WSS.",
    )
    parser.add_argument(
        "--ssl-key",
        default=None,
        metavar="KEY",
        help="Ruta a la clave privada TLS (.pem). Requerida si --ssl-cert está presente.",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("LOG_FILE", ""),  # vacío = solo consola (recomendado en Render)
        help="Archivo de log. Vacío = solo consola (default en Render).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Activa logging de nivel DEBUG",
    )
    args = parser.parse_args()

    # Configurar logging antes de cualquier otra cosa
    setup_logging(args.log_file, args.verbose)

    logger = logging.getLogger(__name__)

    # Resolver rutas de certificados relativas al directorio de este script
    base     = Path(__file__).parent
    ssl_cert = ssl_key = None

    if args.ssl_cert:
        ssl_cert = args.ssl_cert if os.path.isabs(args.ssl_cert) else str(base / args.ssl_cert)
        if not os.path.isfile(ssl_cert):
            sys.exit(f"Error: certificado TLS no encontrado: {ssl_cert}")

    if args.ssl_key:
        ssl_key = args.ssl_key if os.path.isabs(args.ssl_key) else str(base / args.ssl_key)
        if not os.path.isfile(ssl_key):
            sys.exit(f"Error: clave TLS no encontrada: {ssl_key}")

    if bool(ssl_cert) != bool(ssl_key):
        sys.exit("Error: --ssl-cert y --ssl-key deben especificarse juntos.")

    # Crear y arrancar el servidor
    server = EchoServer(
        host     = args.host,
        port     = args.port,
        ssl_cert = ssl_cert,
        ssl_key  = ssl_key,
    )

    logger.info("Iniciando Echo Bot  host=%s  port=%d  tls=%s", args.host, args.port, bool(ssl_cert))
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info("Echo Bot detenido por el usuario")


if __name__ == "__main__":
    main()
