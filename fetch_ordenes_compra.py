"""
Daily fetcher for Órdenes de Compra from the Mercado Público API (ChileCompra).
Docs: https://api.mercadopublico.cl/modules/OrdenCompra.aspx

Fetches all purchase orders for today's date, appends them to
data/ordenes_compra.csv, and sends a summary email with the CSV attached.

Usage:
    python fetch_ordenes_compra.py [--fecha DDMMAAAA] [--estado <estado>]

Environment variables:
    MP_TICKET       – API ticket (required).
    EMAIL_FROM      – Gmail address used to send the report.
    EMAIL_PASSWORD  – Gmail App Password (not your regular password).
    EMAIL_TO        – Recipient address(es), comma-separated.
"""

import argparse
import csv
import io
import os
import smtplib
import sys
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
CSV_FILE = "data/ordenes_compra.csv"

TICKET = os.getenv("MP_TICKET", "F8537A18-6766-4DEF-9E59-426B4FEE2844")

EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")          # comma-separated if multiple

CSV_FIELDS = [
    "fecha_consulta",
    "CodigoOC",
    "Nombre",
    "Estado",
    "CodigoEstado",
    "FechaCreacion",
    "FechaEnvio",
    "FechaAceptacion",
    "TipoOrden",
    "TipoMoneda",
    "TotalNeto",
    "TotalImpuesto",
    "TotalCargos",
    "TotalDescuentos",
    "TotalBruto",
    "CodigoOrganismo",
    "NombreOrganismo",
    "NombreUnidad",
    "CodigoProveedor",
    "NombreProveedor",
]

# ── API call ─────────────────────────────────────────────────────────────────


def fetch_ordenes(fecha: str, estado: str) -> list[dict]:
    """Call the API and return the Listado array."""
    params = {
        "fecha": fecha,
        "estado": estado,
        "ticket": TICKET,
    }

    print(f"GET {BASE_URL}")
    print(f"  params: fecha={fecha}, estado={estado}")

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    cantidad = data.get("Cantidad", 0)
    print(f"  → {cantidad} orden(es) recibida(s)")

    return data.get("Listado") or []


# ── Row parsing ───────────────────────────────────────────────────────────────


def parse_orden(orden: dict, fecha_consulta: str) -> dict:
    """Flatten a single order dict into a CSV-friendly row."""
    comprador = orden.get("Comprador") or {}
    proveedor = orden.get("Proveedor") or {}

    return {
        "fecha_consulta": fecha_consulta,
        "CodigoOC": orden.get("CodigoOC") or orden.get("Codigo", ""),
        "Nombre": orden.get("Nombre", ""),
        "Estado": orden.get("Estado", ""),
        "CodigoEstado": orden.get("CodigoEstado", ""),
        "FechaCreacion": orden.get("FechaCreacion", ""),
        "FechaEnvio": orden.get("FechaEnvio", ""),
        "FechaAceptacion": orden.get("FechaAceptacion", ""),
        "TipoOrden": orden.get("TipoOrden") or orden.get("Tipo", {}).get("Codigo", ""),
        "TipoMoneda": orden.get("TipoMoneda") or orden.get("UnidadMonetaria", ""),
        "TotalNeto": orden.get("TotalNeto", ""),
        "TotalImpuesto": orden.get("TotalImpuesto", ""),
        "TotalCargos": orden.get("TotalCargos", ""),
        "TotalDescuentos": orden.get("TotalDescuentos", ""),
        "TotalBruto": orden.get("TotalBruto", ""),
        "CodigoOrganismo": comprador.get("CodigoOrganismo", ""),
        "NombreOrganismo": comprador.get("NombreOrganismo", ""),
        "NombreUnidad": comprador.get("NombreUnidad", ""),
        "CodigoProveedor": proveedor.get("CodigoProveedor", ""),
        "NombreProveedor": proveedor.get("NombreProveedor", ""),
    }


# ── CSV writer ────────────────────────────────────────────────────────────────


def save_to_csv(rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"\n{len(rows)} fila(s) guardada(s) en {CSV_FILE}")


# ── Email ─────────────────────────────────────────────────────────────────────


def _build_html_table(rows: list[dict], max_rows: int = 50) -> str:
    """Return an HTML table with the first max_rows of this run."""
    display = rows[:max_rows]
    visible_cols = [
        "CodigoOC", "Estado", "NombreOrganismo",
        "NombreProveedor", "TotalBruto", "TipoMoneda",
    ]

    header = "".join(f"<th>{c}</th>" for c in visible_cols)
    body_rows = ""
    for i, row in enumerate(display):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        cells = "".join(f"<td>{row.get(c, '')}</td>" for c in visible_cols)
        body_rows += f'<tr style="background:{bg}">{cells}</tr>'

    truncation = ""
    if len(rows) > max_rows:
        truncation = (
            f'<p style="color:#888">... y {len(rows) - max_rows} fila(s) más '
            f'en el archivo adjunto.</p>'
        )

    return f"""
    <table border="1" cellpadding="5" cellspacing="0"
           style="border-collapse:collapse;font-family:monospace;font-size:12px">
      <thead style="background:#1a56db;color:white">
        <tr>{header}</tr>
      </thead>
      <tbody>{body_rows}</tbody>
    </table>
    {truncation}
    """


def _rows_to_csv_bytes(rows: list[dict]) -> bytes:
    """Serialize rows to CSV bytes for the email attachment."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")  # utf-8-sig = Excel-friendly BOM


def send_email(rows: list[dict], fecha_consulta: str, estado: str) -> None:
    """Send a summary email with an HTML table and the CSV attached."""
    if not all([EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO]):
        print("Correo no configurado — saltando envío de email.")
        return

    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]
    subject = (
        f"OC Mercado Público · {fecha_consulta} · "
        f"{len(rows)} orden(es) · estado={estado}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)

    # ── Plain text fallback ──
    plain = (
        f"Órdenes de Compra — {fecha_consulta}\n"
        f"Estado: {estado}\n"
        f"Total órdenes: {len(rows)}\n\n"
        "Ver adjunto para el detalle completo."
    )
    msg.attach(MIMEText(plain, "plain", "utf-8"))

    # ── HTML body ──
    html = f"""
    <html><body>
      <h2 style="color:#1a56db">Mercado Público — Órdenes de Compra</h2>
      <p><b>Fecha:</b> {fecha_consulta} &nbsp;|&nbsp;
         <b>Estado:</b> {estado} &nbsp;|&nbsp;
         <b>Total:</b> {len(rows)} orden(es)</p>
      {_build_html_table(rows)}
      <p style="color:#aaa;font-size:11px">
        Generado automáticamente · GitHub Actions
      </p>
    </body></html>
    """
    msg.attach(MIMEText(html, "html", "utf-8"))

    # ── CSV attachment ──
    csv_bytes = _rows_to_csv_bytes(rows)
    attachment = MIMEBase("application", "octet-stream")
    attachment.set_payload(csv_bytes)
    encoders.encode_base64(attachment)
    filename = f"ordenes_compra_{fecha_consulta}_{estado}.csv"
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    # ── Send via Gmail SMTP ──
    print(f"\nEnviando correo a {recipients} …")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_bytes())

    print("Correo enviado correctamente.")


# ── CLI entry point ───────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily OC from Mercado Público")
    parser.add_argument(
        "--fecha",
        default=datetime.now().strftime("%d%m%Y"),
        help="Date in DDMMAAAA format (default: today)",
    )
    parser.add_argument(
        "--estado",
        default="todos",
        help="Order state filter: todos, aceptada, enviadaproveedor, etc.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fecha_consulta = datetime.now().strftime("%Y-%m-%d")

    listado = fetch_ordenes(fecha=args.fecha, estado=args.estado)

    if not listado:
        print("No se encontraron órdenes para los filtros indicados.")
        return

    rows = [parse_orden(orden, fecha_consulta) for orden in listado]
    save_to_csv(rows)

    # Print a quick summary to the Actions log
    print("\n── Resumen ─────────────────────────────────────────────────────")
    for row in rows[:10]:
        print(
            f"  {row['CodigoOC']:<20} {row['Estado']:<20} "
            f"{row['TotalBruto']:>15} {row['TipoMoneda']}"
        )
    if len(rows) > 10:
        print(f"  ... y {len(rows) - 10} más")

    send_email(rows, fecha_consulta, args.estado)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"Error HTTP {e.response.status_code}: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Error de red: {e}", file=sys.stderr)
        sys.exit(1)
