"""
Daily fetcher for Órdenes de Compra from the Mercado Público API (ChileCompra).
Docs: https://api.mercadopublico.cl/modules/OrdenCompra.aspx

Fetches all purchase orders for today's date and appends them to
data/ordenes_compra.csv.

Usage:
    python fetch_ordenes_compra.py [--fecha DDMMAAAA] [--estado <estado>]

Environment variables:
    MP_TICKET  – API ticket (required). Get yours at https://api.mercadopublico.cl
"""

import argparse
import csv
import os
import sys
from datetime import datetime

import requests

# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
CSV_FILE = "data/ordenes_compra.csv"

# Read ticket from env var so it never needs to be hard-coded in source.
# Fallback to the public test ticket only for development.
TICKET = os.getenv("MP_TICKET", "F8537A18-6766-4DEF-9E59-426B4FEE2844")

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

    # Print a quick summary table to the Actions log
    print("\n── Resumen ─────────────────────────────────────────────────────")
    for row in rows[:10]:
        print(f"  {row['CodigoOC']:<20} {row['Estado']:<20} {row['TotalBruto']:>15} {row['TipoMoneda']}")
    if len(rows) > 10:
        print(f"  ... y {len(rows) - 10} más")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"Error HTTP {e.response.status_code}: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Error de red: {e}", file=sys.stderr)
        sys.exit(1)
