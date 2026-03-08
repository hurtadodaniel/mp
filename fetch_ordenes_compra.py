"""
Pipeline diario de Órdenes de Compra — Mercado Público (ChileCompra).

Extrae el listado de OC del día, consulta el detalle de cada una
(con lógica incremental: no re-consulta OCs cuyo detalle ya existe
en el CSV consolidado), clasifica por categoría de producto, y guarda
todo en CSVs incrementales dentro de data/.

Uso:
    python fetch_ordenes_compra.py [--fecha DDMMAAAA]

Variables de entorno:
    MP_TICKET       – API ticket (requerido).
    EMAIL_FROM      – Gmail para enviar el reporte (opcional).
    EMAIL_PASSWORD  – Gmail App Password (opcional).
    EMAIL_TO        – Destinatarios separados por coma (opcional).
"""

import argparse
import ast
import csv
import io
import json
import os
import random
import re
import smtplib
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from threading import Semaphore

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from google import genai as _genai_module
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────

TICKET = os.getenv("MP_TICKET") or "F8537A18-6766-4DEF-9E59-426B4FEE2844"
LISTA_PROVEEDORES = ["27693"]

URL_LISTADO = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
URL_DETALLE = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"

MAX_WORKERS = 10
DELAY_POR_HILO = 1.5
MAX_RETRIES = 4
MAX_RETRIES_LISTADO = 8
MAX_RETRIES_SEQ = 3
BACKOFF = 2.5
BACKOFF_MAX = 40
BACKOFF_MAX_SEQ = 10

DATA_DIR = Path("data")
CSV_LISTADO = DATA_DIR / "ordenes_listado.csv"
CSV_DETALLE = DATA_DIR / "ordenes_detalle.csv"
CSV_CONSOLIDADO = DATA_DIR / "ordenes_consolidado.csv"

EMAIL_FROM = os.getenv("EMAIL_FROM") or "hurtadodaniel.cl@gmail.com"
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD") or "jcbf wpfn psqb tfnx"
EMAIL_TO = os.getenv("EMAIL_TO") or "hurtadodaniel.cl@gmail.com"

# ── ALARMAS ───────────────────────────────────────────────────────────────────

EMAIL_ALERTAS = os.getenv("EMAIL_ALERTAS") or EMAIL_FROM
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CLIENTES_PRIORITARIOS_PATH = DATA_DIR / "clientes_prioritarios.json"
ALARMAS_PATH = DATA_DIR / "alarmas.csv"
GESTIONES_PATH = DATA_DIR / "gestiones.csv"

COLS_ALARMAS = [
    "id_alarma", "codigo_oc", "prefijo_cliente", "nombre_organismo",
    "monto", "fecha_creacion", "fecha_envio", "fecha_cierre", "estado_oc", "categoria",
    "plazo", "fecha_limite",
    "fecha_detectada", "estado_alarma", "fecha_gestion", "ejecutivo_gestion",
]

# ── SCHEMA (garantiza columnas aunque no haya datos) ──────────────────────────

COLS_LISTADO = [
    "Codigo", "Nombre", "Tipo", "Estado",
    "CodigoOrganismoPublico", "NombreOrganismoPublico",
    "FechaCreacion", "FechaCierre",
    "CodigoProveedorConsultado", "FechaConsulta",
]

COLS_DETALLE = [
    "Codigo", "Nombre", "Descripcion", "Tipo", "Estado",
    "NombreProductoGenerico", "EspecificacionComprador", "EspecificacionProveedor",
    "Items.Listado", "Fechas.FechaEnvio",
    "CodigoOrganismoPublico", "NombreOrganismoPublico",
    "Monto", "CategoriaProducto", "FechaProcesamiento",
]


def _empty_listado():
    return pd.DataFrame(columns=COLS_LISTADO)


def _empty_detalles():
    return pd.DataFrame(columns=COLS_DETALLE)


# ── BACKOFF ───────────────────────────────────────────────────────────────────

def _wait_backoff(intento, backoff=BACKOFF, max_wait=BACKOFF_MAX):
    return min(backoff ** intento + random.uniform(0, 2.0), max_wait)


# ── LÓGICA INCREMENTAL (CSV como fuente de dedup) ────────────────────────────

def _load_existing_detail_codes() -> set:
    """Lee el CSV de detalle existente y retorna los códigos ya consultados."""
    if not CSV_DETALLE.exists():
        return set()
    try:
        df = pd.read_csv(CSV_DETALLE, usecols=["Codigo"], dtype=str)
        codes = set(df["Codigo"].dropna().unique())
        print(f"  CSV existente: {len(codes)} OC con detalle previo.")
        return codes
    except Exception as e:
        print(f"  Advertencia leyendo CSV existente: {e}")
        return set()


def _load_existing_detail_df() -> pd.DataFrame:
    """Carga el DataFrame completo de detalles existentes."""
    if not CSV_DETALLE.exists():
        return _empty_detalles()
    try:
        return pd.read_csv(CSV_DETALLE, dtype=str, low_memory=False)
    except Exception:
        return _empty_detalles()


# ── FETCH LISTADO ─────────────────────────────────────────────────────────────

def _fetch_listado(proveedor, fecha, ticket, url, max_retries=MAX_RETRIES_LISTADO):
    last_error = None
    for intento in range(1, max_retries + 1):
        try:
            r = requests.get(url, params={
                "fecha": fecha, "CodigoProveedor": proveedor, "ticket": ticket
            }, verify=False, timeout=25)
            if r.status_code in (500, 502, 503, 504):
                wait = _wait_backoff(intento)
                body = r.text[:300] if r.text else "(sin cuerpo)"
                print(f"    HTTP {r.status_code} — intento {intento}/{max_retries}, "
                      f"reintentando en {wait:.0f}s... | body: {body}")
                last_error = f"HTTP {r.status_code}"
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            wait = _wait_backoff(intento)
            print(f"    Timeout — intento {intento}/{max_retries}, "
                  f"reintentando en {wait:.0f}s...")
            last_error = "Timeout"
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = _wait_backoff(intento)
            print(f"    ConnectionError — intento {intento}/{max_retries}, "
                  f"reintentando en {wait:.0f}s...")
            last_error = "ConnectionError"
            time.sleep(wait)
        except Exception as e:
            last_error = str(e)
            if intento == max_retries:
                raise
            wait = _wait_backoff(intento)
            print(f"    Error ({e}) — intento {intento}/{max_retries}, "
                  f"reintentando en {wait:.0f}s...")
            time.sleep(wait)
    raise RuntimeError(
        f"Listado: {max_retries} reintentos agotados para proveedor {proveedor}. "
        f"Último error: {last_error}"
    )


# ── CLASIFICACIÓN ─────────────────────────────────────────────────────────────

def _items_text(row, fields=None):
    if fields is None:
        fields = ["Categoria", "Producto",
                  "EspecificacionComprador", "EspecificacionProveedor"]
    items = row.get("Items.Listado")
    if items is None:
        return ""
    if isinstance(items, str):
        try:
            items = ast.literal_eval(items)
        except Exception:
            return ""
    if not isinstance(items, list):
        return ""
    text = ""
    for item in items:
        if isinstance(item, dict):
            for f in fields:
                v = item.get(f)
                if v and pd.notna(v):
                    text += " " + str(v)
    return text.lower()


def classify_row(row):
    tipo = str(row.get("Tipo", "")).strip().upper()
    prod_name = str(row.get("NombreProductoGenerico", "") or "").strip()
    clean_prod = prod_name.lower()

    if tipo == "CM":
        if clean_prod.startswith("supermercado"):
            return "SUPERMERCADO"
        if clean_prod.startswith("alimentaci"):
            return "ALIMENTACION"
        if clean_prod.startswith("vestuario"):
            return "VESTUARIO_Y_CALZADO"
        it = _items_text(row, [
            "EspecificacionComprador", "EspecificacionProveedor",
            "Categoria", "Producto",
        ])
        if it:
            if "supermercado" in it:
                return "SUPERMERCADO"
            if "alimentaci" in it or "alimento" in it:
                return "ALIMENTACION"
            if "vestuario" in it or "calzado" in it:
                return "VESTUARIO_Y_CALZADO"
            if "sala cuna" in it or "jardin" in it:
                return "SALA CUNA"
        return prod_name or "SIN CLASIFICAR"

    text = " ".join(
        str(row.get(c, "") or "")
        for c in ["Nombre", "Descripcion", "NombreProductoGenerico",
                   "EspecificacionComprador", "EspecificacionProveedor"]
    )
    text += " " + _items_text(row)
    text = text.lower()

    kw_map = [
        ("ALIMENTACION", [
            "alimentaci", "alimentos", "restaurant", "recarga tarjeta",
            "rancho", "colaci", "raciones", "edenred",
        ]),
        ("SALA CUNA", ["sala cuna", "salas cunas", "jardin", "jardín"]),
        ("VESTUARIO_Y_CALZADO", ["vestuario", "calzado", "giftcard vestuario"]),
        ("SUPERMERCADO", ["supermercado"]),
    ]
    for cat, kws in kw_map:
        if any(kw in text for kw in kws):
            return cat
    return "SIN CLASIFICAR"


# ── EMAIL ─────────────────────────────────────────────────────────────────────

def _build_html_table(rows: list[dict], max_rows: int = 50) -> str:
    display_rows = rows[:max_rows]
    visible_cols = [
        "Codigo", "Estado", "NombreOrganismoPublico",
        "NombreProductoGenerico", "CategoriaProducto", "Monto",
    ]
    header = "".join(f"<th>{c}</th>" for c in visible_cols)
    body_rows = ""
    for i, row in enumerate(display_rows):
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


def send_email(rows_del_dia: list[dict], fecha_consulta: str) -> None:
    if not all([EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO]):
        print("  Correo no configurado — saltando envío de email.")
        return

    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]
    subject = (
        f"OC Mercado Público · {fecha_consulta} · "
        f"{len(rows_del_dia)} orden(es) del día"
    )

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)

    plain = (
        f"Órdenes de Compra — {fecha_consulta}\n"
        f"Total órdenes del día: {len(rows_del_dia)}\n\n"
        "Ver adjunto para el detalle completo."
    )
    html = f"""
    <html><body>
      <h2 style="color:#1a56db">Mercado Público — Órdenes de Compra</h2>
      <p><b>Fecha:</b> {fecha_consulta} &nbsp;|&nbsp;
         <b>Total del día:</b> {len(rows_del_dia)} orden(es)</p>
      {_build_html_table(rows_del_dia)}
      <p style="color:#aaa;font-size:11px">
        Generado automáticamente · GitHub Actions
      </p>
    </body></html>
    """

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alt)

    # CSV adjunto con todas las OC del día
    buf = io.StringIO()
    if rows_del_dia:
        writer = csv.DictWriter(buf, fieldnames=list(rows_del_dia[0].keys()),
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_del_dia)
    csv_bytes = buf.getvalue().encode("utf-8-sig")
    attachment = MIMEBase("application", "octet-stream")
    attachment.set_payload(csv_bytes)
    encoders.encode_base64(attachment)
    filename = f"ordenes_compra_{fecha_consulta}.csv"
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    print(f"  Enviando correo a {recipients} …")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_bytes())
    print("  Correo enviado correctamente.")


# ── SISTEMA DE ALARMAS ────────────────────────────────────────────────────────

def cargar_clientes_prioritarios() -> tuple:
    """Retorna (prefijos_set, plazos_dict) desde clientes_prioritarios.json.

    prefijos_set: set de strings para búsqueda rápida O(1).
    plazos_dict:  {prefijo: plazo_str} con el SLA de cada cliente.
    En caso de error retorna (set(), {}).
    """
    if not CLIENTES_PRIORITARIOS_PATH.exists():
        print("  [ALARMAS] clientes_prioritarios.json no encontrado — alarmas desactivadas.")
        return set(), {}
    try:
        with open(CLIENTES_PRIORITARIOS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        clientes = data.get("clientes", [])
        prefijos = {str(c["prefijo"]).strip() for c in clientes if c.get("prefijo")}
        plazos = {str(c["prefijo"]).strip(): c.get("plazo", "") for c in clientes if c.get("prefijo")}
        prefijos.discard("EJEMPLO")
        plazos.pop("EJEMPLO", None)
        if not prefijos:
            print("  [ALARMAS] Sin clientes prioritarios configurados.")
        else:
            print(f"  [ALARMAS] Clientes prioritarios: {sorted(prefijos)}")
        return prefijos, plazos
    except Exception as e:
        print(f"  [ALARMAS] Error leyendo clientes_prioritarios.json: {e}")
        return set(), {}


def parsear_plazo(plazo_str: str, fecha_envio_str: str) -> str:
    """Calcula fecha_limite sumando el plazo a fecha_envio.

    Patrones soportados:
      - "N horas desde emisión"  → fecha_envio + N horas
      - "N días corridos"        → fecha_envio + N días
      - Cualquier otro patrón    → "" (sin deadline calculable)

    Retorna string ISO o "" en caso de error/patrón desconocido.
    """
    if not plazo_str or not fecha_envio_str:
        return ""
    try:
        fecha_envio = datetime.fromisoformat(fecha_envio_str)
    except (ValueError, TypeError):
        return ""
    plazo_lower = plazo_str.lower()
    m_horas = re.search(r"(\d+)\s*hora", plazo_lower)
    m_dias = re.search(r"(\d+)\s*d[íi]a", plazo_lower)
    if m_horas:
        return (fecha_envio + timedelta(hours=int(m_horas.group(1)))).isoformat()
    if m_dias:
        return (fecha_envio + timedelta(days=int(m_dias.group(1)))).isoformat()
    return ""


_URGENCY_ORDER = {"VENCIDA": 0, "URGENTE": 1, "HOY": 2, "A_TIEMPO": 3, "": 4}


def clasificar_urgencia(fecha_limite_str: str) -> str:
    """Clasifica urgencia según tiempo restante hasta fecha_limite.

    Retorna: "VENCIDA" | "URGENTE" (< 4h) | "HOY" | "A_TIEMPO" | ""
    Vacío se retorna cuando no hay fecha_limite definida (sin SLA calculable).
    """
    if not fecha_limite_str or str(fecha_limite_str).strip() in ("", "nan", "None"):
        return ""
    try:
        tz_chile = ZoneInfo("America/Santiago")
        ahora = datetime.now(tz_chile)
        fecha_limite = datetime.fromisoformat(str(fecha_limite_str))
        if fecha_limite.tzinfo is None:
            fecha_limite = fecha_limite.replace(tzinfo=tz_chile)
        remaining = fecha_limite - ahora
        if remaining.total_seconds() < 0:
            return "VENCIDA"
        if remaining.total_seconds() < 4 * 3600:
            return "URGENTE"
        if fecha_limite.date() == ahora.date():
            return "HOY"
        return "A_TIEMPO"
    except (ValueError, TypeError):
        return ""


def cargar_alarmas_existentes() -> pd.DataFrame:
    """Carga el historial de alarmas existentes o retorna DataFrame vacío."""
    if not ALARMAS_PATH.exists():
        return pd.DataFrame(columns=COLS_ALARMAS)
    try:
        df = pd.read_csv(ALARMAS_PATH, dtype=str)
        for col in COLS_ALARMAS:
            if col not in df.columns:
                df[col] = ""
        return df[COLS_ALARMAS]
    except Exception as e:
        print(f"  [ALARMAS] Error leyendo alarmas.csv: {e}")
        return pd.DataFrame(columns=COLS_ALARMAS)


def aplicar_gestiones(df_alarmas: pd.DataFrame) -> pd.DataFrame:
    """Marca alarmas como GESTIONADA según las filas en gestiones.csv."""
    if not GESTIONES_PATH.exists() or df_alarmas.empty:
        return df_alarmas
    try:
        df_gest = pd.read_csv(GESTIONES_PATH, dtype=str)
        if df_gest.empty or "codigo_oc" not in df_gest.columns:
            return df_alarmas
        for _, g in df_gest.iterrows():
            codigo = str(g.get("codigo_oc", "")).strip()
            if not codigo:
                continue
            mask = (df_alarmas["codigo_oc"] == codigo) & (df_alarmas["estado_alarma"] == "ACTIVA")
            if mask.any():
                df_alarmas.loc[mask, "estado_alarma"] = "GESTIONADA"
                df_alarmas.loc[mask, "fecha_gestion"] = str(g.get("fecha_gestion", "")).strip()
                df_alarmas.loc[mask, "ejecutivo_gestion"] = str(g.get("ejecutivo", "")).strip()
                print(f"  [ALARMAS] OC {codigo} marcada GESTIONADA por {g.get('ejecutivo', '?')}")
    except Exception as e:
        print(f"  [ALARMAS] Error procesando gestiones.csv: {e}")
    return df_alarmas


def detectar_nuevas_alarmas(
    df_consol: pd.DataFrame,
    prefijos_prioritarios: set,
    df_alarmas_existentes: pd.DataFrame,
    plazos_dict: dict = None,
) -> pd.DataFrame:
    """Detecta OC nuevas de clientes prioritarios que aún no tienen alarma."""
    if plazos_dict is None:
        plazos_dict = {}
    if df_consol.empty or not prefijos_prioritarios:
        return pd.DataFrame(columns=COLS_ALARMAS)

    # Extraer prefijo (numero antes del primer guion)
    df = df_consol.copy()
    df["_prefijo"] = df["Codigo"].astype(str).str.split("-").str[0].str.strip()

    df_match = df[df["_prefijo"].isin(prefijos_prioritarios)].copy()
    if df_match.empty:
        return pd.DataFrame(columns=COLS_ALARMAS)

    # Excluir OC que ya tienen alarma registrada (sin importar estado)
    codigos_con_alarma = set(df_alarmas_existentes["codigo_oc"].dropna().unique()) if not df_alarmas_existentes.empty else set()
    df_nuevas = df_match[~df_match["Codigo"].isin(codigos_con_alarma)]

    if df_nuevas.empty:
        return pd.DataFrame(columns=COLS_ALARMAS)

    def _get(row, *cols):
        for c in cols:
            v = str(row.get(c, "") or "").strip()
            if v and v.lower() not in ("nan", "none", ""):
                return v
        return ""

    ahora = datetime.now().isoformat()
    filas = []
    for _, row in df_nuevas.iterrows():
        prefijo = _get(row, "_prefijo")
        fecha_envio = _get(row, "Fechas.FechaEnvio")
        plazo = plazos_dict.get(prefijo, "")
        filas.append({
            "id_alarma": str(uuid.uuid4()),
            "codigo_oc": _get(row, "Codigo"),
            "prefijo_cliente": prefijo,
            "nombre_organismo": _get(row, "Comprador.NombreOrganismo", "NombreOrganismoPublico_base"),
            "monto": _get(row, "Monto", "TotalNeto", "Total"),
            "fecha_creacion": _get(row, "Fechas.FechaCreacion", "FechaCreacion"),
            "fecha_envio": fecha_envio,
            "fecha_cierre": _get(row, "Fechas.FechaCancelacion", "FechaCierre"),
            "estado_oc": _get(row, "Estado"),
            "categoria": _get(row, "CategoriaProducto"),
            "plazo": plazo,
            "fecha_limite": parsear_plazo(plazo, fecha_envio),
            "fecha_detectada": ahora,
            "estado_alarma": "ACTIVA",
            "fecha_gestion": "",
            "ejecutivo_gestion": "",
        })

    print(f"  [ALARMAS] {len(filas)} OC nueva(s) de clientes prioritarios detectadas.")
    return pd.DataFrame(filas, columns=COLS_ALARMAS)


def guardar_alarmas(df_alarmas: pd.DataFrame) -> None:
    """Escribe el CSV completo de alarmas (reemplaza)."""
    df_alarmas.to_csv(ALARMAS_PATH, index=False, encoding="utf-8")
    print(f"  [ALARMAS] alarmas.csv guardado: {len(df_alarmas)} registros totales.")


def generar_resumen_gemini(df_activas: pd.DataFrame) -> str:
    """Genera resumen ejecutivo en español usando Gemini AI.

    Retorna string con 2-3 oraciones, o "" si no disponible/falla.
    Nunca lanza excepción — fallo silencioso.
    """
    if not _GENAI_AVAILABLE or not GEMINI_API_KEY or df_activas.empty:
        return ""
    try:
        cols_payload = [c for c in ["codigo_oc", "nombre_organismo", "monto", "fecha_envio",
                                     "fecha_limite", "plazo", "urgencia"] if c in df_activas.columns]
        alarmas_data = df_activas[cols_payload].head(20).to_dict("records")
        json_payload = json.dumps(alarmas_data, ensure_ascii=False, default=str)
        prompt = (
            "Eres el asistente de operaciones de una empresa de servicios de alimentación en Chile "
            "que vende a organismos públicos a través de Mercado Público.\n\n"
            f"Tienes las siguientes alarmas activas de órdenes de compra de clientes prioritarios:\n{json_payload}\n\n"
            "Genera un resumen ejecutivo de 2 a 3 oraciones en español para el equipo de operaciones. "
            "El resumen debe: "
            "1. Identificar cuántas OC requieren acción inmediata (VENCIDA o URGENTE) y nombrar los clientes más críticos. "
            "2. Mencionar el monto total aproximado en juego si es relevante. "
            "3. Sugerir la prioridad de acción para hoy. "
            "Sé directo y usa lenguaje operacional. No uses bullet points, solo párrafo corrido."
        )
        client = _genai_module.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return response.text.strip()
    except Exception as e:
        print(f"  [GEMINI] Error generando resumen IA: {e}")
        return ""


def enviar_email_resumen(df_activas: pd.DataFrame, df_historial: pd.DataFrame, n_dias: int = 7) -> None:
    """Envía digest con alarmas activas + historial de los últimos n_dias días."""
    if not all([EMAIL_FROM, EMAIL_PASSWORD, EMAIL_ALERTAS]):
        print("  [RESUMEN] Email no configurado — saltando.")
        return

    recipients = [r.strip() for r in EMAIL_ALERTAS.split(",") if r.strip()]
    ahora = datetime.now(ZoneInfo("America/Santiago")).strftime("%Y-%m-%d %H:%M")
    if len(df_activas) == 0:
        subject = f"✅ Bot activo · Sin alarmas pendientes · {ahora}"
    else:
        subject = f"⚠️ Resumen Alarmas · {len(df_activas)} activa(s) · {ahora}"

    cols_show = [
        "codigo_oc", "nombre_organismo", "monto", "fecha_envio",
        "plazo", "fecha_limite", "urgencia",
        "categoria", "fecha_cierre", "estado_alarma", "ejecutivo_gestion",
    ]

    # Calcular urgencia y ordenar activas (más urgente primero)
    if not df_activas.empty:
        df_activas = df_activas.copy()
        df_activas["urgencia"] = df_activas["fecha_limite"].apply(clasificar_urgencia)
        df_activas["_rank"] = df_activas["urgencia"].map(_URGENCY_ORDER).fillna(4)
        df_activas = df_activas.sort_values("_rank").drop(columns=["_rank"])

    _BADGE_COLORS = {
        "VENCIDA": ("#dc3545", "white"),
        "URGENTE": ("#fd7e14", "white"),
        "HOY":     ("#ffc107", "#222"),
        "A_TIEMPO": ("#28a745", "white"),
    }

    def _tabla(df, highlight_col="estado_alarma"):
        if df.empty:
            return "<p style='color:#888;font-style:italic'>Sin registros.</p>"
        header = "".join(
            f"<th style='padding:6px 10px;text-align:left;white-space:nowrap'>{c}</th>"
            for c in cols_show if c in df.columns
        )
        rows_html = ""
        for i, (_, row) in enumerate(df.iterrows()):
            estado = str(row.get("estado_alarma", ""))
            if estado == "ACTIVA":
                bg = "#fff3cd" if i % 2 == 0 else "#fff8e7"
            elif estado == "GESTIONADA":
                bg = "#d4edda" if i % 2 == 0 else "#e8f5e9"
            else:
                bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
            cells = ""
            for c in cols_show:
                if c not in df.columns:
                    continue
                val = str(row.get(c, "") or "")
                if c == "urgencia" and val in _BADGE_COLORS:
                    bg_c, txt_c = _BADGE_COLORS[val]
                    cell_html = (
                        f"<span style='background:{bg_c};color:{txt_c};"
                        f"padding:2px 7px;border-radius:3px;font-weight:bold;"
                        f"font-size:11px'>{val}</span>"
                    )
                else:
                    cell_html = val
                cells += f"<td style='padding:5px 10px'>{cell_html}</td>"
            rows_html += f"<tr style='background:{bg}'>{cells}</tr>"
        return f"""
        <table border="0" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-family:monospace;font-size:12px;width:100%;margin-bottom:16px">
          <thead style="background:#1a56db;color:white"><tr>{header}</tr></thead>
          <tbody>{rows_html}</tbody>
        </table>"""

    n_gestionadas = len(df_historial[df_historial["estado_alarma"] == "GESTIONADA"]) if not df_historial.empty else 0
    repo_url = "https://github.com/hurtadodaniel/mp/edit/master/data/gestiones.csv"

    # Panel IA — generado antes de construir el HTML
    resumen_ia = generar_resumen_gemini(df_activas)
    gemini_panel_html = ""
    if resumen_ia:
        gemini_panel_html = f"""
        <div style="background:#f0f4ff;border-left:4px solid #1a56db;
                    padding:14px 18px;margin-bottom:18px;border-radius:0 4px 4px 0">
          <p style="margin:0 0 6px;font-size:11px;color:#555;font-weight:bold;
                    text-transform:uppercase;letter-spacing:0.5px">Resumen IA · Gemini</p>
          <p style="margin:0;font-size:13px;color:#222;line-height:1.6">{resumen_ia}</p>
        </div>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;max-width:960px;margin:auto">
      <div style="background:#1a56db;color:white;padding:16px 24px;border-radius:6px 6px 0 0">
        <h2 style="margin:0">📋 Resumen de Alarmas — Mercado Público</h2>
        <p style="margin:4px 0 0">{ahora} · Últimos {n_dias} días</p>
      </div>
      <div style="background:#f8faff;border:2px solid #1a56db;padding:16px 24px">

        {gemini_panel_html}

        <h3 style="color:#c0392b;margin-top:0">
          🔴 Alarmas ACTIVAS ({len(df_activas)})
        </h3>
        {_tabla(df_activas)}

        <h3 style="color:#27ae60">
          ✅ Gestionadas en los últimos {n_dias} días ({n_gestionadas})
        </h3>
        {_tabla(df_historial[df_historial["estado_alarma"] == "GESTIONADA"]) if not df_historial.empty else "<p style='color:#888'>Sin registros.</p>"}

        <h3 style="color:#555">
          📅 Historial completo últimos {n_dias} días ({len(df_historial)})
        </h3>
        {_tabla(df_historial)}

        <hr style="border:none;border-top:1px solid #ddd;margin:16px 0">
        <p style="font-size:12px;color:#555">
          Para marcar una OC como gestionada:
          <a href="{repo_url}">Editar gestiones.csv en GitHub</a>
        </p>
        <p style="color:#aaa;font-size:11px">Generado automáticamente · GitHub Actions · Mercado Público</p>
      </div>
    </body></html>
    """

    plain_activas = "\n".join(
        f"  [{r['estado_alarma']}] {r['codigo_oc']} | {r['nombre_organismo']} "
        f"| urgencia: {r.get('urgencia','')} | límite: {r.get('fecha_limite','')} "
        f"| {r.get('categoria','')} | cierre: {r.get('fecha_cierre','')}"
        for _, r in df_activas.iterrows()
    )
    plain = (
        f"RESUMEN ALARMAS — {ahora}\n"
        f"Activas: {len(df_activas)} | Gestionadas últimos {n_dias}d: {n_gestionadas}\n"
    )
    if resumen_ia:
        plain += f"\nRESUMEN IA:\n{resumen_ia}\n"
    plain += f"\n{plain_activas}\n\nGestionar en: {repo_url}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    print(f"  [RESUMEN] Enviando digest a {recipients} …")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_bytes())
    print("  [RESUMEN] Digest enviado correctamente.")


def enviar_email_alarmas(nuevas: pd.DataFrame, activas: pd.DataFrame) -> None:
    """Envía email de alerta con nuevas OC prioritarias y resumen de activas."""
    if nuevas.empty:
        return
    if not all([EMAIL_FROM, EMAIL_PASSWORD, EMAIL_ALERTAS]):
        print("  [ALARMAS] Email no configurado — saltando alerta.")
        return

    recipients = [r.strip() for r in EMAIL_ALERTAS.split(",") if r.strip()]
    fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"\u26a0\ufe0f ALARMA \u00b7 {len(nuevas)} OC Prioritaria(s) Nueva(s) \u00b7 {fecha_hoy}"

    def _tabla_alarmas(df, highlight=False):
        cols_show = ["codigo_oc", "nombre_organismo", "monto", "categoria",
                     "fecha_creacion", "fecha_envio", "fecha_cierre", "estado_oc"]
        header = "".join(f"<th style='padding:6px 10px;text-align:left'>{c}</th>" for c in cols_show)
        rows_html = ""
        for i, (_, row) in enumerate(df.iterrows()):
            bg = "#fff3cd" if highlight and i % 2 == 0 else ("#fff8e7" if highlight else ("#f9f9f9" if i % 2 == 0 else "#ffffff"))
            cells = "".join(f"<td style='padding:5px 10px'>{row.get(c, '')}</td>" for c in cols_show)
            rows_html += f"<tr style='background:{bg}'>{cells}</tr>"
        thead_bg = "#c0392b" if highlight else "#555"
        return f"""
        <table border="0" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-family:monospace;font-size:12px;width:100%;margin-bottom:16px">
          <thead style="background:{thead_bg};color:white">
            <tr>{header}</tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>"""

    repo_url = "https://github.com/hurtadodaniel/mp/edit/master/data/gestiones.csv"
    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;max-width:900px;margin:auto">
      <div style="background:#c0392b;color:white;padding:16px 24px;border-radius:6px 6px 0 0">
        <h2 style="margin:0">\u26a0\ufe0f ALARMA &mdash; {len(nuevas)} OC Prioritaria(s) Nueva(s)</h2>
        <p style="margin:4px 0 0">{fecha_hoy} &middot; Acción requerida</p>
      </div>
      <div style="background:#fff9f9;border:2px solid #c0392b;padding:16px 24px">
        <h3 style="color:#c0392b;margin-top:0">OC Nuevas Detectadas</h3>
        {_tabla_alarmas(nuevas, highlight=True)}

        <h3 style="color:#555">Total Alarmas Activas: {len(activas)}</h3>
        {_tabla_alarmas(activas) if not activas.empty else "<p style='color:#888'>Sin alarmas activas.</p>"}

        <hr style="border:none;border-top:1px solid #ddd;margin:16px 0">
        <h4 style="color:#333">¿Cómo confirmar que gestionaste una OC?</h4>
        <ol style="font-size:13px;line-height:1.8">
          <li>Haz clic en este link: <a href="{repo_url}">Editar gestiones.csv en GitHub</a></li>
          <li>Haz clic en el ícono del lápiz (Edit this file)</li>
          <li>Al final del archivo agrega una nueva línea con este formato:<br>
              <code style="background:#f0f0f0;padding:2px 6px">{nuevas.iloc[0]["codigo_oc"]},Tu Nombre,{datetime.now().strftime("%Y-%m-%d")},gestionada</code>
          </li>
          <li>Haz clic en "Commit changes" y luego en "Commit changes" nuevamente</li>
          <li>El próximo run automático marcará la alarma como GESTIONADA</li>
        </ol>
        <p style="color:#aaa;font-size:11px">Generado automáticamente &middot; GitHub Actions &middot; Mercado Público</p>
      </div>
    </body></html>
    """

    plain = (
        f"ALARMA — {len(nuevas)} OC Prioritaria(s) Nueva(s) — {fecha_hoy}\n\n"
        + "\n".join(
            f"  {r['codigo_oc']} | {r['nombre_organismo']} | {r['categoria']} | cierre: {r['fecha_cierre']}"
            for _, r in nuevas.iterrows()
        )
        + f"\n\nTotal activas: {len(activas)}"
        + f"\n\nPara confirmar gestión edita: {repo_url}"
    )

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alt)

    print(f"  [ALARMAS] Enviando alerta a {recipients} …")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_bytes())
    print("  [ALARMAS] Alerta enviada correctamente.")


# ── PIPELINE PRINCIPAL ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch daily OC from Mercado Público (incremental)"
    )
    parser.add_argument(
        "--fecha", default=None,
        help="Fecha en formato DDMMAAAA (default: hoy)",
    )
    parser.add_argument(
        "--resumen", action="store_true",
        help="Solo envía digest de alarmas de los últimos 7 días (sin fetch de OC)",
    )
    args = parser.parse_args()

    # ── Modo resumen: sin fetch, solo digest de alarmas ───────────────────────
    if args.resumen:
        print("Modo RESUMEN — leyendo alarmas.csv y enviando digest...")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df_alarmas = cargar_alarmas_existentes()
        df_alarmas = aplicar_gestiones(df_alarmas)
        guardar_alarmas(df_alarmas)

        df_activas = df_alarmas[df_alarmas["estado_alarma"] == "ACTIVA"].copy()

        n_dias = 7
        cutoff = (datetime.now() - timedelta(days=n_dias)).isoformat()
        df_historial = df_alarmas[df_alarmas["fecha_detectada"] >= cutoff].copy() \
            if "fecha_detectada" in df_alarmas.columns and not df_alarmas.empty \
            else df_alarmas.copy()

        print(f"  Activas: {len(df_activas)} | Historial últimos {n_dias}d: {len(df_historial)}")
        enviar_email_resumen(df_activas, df_historial, n_dias)
        print("Resumen completado.")
        return

    fecha_extraccion = datetime.now(ZoneInfo("America/Santiago"))
    fecha_str = args.fecha or fecha_extraccion.strftime("%d%m%Y")
    fecha_consulta = fecha_extraccion.strftime("%Y-%m-%d")

    ticket_source = "env MP_TICKET" if os.getenv("MP_TICKET") else "hardcoded"
    print(f"Fecha de extracción: {fecha_extraccion.strftime('%d-%m-%Y')}")
    print(f"Parámetro API: fecha={fecha_str}")
    print(f"Ticket: {TICKET[:8]}...{TICKET[-4:]} (fuente: {ticket_source})")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PASO 1: Obtener listado de órdenes
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[1/5] Consultando listado de órdenes...")
    resultados = []
    errores_listado = []

    for proveedor in LISTA_PROVEEDORES:
        try:
            data = _fetch_listado(proveedor, fecha_str, TICKET, URL_LISTADO)
            cant = data.get("Cantidad", "N/A")
            print(f"  Proveedor {proveedor}: cantidad={cant}")
            if data.get("Listado"):
                df_t = pd.DataFrame(data["Listado"])
                df_t["CodigoProveedorConsultado"] = proveedor
                df_t["FechaConsulta"] = fecha_consulta
                resultados.append(df_t)
                print(f"  {len(df_t)} órdenes cargadas.")
            else:
                print(f"  Sin resultados para la fecha.")
                # Mostrar respuesta completa para diagnóstico
                print(f"  [DEBUG] Respuesta API: {data}")
        except Exception as e:
            print(f"  Error definitivo proveedor {proveedor}: {e}")
            errores_listado.append(proveedor)

    if errores_listado:
        print(f"\n  Proveedores fallidos tras {MAX_RETRIES_LISTADO} reintentos: "
              f"{errores_listado}")

    if not resultados:
        msg = "API en error" if errores_listado else "sin órdenes para la fecha"
        print(f"  No se cargaron órdenes ({msg}).")
        print("  Generando DataFrames vacíos con schema...")
        df_listado = _empty_listado()
        df_detalles = _empty_detalles()
        todos_codigos = []
        codigos_faltantes = []
    else:
        df_listado = pd.concat(resultados, ignore_index=True)
        todos_codigos = df_listado["Codigo"].astype(str).unique().tolist()
        print(f"  Total OC: {len(df_listado)} | Únicos: {len(todos_codigos)}")

        # ══════════════════════════════════════════════════════════════════════
        # PASO 2: Lógica incremental — leer CSV existente y filtrar pendientes
        # ══════════════════════════════════════════════════════════════════════
        print("\n[2/5] Verificando detalle existente (lógica incremental)...")
        already_done = _load_existing_detail_codes()
        df_existing_detail = _load_existing_detail_df()

        pendientes = [c for c in todos_codigos if c not in already_done]
        print(f"  Ya consultados: {len(already_done)} | Nuevos a consultar: {len(pendientes)}")

        if not pendientes:
            print("  Todos los códigos ya tienen detalle — sin llamadas a la API.")
            df_detalles = df_existing_detail
        else:
            # ── Fetch concurrente para pendientes ─────────────────────────────
            print(f"\n  Consultando detalle de {len(pendientes)} OC (concurrente, "
                  f"{MAX_WORKERS} hilos)...")

            _sem = Semaphore(MAX_WORKERS)

            def _fetch_detail(codigo):
                with _sem:
                    time.sleep(DELAY_POR_HILO + random.uniform(0, 0.5))
                    for intento in range(1, MAX_RETRIES + 1):
                        try:
                            r = requests.get(URL_DETALLE, params={
                                "codigo": codigo, "ticket": TICKET
                            }, verify=False, timeout=25)
                            if r.status_code in (500, 502, 503, 504):
                                if intento < MAX_RETRIES:
                                    time.sleep(_wait_backoff(intento))
                                    continue
                                return codigo, None
                            r.raise_for_status()
                            data = r.json()
                            if data.get("Listado"):
                                return codigo, data["Listado"][0]
                            return codigo, None
                        except Exception:
                            if intento < MAX_RETRIES:
                                time.sleep(_wait_backoff(intento))
                    return codigo, None

            detalles_ok = []
            detalles_fail = []
            total_pend = len(pendientes)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {pool.submit(_fetch_detail, c): c for c in pendientes}
                for i, fut in enumerate(as_completed(futures), 1):
                    codigo, resultado = fut.result()
                    if resultado:
                        detalles_ok.append(resultado)
                    else:
                        detalles_fail.append(codigo)
                    if i % 10 == 0 or i == total_pend:
                        print(f"    {i}/{total_pend} | OK={len(detalles_ok)} "
                              f"| Fail={len(detalles_fail)}")

            # ══════════════════════════════════════════════════════════════════
            # PASO 3: Reconciliación + retry secuencial de fallas
            # ══════════════════════════════════════════════════════════════════
            print("\n[3/5] Reconciliación y rescate de fallas...")

            if detalles_ok:
                df_new = pd.json_normalize(detalles_ok)
            else:
                df_new = _empty_detalles()

            # Retry secuencial para los que fallaron
            if detalles_fail:
                print(f"  {len(detalles_fail)} OC sin detalle — "
                      f"rescue secuencial (max {MAX_RETRIES_SEQ} reintentos)...")
                rescatados = []
                aun_faltantes = []

                for i, codigo in enumerate(detalles_fail, 1):
                    detalle = None
                    for intento in range(1, MAX_RETRIES_SEQ + 1):
                        try:
                            r = requests.get(URL_DETALLE, params={
                                "codigo": codigo, "ticket": TICKET
                            }, verify=False, timeout=30)
                            r.raise_for_status()
                            data = r.json()
                            if data.get("Listado"):
                                detalle = data["Listado"][0]
                                break
                        except Exception:
                            pass
                        wait = _wait_backoff(intento, max_wait=BACKOFF_MAX_SEQ)
                        print(f"    [{i}/{len(detalles_fail)}] {codigo} "
                              f"intento {intento}/{MAX_RETRIES_SEQ} — {wait:.0f}s...")
                        time.sleep(wait)

                    if detalle:
                        rescatados.append(detalle)
                    else:
                        aun_faltantes.append(codigo)

                if rescatados:
                    df_rescued = pd.json_normalize(rescatados)
                    df_new = pd.concat([df_new, df_rescued], ignore_index=True)

                detalles_fail = aun_faltantes

            # Merge con datos existentes
            if not df_existing_detail.empty and not df_new.empty:
                df_detalles = pd.concat(
                    [df_existing_detail, df_new], ignore_index=True
                ).drop_duplicates(subset="Codigo", keep="last")
            elif not df_new.empty:
                df_detalles = df_new
            else:
                df_detalles = df_existing_detail if not df_existing_detail.empty else _empty_detalles()

        # Cobertura
        codigos_con_detalle = set(df_detalles["Codigo"].astype(str).unique()) if not df_detalles.empty else set()
        codigos_faltantes = [c for c in todos_codigos if c not in codigos_con_detalle]
        cobertura = len(todos_codigos) - len(codigos_faltantes)

        if todos_codigos:
            pct = cobertura / len(todos_codigos) * 100
            print(f"\n  Cobertura: {cobertura}/{len(todos_codigos)} ({pct:.1f}%)")
        if codigos_faltantes:
            print(f"  Sin detalle definitivo: {codigos_faltantes}")
        else:
            print("  Todas las OC tienen detalle.")

    # ══════════════════════════════════════════════════════════════════════════
    # PASO 4: Clasificación + timestamp
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[4/5] Clasificando productos...")

    if not df_detalles.empty:
        # Solo clasificar filas que no tienen categoría aún
        mask_sin_cat = (
            df_detalles["CategoriaProducto"].isna()
            | (df_detalles["CategoriaProducto"] == "")
        ) if "CategoriaProducto" in df_detalles.columns else pd.Series(True, index=df_detalles.index)

        if mask_sin_cat.any():
            df_detalles.loc[mask_sin_cat, "CategoriaProducto"] = (
                df_detalles.loc[mask_sin_cat].apply(classify_row, axis=1)
            )
            df_detalles.loc[mask_sin_cat, "FechaProcesamiento"] = (
                datetime.now().isoformat()
            )

        total = len(df_detalles)
        clasificados = (df_detalles["CategoriaProducto"] != "SIN CLASIFICAR").sum()
        print(f"  Clasificados: {clasificados}/{total} "
              f"({clasificados / total * 100:.1f}%)")
        print(df_detalles["CategoriaProducto"].value_counts().to_string())
    else:
        if "CategoriaProducto" not in df_detalles.columns:
            df_detalles["CategoriaProducto"] = pd.Series(dtype=str)
        if "FechaProcesamiento" not in df_detalles.columns:
            df_detalles["FechaProcesamiento"] = pd.Series(dtype=str)
        print("  df_detalles vacío.")

    # Merge consolidado
    df_merged = pd.merge(
        df_listado, df_detalles, on="Codigo", how="left",
        suffixes=("_base", "_detalle"),
    )

    # ══════════════════════════════════════════════════════════════════════════
    # PASO 5: Guardar CSVs incrementales
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[5/5] Guardando CSVs...")

    # Listado: append incremental
    listado_exists = CSV_LISTADO.exists()
    df_listado.to_csv(
        CSV_LISTADO, mode="a", header=not listado_exists,
        index=False, encoding="utf-8",
    )
    print(f"  {CSV_LISTADO}: +{len(df_listado)} filas "
          f"({'append' if listado_exists else 'nuevo'})")

    # Detalle: reescribir completo (es el consolidado incremental con dedup)
    df_detalles.to_csv(CSV_DETALLE, index=False, encoding="utf-8")
    print(f"  {CSV_DETALLE}: {len(df_detalles)} filas totales (consolidado dedup)")

    # Consolidado: acumulativo con dedup por Codigo (keep=last para actualizar)
    if CSV_CONSOLIDADO.exists():
        df_consol_existing = pd.read_csv(CSV_CONSOLIDADO, dtype=str)
        df_consol = pd.concat([df_consol_existing, df_merged.astype(str)], ignore_index=True)
        df_consol = df_consol.drop_duplicates(subset="Codigo", keep="last")
        nuevas_consol = len(df_consol) - len(df_consol_existing.drop_duplicates(subset="Codigo"))
    else:
        df_consol = df_merged.astype(str)
        nuevas_consol = len(df_consol)
    df_consol.to_csv(CSV_CONSOLIDADO, index=False, encoding="utf-8")
    print(f"  {CSV_CONSOLIDADO}: {len(df_consol)} filas totales (+{nuevas_consol} nuevas)")

    if codigos_faltantes:
        csv_faltantes = DATA_DIR / "ordenes_sin_detalle.csv"
        pd.DataFrame({"Codigo_sin_detalle": codigos_faltantes}).to_csv(
            csv_faltantes, index=False,
        )
        print(f"  {csv_faltantes}: {len(codigos_faltantes)} códigos sin detalle")

    # Resumen
    print("\n── Resumen ─────────────────────────────────────────────────────")
    print(f"  Listado:  {len(df_listado)} OC del día")
    print(f"  Detalle:  {len(df_detalles)} OC con detalle (acumulado)")
    print(f"  Consolidado: {len(df_consol)} filas acumuladas (+{nuevas_consol} nuevas hoy)")

    # Email de OC: solo si este run detectó OC nuevas (evita spam en runs vacíos)
    if nuevas_consol > 0:
        if "FechaConsulta" in df_consol.columns:
            rows_del_dia = df_consol[
                df_consol["FechaConsulta"] == fecha_consulta
            ].to_dict("records")
        else:
            rows_del_dia = df_consol.to_dict("records")
        send_email(rows_del_dia, fecha_consulta)
    else:
        print("  Sin OC nuevas en este run — email de OC omitido.")

    # ══════════════════════════════════════════════════════════════════════════
    # ALARMAS: clientes prioritarios
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[ALARMAS] Verificando clientes prioritarios...")
    prefijos_prioritarios, plazos_dict = cargar_clientes_prioritarios()
    if prefijos_prioritarios:
        df_alarmas = cargar_alarmas_existentes()
        df_alarmas = aplicar_gestiones(df_alarmas)
        nuevas_alarmas = detectar_nuevas_alarmas(df_consol, prefijos_prioritarios, df_alarmas, plazos_dict)
        if not nuevas_alarmas.empty:
            df_alarmas = pd.concat([df_alarmas, nuevas_alarmas], ignore_index=True)
        guardar_alarmas(df_alarmas)
        df_activas = df_alarmas[df_alarmas["estado_alarma"] == "ACTIVA"].copy()
        print(f"  [ALARMAS] Activas totales: {len(df_activas)} | Nuevas: {len(nuevas_alarmas)}")
        enviar_email_alarmas(nuevas_alarmas, df_activas)
    else:
        print("  [ALARMAS] Sin clientes prioritarios — sin verificación.")

    ahora = datetime.now()
    print(f"\nPipeline completado — {ahora.strftime('%d/%m/%Y %H:%M:%S')}")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"Error HTTP {e.response.status_code}: {e.response.text}",
              file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Error de red: {e}", file=sys.stderr)
        sys.exit(1)
