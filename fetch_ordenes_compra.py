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
import os
import random
import smtplib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────

TICKET = os.getenv("MP_TICKET", "F8537A18-6766-4DEF-9E59-426B4FEE2844")
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

EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

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
                print(f"    HTTP {r.status_code} — intento {intento}/{max_retries}, "
                      f"reintentando en {wait:.0f}s...")
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


def send_email(new_rows: list[dict], fecha_consulta: str) -> None:
    if not all([EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO]):
        print("  Correo no configurado — saltando envío de email.")
        return

    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]
    subject = (
        f"OC Mercado Público · {fecha_consulta} · "
        f"{len(new_rows)} orden(es) nuevas"
    )

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)

    plain = (
        f"Órdenes de Compra — {fecha_consulta}\n"
        f"Órdenes nuevas procesadas: {len(new_rows)}\n\n"
        "Ver adjunto para el detalle completo."
    )
    html = f"""
    <html><body>
      <h2 style="color:#1a56db">Mercado Público — Órdenes de Compra</h2>
      <p><b>Fecha:</b> {fecha_consulta} &nbsp;|&nbsp;
         <b>Nuevas:</b> {len(new_rows)} orden(es)</p>
      {_build_html_table(new_rows)}
      <p style="color:#aaa;font-size:11px">
        Generado automáticamente · GitHub Actions
      </p>
    </body></html>
    """

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alt)

    # CSV attachment with today's new records
    buf = io.StringIO()
    if new_rows:
        writer = csv.DictWriter(buf, fieldnames=list(new_rows[0].keys()),
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(new_rows)
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


# ── PIPELINE PRINCIPAL ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch daily OC from Mercado Público (incremental)"
    )
    parser.add_argument(
        "--fecha", default=None,
        help="Fecha en formato DDMMAAAA (default: hoy)",
    )
    args = parser.parse_args()

    fecha_extraccion = datetime.now()
    fecha_str = args.fecha or fecha_extraccion.strftime("%d%m%Y")
    fecha_consulta = fecha_extraccion.strftime("%Y-%m-%d")

    print(f"Fecha de extracción: {fecha_extraccion.strftime('%d-%m-%Y')}")
    print(f"Parámetro API: fecha={fecha_str}")

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
                df_t["FechaConsulta"] = fecha_str
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

    # Consolidado merge: reescribir completo
    df_merged.to_csv(CSV_CONSOLIDADO, index=False, encoding="utf-8")
    print(f"  {CSV_CONSOLIDADO}: {len(df_merged)} filas")

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
    print(f"  Merge:    {len(df_merged)} filas consolidadas")

    # Email (opcional)
    if not df_detalles.empty:
        new_rows = df_detalles[
            df_detalles["Codigo"].astype(str).isin(todos_codigos)
        ].to_dict("records") if todos_codigos else []
        send_email(new_rows, fecha_consulta)

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
