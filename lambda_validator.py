"""
ShopStream — Lambda de validación de esquema
Punto 2 del parcial

Se activa con eventos S3 PutObject.
- Valida esquema de cada evento (tipos, campos obligatorios, rangos)
- Mueve archivos inválidos a quarantine/ con metadata del error
- Registra métricas en CloudWatch
"""

import gzip
import io
import json
import os
import urllib.parse
from datetime import datetime

import boto3

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

NAMESPACE = "ShopStream/Ingestion"

# ──────────────────────────────────────────────
# ESQUEMAS POR TIPO DE EVENTO
# ──────────────────────────────────────────────

SCHEMAS = {
    "page_view": {
        "required": [
            "event_type", "user_id", "session_id", "page_url",
            "page_type", "timestamp", "time_on_page_seconds",
            "referrer", "device_type", "country"
        ],
        "types": {
            "time_on_page_seconds": (int, float),
        },
        "ranges": {
            "time_on_page_seconds": (0, 86400),
        },
        "enums": {
            "page_type": ["home", "category", "product", "cart", "checkout", "search", "other"],
            "device_type": ["mobile", "desktop", "tablet"],
        },
    },
    "click": {
        "required": [
            "event_type", "user_id", "session_id", "element_id",
            "element_type", "page_url", "timestamp", "x_position", "y_position"
        ],
        "types": {
            "x_position": (int, float),
            "y_position": (int, float),
        },
        "ranges": {
            "x_position": (0, 7680),
            "y_position": (0, 4320),
        },
        "enums": {},
    },
    "search": {
        "required": [
            "event_type", "user_id", "session_id",
            "query", "results_count", "timestamp"
        ],
        "types": {
            "results_count": (int, float),
        },
        "ranges": {
            "results_count": (0, 10000),
        },
        "enums": {},
    },
    "product_view": {
        "required": [
            "event_type", "user_id", "session_id", "product_id",
            "category", "price", "timestamp", "time_on_page_seconds"
        ],
        "types": {
            "price": (int, float),
            "time_on_page_seconds": (int, float),
        },
        "ranges": {
            "price": (0, 100000),
            "time_on_page_seconds": (0, 86400),
        },
        "enums": {},
    },
    "cart_event": {
        "required": [
            "event_type", "user_id", "session_id",
            "product_id", "action", "timestamp"
        ],
        "types": {},
        "ranges": {},
        "enums": {
            "action": ["add", "remove"],
        },
    },
}


# ──────────────────────────────────────────────
# VALIDACIÓN DE UN EVENTO
# ──────────────────────────────────────────────

def validate_event(event: dict) -> list:
    """
    Valida un evento contra su esquema.
    Retorna lista de errores (vacía = válido).
    """
    errors = []

    # 1. Verificar que tenga event_type
    event_type = event.get("event_type")
    if not event_type:
        return ["missing field: event_type"]

    if event_type not in SCHEMAS:
        return [f"unknown event_type: {event_type}"]

    schema = SCHEMAS[event_type]

    # 2. Campos obligatorios
    for field in schema["required"]:
        if field not in event or event[field] is None or event[field] == "":
            errors.append(f"missing or empty field: {field}")

    # 3. Tipos de datos
    for field, expected_type in schema["types"].items():
        if field in event and event[field] is not None:
            if not isinstance(event[field], expected_type):
                errors.append(f"wrong type for {field}: expected {expected_type}, got {type(event[field])}")

    # 4. Rangos numéricos
    for field, (min_val, max_val) in schema["ranges"].items():
        if field in event and event[field] is not None:
            try:
                val = float(event[field])
                if not (min_val <= val <= max_val):
                    errors.append(f"out of range {field}: {val} not in [{min_val}, {max_val}]")
            except (ValueError, TypeError):
                pass  # ya capturado en tipos

    # 5. Valores permitidos (enums)
    for field, allowed in schema["enums"].items():
        if field in event and event[field] not in allowed:
            errors.append(f"invalid value for {field}: '{event[field]}' not in {allowed}")

    # 6. Formato de timestamp (ISO 8601 básico)
    ts = event.get("timestamp", "")
    if ts:
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"invalid timestamp format: {ts}")

    return errors


# ──────────────────────────────────────────────
# MÉTRICAS EN CLOUDWATCH
# ──────────────────────────────────────────────

def put_metric(name: str, value: float, unit: str = "Count", dimensions: list = None):
    """Envía una métrica a CloudWatch."""
    metric = {
        "MetricName": name,
        "Value": value,
        "Unit": unit,
        "Timestamp": datetime.utcnow(),
    }
    if dimensions:
        metric["Dimensions"] = dimensions

    try:
        cloudwatch.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[metric]
        )
    except Exception as e:
        print(f"[WARN] No se pudo enviar métrica {name}: {e}")


# ──────────────────────────────────────────────
# HANDLER PRINCIPAL
# ──────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Entry point de la Lambda.
    Recibe evento S3 PutObject, valida el archivo, mueve inválidos a quarantine/.
    """
    results = []

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size_bytes = record["s3"]["object"].get("size", 0)

        print(f"[INFO] Procesando: s3://{bucket}/{key} ({size_bytes} bytes)")

        # Ignorar archivos que ya están en quarantine o reference
        if key.startswith("quarantine/") or key.startswith("reference/"):
            print(f"[INFO] Ignorando archivo en {key.split('/')[0]}/")
            continue

        try:
            result = process_file(bucket, key, size_bytes)
            results.append(result)
        except Exception as e:
            print(f"[ERROR] Fallo procesando {key}: {e}")
            put_metric("ProcessingErrors", 1, dimensions=[
                {"Name": "S3Key", "Value": key[:256]}
            ])

    return {"statusCode": 200, "body": json.dumps(results)}


def process_file(bucket: str, key: str, size_bytes: int) -> dict:
    """Descarga, valida y decide qué hacer con un archivo."""

    # Descargar archivo
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()

    # Descomprimir si es gzip
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
            content = gz.read().decode("utf-8")
    else:
        content = body.decode("utf-8")

    # Parsear líneas JSONL
    lines = [l.strip() for l in content.splitlines() if l.strip()]
    total = len(lines)
    valid_events = []
    invalid_events = []

    for i, line in enumerate(lines):
        try:
            ev = json.loads(line)
            errors = validate_event(ev)
            if errors:
                invalid_events.append({"line": i + 1, "event": ev, "errors": errors})
            else:
                valid_events.append(ev)
        except json.JSONDecodeError as e:
            invalid_events.append({
                "line": i + 1,
                "raw": line[:200],
                "errors": [f"invalid JSON: {str(e)}"]
            })

    valid_count = len(valid_events)
    invalid_count = len(invalid_events)
    error_rate = (invalid_count / total * 100) if total > 0 else 0

    print(f"[INFO] Total: {total} | Válidos: {valid_count} | Inválidos: {invalid_count} ({error_rate:.1f}%)")

    # ── Métricas CloudWatch ──
    event_type = _extract_event_type(key)
    dims = [{"Name": "EventType", "Value": event_type}]

    put_metric("FilesProcessed", 1, dimensions=dims)
    put_metric("RecordsValid", valid_count, dimensions=dims)
    put_metric("RecordsInvalid", invalid_count, dimensions=dims)
    put_metric("FileSizeBytes", size_bytes, unit="Bytes", dimensions=dims)

    # ── Si hay inválidos, mover a quarantine ──
    if invalid_events:
        quarantine_key = _build_quarantine_key(key)
        quarantine_payload = {
            "original_key": key,
            "processed_at": datetime.utcnow().isoformat() + "Z",
            "total_records": total,
            "invalid_count": invalid_count,
            "error_rate_pct": round(error_rate, 2),
            "invalid_events": invalid_events[:100],  # máximo 100 para no inflar
        }
        s3.put_object(
            Bucket=bucket,
            Key=quarantine_key,
            Body=json.dumps(quarantine_payload, indent=2).encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "original-key": key,
                "invalid-count": str(invalid_count),
                "error-rate": str(round(error_rate, 2)),
            }
        )
        print(f"[WARN] Quarantine: s3://{bucket}/{quarantine_key}")

    return {
        "key": key,
        "total": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "error_rate_pct": round(error_rate, 2),
    }


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def _extract_event_type(key: str) -> str:
    """Extrae el event_type del path de S3. Ej: raw/event_type=page_view/... → page_view"""
    for part in key.split("/"):
        if part.startswith("event_type="):
            return part.split("=")[1]
    return "unknown"


def _build_quarantine_key(original_key: str) -> str:
    """
    Construye la clave de quarantine preservando la estructura del path.
    Ej: raw/event_type=click/.../events.json.gz
     →  quarantine/event_type=click/.../events_errors.json
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = original_key.replace("raw/", "").replace(".json.gz", "").replace(".json", "")
    return f"quarantine/{base}_errors_{timestamp}.json"