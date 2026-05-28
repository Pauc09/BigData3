"""
ShopStream — Generador de datos sintéticos
Punto 1 del parcial: genera ≥500k eventos diarios y los sube a S3

Uso:
    python generate_data.py --date 2026-05-20 --records 500000
    python generate_data.py --date 2026-05-20 --records 500000 --bucket shopstream-datalake-XXXX
"""

import argparse
import gzip
import io
import json
import random
import uuid
from datetime import datetime, timedelta

import boto3
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

# ──────────────────────────────────────────────
# CONFIGURACIÓN GENERAL
# ──────────────────────────────────────────────

BUCKET_NAME = "shopstream-datalake-7489"  # cambia esto o pásalo por argumento

# Catálogo de productos (100 productos ficticios)
CATEGORIES = ["electronics", "clothing", "home", "sports", "beauty", "books", "toys"]
PRODUCTS = [
    {
        "product_id": f"PROD-{i:04d}",
        "name": fake.bs(),
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(5.99, 499.99), 2),
    }
    for i in range(1, 101)
]

# Catálogo de usuarios (10k usuarios ficticios)
COUNTRIES = ["CO", "MX", "AR", "US", "BR", "ES", "PE", "CL"]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]  # mobile domina

USERS = [
    {
        "user_id": f"USR-{i:05d}",
        "country": random.choices(COUNTRIES, weights=[20, 18, 12, 15, 14, 8, 7, 6])[0],
        "device_type": random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
    }
    for i in range(1, 10001)
]

PAGE_TYPES = ["home", "category", "product", "cart", "checkout", "search", "other"]
PAGE_URLS = {
    "home": ["/", "/home"],
    "category": [f"/category/{c}" for c in CATEGORIES],
    "product": [f"/product/{p['product_id']}" for p in PRODUCTS],
    "cart": ["/cart", "/cart/view"],
    "checkout": ["/checkout", "/checkout/payment", "/checkout/confirm"],
    "search": ["/search"],
    "other": ["/about", "/contact", "/help", "/faq"],
}

ELEMENT_TYPES = ["button", "link", "image", "add_to_cart", "banner", "filter"]
REFERRERS = ["google", "facebook", "instagram", "direct", "email", "none"]


# ──────────────────────────────────────────────
# GENERADORES DE EVENTOS
# ──────────────────────────────────────────────

def make_timestamp(date_str: str) -> str:
    """Genera un timestamp aleatorio dentro del día indicado."""
    base = datetime.strptime(date_str, "%Y-%m-%d")
    seconds = random.randint(0, 86399)
    return (base + timedelta(seconds=seconds)).isoformat() + "Z"


def gen_page_view(user: dict, session_id: str, date_str: str) -> dict:
    page_type = random.choices(
        PAGE_TYPES,
        weights=[15, 25, 35, 10, 5, 8, 2]
    )[0]
    url = random.choice(PAGE_URLS[page_type])
    return {
        "event_type": "page_view",
        "user_id": user["user_id"],
        "session_id": session_id,
        "page_url": url,
        "page_type": page_type,
        "timestamp": make_timestamp(date_str),
        "time_on_page_seconds": int(random.expovariate(1 / 90)),  # media ~90s
        "referrer": random.choices(REFERRERS, weights=[35, 20, 15, 20, 8, 2])[0],
        "device_type": user["device_type"],
        "country": user["country"],
    }


def gen_click(user: dict, session_id: str, page_url: str, date_str: str) -> dict:
    return {
        "event_type": "click",
        "user_id": user["user_id"],
        "session_id": session_id,
        "element_id": f"el-{uuid.uuid4().hex[:8]}",
        "element_type": random.choice(ELEMENT_TYPES),
        "page_url": page_url,
        "timestamp": make_timestamp(date_str),
        "x_position": random.randint(0, 1920),
        "y_position": random.randint(0, 1080),
    }


def gen_search(user: dict, session_id: str, date_str: str) -> dict:
    return {
        "event_type": "search",
        "user_id": user["user_id"],
        "session_id": session_id,
        "query": fake.bs(),
        "results_count": random.randint(0, 150),
        "timestamp": make_timestamp(date_str),
    }


def gen_product_view(user: dict, session_id: str, date_str: str) -> dict:
    product = random.choice(PRODUCTS)
    return {
        "event_type": "product_view",
        "user_id": user["user_id"],
        "session_id": session_id,
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "timestamp": make_timestamp(date_str),
        "time_on_page_seconds": int(random.expovariate(1 / 120)),
    }


def gen_cart_event(user: dict, session_id: str, date_str: str) -> dict:
    product = random.choice(PRODUCTS)
    return {
        "event_type": "cart_event",
        "user_id": user["user_id"],
        "session_id": session_id,
        "product_id": product["product_id"],
        "action": random.choices(["add", "remove"], weights=[75, 25])[0],
        "timestamp": make_timestamp(date_str),
    }


# 
# GENERADOR DE SESIONES REALISTAS
# 

def generate_session(date_str: str) -> list:
    """
    Genera una sesión completa con un flujo realista de eventos.
    Una sesión = 1 usuario, varios eventos relacionados.
    """
    user = random.choice(USERS)
    session_id = f"SES-{uuid.uuid4().hex[:12]}"
    events = []

    # Toda sesión empieza con al menos 1 page_view
    num_pageviews = random.choices(
        [1, 2, 3, 4, 5, 6, 7, 8],
        weights=[30, 20, 15, 12, 10, 7, 4, 2]
    )[0]

    last_url = "/"
    for _ in range(num_pageviews):
        pv = gen_page_view(user, session_id, date_str)
        events.append(pv)
        last_url = pv["page_url"]

        # 60% de chance de hacer clic en la página
        if random.random() < 0.6:
            events.append(gen_click(user, session_id, last_url, date_str))

    # 30% hacen búsqueda
    if random.random() < 0.30:
        events.append(gen_search(user, session_id, date_str))

    # 45% ven un producto
    if random.random() < 0.45:
        events.append(gen_product_view(user, session_id, date_str))

        # 25% de los que ven producto agregan al carrito
        if random.random() < 0.25:
            events.append(gen_cart_event(user, session_id, date_str))

    return events


# 
# GENERACIÓN MASIVA Y UPLOAD A S3
# 

def generate_events(date_str: str, target_records: int) -> list:
    """Genera eventos hasta alcanzar el número objetivo."""
    all_events = []
    print(f" Generando eventos para {date_str}...")

    while len(all_events) < target_records:
        session_events = generate_session(date_str)
        all_events.extend(session_events)

        if len(all_events) % 50000 == 0:
            print(f"   {len(all_events):,} eventos generados...")

    print(f" Total generado: {len(all_events):,} eventos")
    return all_events


def upload_to_s3(events: list, date_str: str, bucket: str):
    """
    Sube los eventos a S3 particionados por tipo de evento y fecha.
    Estructura: s3://bucket/raw/event_type=X/year=YYYY/month=MM/day=DD/events.json.gz
    """
    s3 = boto3.client("s3")
    date = datetime.strptime(date_str, "%Y-%m-%d")

    # Agrupar por tipo de evento
    by_type = {}
    for event in events:
        etype = event["event_type"]
        if etype not in by_type:
            by_type[etype] = []
        by_type[etype].append(event)

    for event_type, type_events in by_type.items():
        # Convertir a JSONL (una línea por evento) y comprimir
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="w") as gz:
            for ev in type_events:
                gz.write((json.dumps(ev) + "\n").encode("utf-8"))
        buffer.seek(0)

        # Clave S3 con particionado tipo Hive
        s3_key = (
            f"raw/event_type={event_type}/"
            f"year={date.year}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}/"
            f"events.json.gz"
        )

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/gzip",
        )
        print(f"    s3://{bucket}/{s3_key}  ({len(type_events):,} eventos)")

    # También subir catálogos de referencia (productos y usuarios)
    upload_catalogs(s3, bucket)


def upload_catalogs(s3_client, bucket: str):
    """Sube los catálogos de productos y usuarios como referencia."""
    for name, data in [("products", PRODUCTS), ("users", USERS)]:
        body = "\n".join(json.dumps(r) for r in data).encode("utf-8")
        key = f"reference/{name}/catalog.json"
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)
        print(f"    Catálogo subido: s3://{bucket}/{key}")


# MAIN


def main():
    parser = argparse.ArgumentParser(description="ShopStream data generator")
    parser.add_argument(
        "--date",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Fecha a generar (YYYY-MM-DD). Default: hoy UTC",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=500000,
        help="Número mínimo de eventos a generar. Default: 500000",
    )
    parser.add_argument(
        "--bucket",
        default=BUCKET_NAME,
        help="Nombre del bucket S3 destino",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Solo genera los datos localmente sin subir a S3",
    )
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"  ShopStream Data Generator")
    print(f"  Fecha:    {args.date}")
    print(f"  Registros:{args.records:,}")
    print(f"  Bucket:   {args.bucket}")
    print(f"{'='*50}\n")

    events = generate_events(args.date, args.records)

    if args.local:
        # Guardar localmente para pruebas
        with open(f"events_{args.date}.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")
        print(f"\n Guardado localmente: events_{args.date}.jsonl")
    else:
        print(f"\n Subiendo a S3...")
        upload_to_s3(events, args.date, args.bucket)

    print(f"\n Listo! {len(events):,} eventos procesados.")


if __name__ == "__main__":
    main()