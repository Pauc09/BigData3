"""
ShopStream — API REST
Punto 5 del parcial

Endpoints:
  GET /pages/top?metric={bounce_rate|time_on_page}&date={date}&limit={n}
  GET /sessions/summary?country={country}&device={device}&date={date}
  GET /anomalies?date={date}

Uso local:
  python app.py

Deploy con Zappa:
  zappa deploy dev
"""

import os
from datetime import datetime

from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text

app = Flask(__name__)
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "ShopStream API"})

# ──────────────────────────────────────────────
# CONFIGURACIÓN DE BASE DE DATOS
# 
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "shopstream")
DB_USER = os.environ.get("DB_USER", "shopstream")
DB_PASS = os.environ.get("DB_PASS", "ShopStream2026!")
DB_PORT = os.environ.get("DB_PORT", "5432")
USE_SQLITE = os.environ.get("USE_SQLITE", "true").lower() == "true"

if USE_SQLITE:
    # Desarrollo local con SQLite
    engine = create_engine("sqlite:///shopstream_dev.db", echo=False)
    print("[INFO] Usando SQLite local para desarrollo")
else:
    # Producción con RDS Postgres
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str, echo=False)
    print(f"[INFO] Conectado a RDS: {DB_HOST}")


# ──────────────────────────────────────────────
# SETUP DE TABLAS (solo para SQLite dev)
# ──────────────────────────────────────────────

def init_dev_db():
    """Crea tablas de ejemplo en SQLite para poder probar la API sin RDS."""
    with engine.connect() as conn:
        # Tabla: métricas de páginas
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS page_metrics (
                date        TEXT,
                page_url    TEXT,
                page_type   TEXT,
                avg_time_on_page REAL,
                bounce_rate REAL,
                total_sessions INTEGER,
                PRIMARY KEY (date, page_url)
            )
        """))

        # Tabla: resumen de sesiones
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS session_summary (
                date            TEXT,
                country         TEXT,
                device_type     TEXT,
                total_sessions  INTEGER,
                avg_session_duration REAL,
                total_pageviews INTEGER,
                unique_users    INTEGER,
                PRIMARY KEY (date, country, device_type)
            )
        """))

        # Tabla: anomalías
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS anomalies (
                date            TEXT,
                session_id      TEXT,
                user_id         TEXT,
                anomaly_type    TEXT,
                z_score         REAL,
                metric_value    REAL,
                description     TEXT,
                PRIMARY KEY (date, session_id, anomaly_type)
            )
        """))

        # Datos de ejemplo para poder probar
        conn.execute(text("""
            INSERT OR IGNORE INTO page_metrics VALUES
            ('2026-05-20', '/home', 'home', 45.2, 0.32, 1200),
            ('2026-05-20', '/category/electronics', 'category', 67.8, 0.21, 980),
            ('2026-05-20', '/product/PROD-0001', 'product', 120.5, 0.15, 750),
            ('2026-05-20', '/cart', 'cart', 89.3, 0.45, 430),
            ('2026-05-20', '/checkout', 'checkout', 210.1, 0.60, 200),
            ('2026-05-20', '/category/clothing', 'category', 55.4, 0.28, 870),
            ('2026-05-20', '/product/PROD-0002', 'product', 95.2, 0.18, 620),
            ('2026-05-20', '/search', 'search', 38.7, 0.41, 540),
            ('2026-05-20', '/product/PROD-0003', 'product', 145.0, 0.12, 390),
            ('2026-05-20', '/category/sports', 'category', 72.1, 0.25, 310)
        """))

        conn.execute(text("""
            INSERT OR IGNORE INTO session_summary VALUES
            ('2026-05-20', 'CO', 'mobile', 3200, 4.5, 9800, 2100),
            ('2026-05-20', 'CO', 'desktop', 1800, 7.2, 7200, 1400),
            ('2026-05-20', 'MX', 'mobile', 2900, 3.8, 8700, 1900),
            ('2026-05-20', 'MX', 'desktop', 1500, 6.9, 5800, 1200),
            ('2026-05-20', 'US', 'mobile', 2100, 5.1, 6300, 1700),
            ('2026-05-20', 'US', 'desktop', 2400, 8.3, 9600, 2000),
            ('2026-05-20', 'AR', 'tablet', 800, 6.0, 2400, 650),
            ('2026-05-20', 'BR', 'mobile', 1900, 4.2, 5700, 1500),
            ('2026-05-20', 'CO', 'tablet', 450, 5.5, 1350, 380),
            ('2026-05-20', 'ES', 'desktop', 1100, 7.8, 4400, 900)
        """))

        conn.execute(text("""
            INSERT OR IGNORE INTO anomalies VALUES
            ('2026-05-20', 'SES-abc001', 'USR-00123', 'high_time_on_page', 4.2, 8200.0, 'Tiempo en página extremadamente alto'),
            ('2026-05-20', 'SES-abc002', 'USR-00456', 'rapid_clicks', 3.8, 145.0, 'Número de clics por minuto anómalo'),
            ('2026-05-20', 'SES-abc003', 'USR-00789', 'high_time_on_page', 5.1, 12400.0, 'Posible bot o sesión inactiva'),
            ('2026-05-20', 'SES-abc004', 'USR-01234', 'unusual_navigation', 3.2, 89.0, 'Patrón de navegación inusual'),
            ('2026-05-20', 'SES-abc005', 'USR-01567', 'rapid_clicks', 4.7, 210.0, 'Actividad de clic automatizada detectada')
        """))

        conn.commit()
    print("[INFO] Base de datos de desarrollo inicializada con datos de ejemplo")


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def validate_date(date_str: str) -> bool:
    """Valida que el string tenga formato YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def error_response(message: str, code: int = 400):
    return jsonify({"error": message, "status": code}), code


# ──────────────────────────────────────────────
# ENDPOINT 1 — Top páginas
# GET /pages/top?metric={bounce_rate|time_on_page}&date={date}&limit={n}
# ──────────────────────────────────────────────

@app.route("/pages/top", methods=["GET"])
def pages_top():
    """
    Retorna las páginas con mayor tasa de rebote o tiempo de permanencia.

    Params:
      metric : bounce_rate | time_on_page  (requerido)
      date   : YYYY-MM-DD                  (requerido)
      limit  : int 1-100                   (opcional, default 10)
    """
    metric = request.args.get("metric")
    date   = request.args.get("date")
    limit  = request.args.get("limit", 10)

    # Validaciones
    if not metric:
        return error_response("Parámetro 'metric' requerido: bounce_rate | time_on_page")
    if metric not in ("bounce_rate", "time_on_page"):
        return error_response("'metric' debe ser 'bounce_rate' o 'time_on_page'")
    if not date:
        return error_response("Parámetro 'date' requerido (YYYY-MM-DD)")
    if not validate_date(date):
        return error_response("Formato de 'date' inválido. Use YYYY-MM-DD")

    try:
        limit = int(limit)
        if not (1 <= limit <= 100):
            raise ValueError
    except (ValueError, TypeError):
        return error_response("'limit' debe ser un entero entre 1 y 100")

    # Columna a ordenar según métrica
    order_col = "bounce_rate" if metric == "bounce_rate" else "avg_time_on_page"

    query = text(f"""
        SELECT
            page_url,
            page_type,
            avg_time_on_page,
            bounce_rate,
            total_sessions
        FROM page_metrics
        WHERE date = :date
        ORDER BY {order_col} DESC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"date": date, "limit": limit}).fetchall()

    if not rows:
        return jsonify({
            "date": date,
            "metric": metric,
            "results": [],
            "message": f"No hay datos para {date}"
        })

    results = [
        {
            "rank": i + 1,
            "page_url": row[0],
            "page_type": row[1],
            "avg_time_on_page_seconds": round(row[2], 2),
            "bounce_rate": round(row[3], 4),
            "bounce_rate_pct": f"{row[3]*100:.1f}%",
            "total_sessions": row[4],
        }
        for i, row in enumerate(rows)
    ]

    return jsonify({
        "date": date,
        "metric": metric,
        "limit": limit,
        "count": len(results),
        "results": results,
    })


# ──────────────────────────────────────────────
# ENDPOINT 2 — Resumen de sesiones
# GET /sessions/summary?country={country}&device={device}&date={date}
# ──────────────────────────────────────────────

@app.route("/sessions/summary", methods=["GET"])
def sessions_summary():
    """
    Resumen de sesiones filtrado por país, dispositivo y fecha.

    Params:
      date    : YYYY-MM-DD  (requerido)
      country : código ISO  (opcional, ej: CO, MX, US)
      device  : mobile | desktop | tablet  (opcional)
    """
    date    = request.args.get("date")
    country = request.args.get("country")
    device  = request.args.get("device")

    if not date:
        return error_response("Parámetro 'date' requerido (YYYY-MM-DD)")
    if not validate_date(date):
        return error_response("Formato de 'date' inválido. Use YYYY-MM-DD")
    if device and device not in ("mobile", "desktop", "tablet"):
        return error_response("'device' debe ser mobile, desktop o tablet")

    # Construir query dinámicamente según filtros
    filters = ["date = :date"]
    params  = {"date": date}

    if country:
        filters.append("country = :country")
        params["country"] = country.upper()

    if device:
        filters.append("device_type = :device")
        params["device"] = device

    where = " AND ".join(filters)

    query = text(f"""
        SELECT
            country,
            device_type,
            SUM(total_sessions)         AS total_sessions,
            AVG(avg_session_duration)   AS avg_duration,
            SUM(total_pageviews)        AS total_pageviews,
            SUM(unique_users)           AS unique_users
        FROM session_summary
        WHERE {where}
        GROUP BY country, device_type
        ORDER BY total_sessions DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return jsonify({
            "date": date,
            "filters": {"country": country, "device": device},
            "results": [],
            "message": f"No hay datos para los filtros aplicados"
        })

    results = [
        {
            "country": row[0],
            "device_type": row[1],
            "total_sessions": int(row[2]),
            "avg_session_duration_minutes": round(row[3], 2),
            "total_pageviews": int(row[4]),
            "unique_users": int(row[5]),
            "pageviews_per_session": round(row[4] / row[2], 2) if row[2] else 0,
        }
        for row in rows
    ]

    # Totales agregados
    totals = {
        "total_sessions": sum(r["total_sessions"] for r in results),
        "total_pageviews": sum(r["total_pageviews"] for r in results),
        "unique_users": sum(r["unique_users"] for r in results),
    }

    return jsonify({
        "date": date,
        "filters": {"country": country, "device": device},
        "count": len(results),
        "totals": totals,
        "results": results,
    })


# ──────────────────────────────────────────────
# ENDPOINT 3 — Anomalías
# GET /anomalies?date={date}
# ──────────────────────────────────────────────

@app.route("/anomalies", methods=["GET"])
def anomalies():
    """
    Lista de sesiones anómalas detectadas en una fecha.

    Params:
      date : YYYY-MM-DD  (requerido)
    """
    date = request.args.get("date")

    if not date:
        return error_response("Parámetro 'date' requerido (YYYY-MM-DD)")
    if not validate_date(date):
        return error_response("Formato de 'date' inválido. Use YYYY-MM-DD")

    query = text("""
        SELECT
            session_id,
            user_id,
            anomaly_type,
            z_score,
            metric_value,
            description
        FROM anomalies
        WHERE date = :date
        ORDER BY z_score DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"date": date}).fetchall()

    results = [
        {
            "session_id":   row[0],
            "user_id":      row[1],
            "anomaly_type": row[2],
            "z_score":      round(row[3], 3),
            "metric_value": round(row[4], 2),
            "description":  row[5],
            "severity":     "high" if row[3] >= 4.0 else "medium" if row[3] >= 3.0 else "low",
        }
        for row in rows
    ]

    return jsonify({
        "date": date,
        "total_anomalies": len(results),
        "anomalies": results,
    })


# ──────────────────────────────────────────────
# HEALTH CHECK
# ──────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "ShopStream API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

if __name__ == "__main__":
    if USE_SQLITE:
        init_dev_db()
    app.run(debug=True, port=5000)