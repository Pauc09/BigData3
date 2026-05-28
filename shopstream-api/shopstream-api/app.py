from flask import Flask, jsonify, request
import psycopg2
import os

app = Flask(__name__)

DB_CONFIG = {
    "host":     "shopstream-dw.czlint0ubsu5.us-east-1.rds.amazonaws.com",
    "port":     5432,
    "database": "shopstream",
    "user":     "shopstream",
    "password": "ShopStream2026!"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ── Endpoint 1: Top páginas por tasa de rebote o tiempo ──
@app.route("/pages/top", methods=["GET"])
def pages_top():
    metric = request.args.get("metric", "bounce_rate")
    date   = request.args.get("date", "2026-05-20")
    limit  = request.args.get("limit", 10)

    if metric == "bounce_rate":
        col = "bounce_rate"
    else:
        col = "avg_time_on_page"

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT date, page_url, page_type, avg_time_on_page, bounce_rate, total_sessions
            FROM page_metrics
            WHERE date = %s
            ORDER BY {col} DESC
            LIMIT %s
        """, (date, limit))
        rows = cur.fetchall()
        conn.close()

        result = []
        for row in rows:
            result.append({
                "date":             row[0],
                "page_url":         row[1],
                "page_type":        row[2],
                "avg_time_on_page": row[3],
                "bounce_rate":      row[4],
                "total_sessions":   row[5],
            })
        return jsonify({"metric": metric, "date": date, "data": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Endpoint 2: Resumen de sesiones por país y dispositivo ──
@app.route("/sessions/summary", methods=["GET"])
def sessions_summary():
    country = request.args.get("country", "")
    device  = request.args.get("device", "")
    date    = request.args.get("date", "2026-05-20")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        query = """
            SELECT date, country, device_type, total_sessions,
                   avg_session_duration, total_pageviews, unique_users
            FROM session_summary
            WHERE date = %s
        """
        params = [date]
        if country:
            query += " AND country = %s"
            params.append(country.upper())
        if device:
            query += " AND device_type = %s"
            params.append(device.lower())

        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        result = []
        for row in rows:
            result.append({
                "date":                 row[0],
                "country":              row[1],
                "device_type":          row[2],
                "total_sessions":       row[3],
                "avg_session_duration": row[4],
                "total_pageviews":      row[5],
                "unique_users":         row[6],
            })
        return jsonify({"date": date, "filters": {"country": country, "device": device}, "data": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Endpoint 3: Anomalías detectadas ──
@app.route("/anomalies", methods=["GET"])
def anomalies():
    date = request.args.get("date", "2026-05-20")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT date, session_id, user_id, anomaly_type, z_score, metric_value, description
            FROM anomalies
            WHERE date = %s
            ORDER BY z_score DESC
        """, (date,))
        rows = cur.fetchall()
        conn.close()

        result = []
        for row in rows:
            result.append({
                "date":         row[0],
                "session_id":   row[1],
                "user_id":      row[2],
                "anomaly_type": row[3],
                "z_score":      row[4],
                "metric_value": row[5],
                "description":  row[6],
            })
        return jsonify({"date": date, "total_anomalies": len(result), "data": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)