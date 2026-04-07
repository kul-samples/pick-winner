from flask import Flask, render_template_string, redirect, url_for, request, session
import psycopg2
import os
import random
import socket
import logging
from datetime import datetime

# --------------------------------------------------
# OpenTelemetry Setup (MUST BE FIRST)
# --------------------------------------------------
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor

# --------------------------------------------------
# Logging Setup
# --------------------------------------------------
LOG_DIR = "/var/log/luckydraw-app"
LOG_FILE = f"{LOG_DIR}/winner.log"

os.makedirs(LOG_DIR, exist_ok=True)

HOSTNAME = socket.gethostname()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s " + HOSTNAME + " %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# OpenTelemetry Configuration
# --------------------------------------------------
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "luckydraw-winner")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4317"
)

resource = Resource.create({
    "service.name": OTEL_SERVICE_NAME,
    "service.instance.id": HOSTNAME
})

provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)

otlp_exporter = OTLPSpanExporter(
    endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
    insecure=True
)

span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)

tracer = trace.get_tracer(__name__)

logger.info("OpenTelemetry initialized for service: %s", OTEL_SERVICE_NAME)

# --------------------------------------------------
# Config
# --------------------------------------------------
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "luckydraw-secret")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "postgres")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

# --------------------------------------------------
# Flask App
# --------------------------------------------------
app = Flask(__name__)
app.secret_key = SECRET_KEY

# Instrument Flask AFTER app creation
FlaskInstrumentor().instrument_app(app)

# Instrument psycopg2
Psycopg2Instrumentor().instrument()

logger.info("LuckyDraw Winner service starting")

# --------------------------------------------------
# DB Connection
# --------------------------------------------------
with tracer.start_as_current_span("postgres_connection"):
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    pg_conn.autocommit = False

logger.info("Connected to Postgres successfully")

# --------------------------------------------------
# Initialize DB
# --------------------------------------------------
def init_db():
    with tracer.start_as_current_span("init_db"):
        with pg_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS luckydraw_winner (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    picked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
        pg_conn.commit()

    logger.info("Winner table ensured")

init_db()

# --------------------------------------------------
# HTML Template
# --------------------------------------------------
HTML = """<html>
<head><title>Lucky Draw Admin</title></head>
<body>
<h2>Lucky Draw Admin</h2>

{% if not_logged %}
<form method="POST" action="/login">
<input name="username" placeholder="Username"><br>
<input name="password" type="password"><br>
<button type="submit">Login</button>
</form>
{% else %}

{% if latest %}
<h3>Winner: {{ latest.name }} ({{ latest.phone }})</h3>
{% endif %}

<form method="POST" action="/pick">
<button {% if no_entries %}disabled{% endif %}>Pick Winner</button>
</form>

<h3>History</h3>
<table border=1>
<tr><th>Name</th><th>Phone</th><th>Time</th></tr>
{% for w in winners %}
<tr>
<td>{{ w.name }}</td>
<td>{{ w.phone }}</td>
<td>{{ w.picked_at }}</td>
</tr>
{% endfor %}
</table>

<a href="/logout">Logout</a>

{% endif %}
</body>
</html>
"""

# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.route("/")
def index():

    with tracer.start_as_current_span("load_winner_dashboard"):

        logged = session.get("logged_in", False)

        with pg_conn.cursor() as cur:

            cur.execute("""
                SELECT name, phone, picked_at
                FROM luckydraw_winner
                ORDER BY picked_at DESC;
            """)

            winners = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*)
                FROM luckydraw
                WHERE state='not-participated';
            """)

            pending = cur.fetchone()[0]

    return render_template_string(
        HTML,
        winners=[{
            "name": w[0],
            "phone": w[1],
            "picked_at": w[2]
        } for w in winners],
        latest={
            "name": winners[0][0],
            "phone": winners[0][1],
            "picked_at": winners[0][2]
        } if winners else None,
        no_entries=pending == 0,
        not_logged=not logged
    )


@app.route("/login", methods=["POST"])
def login():

    with tracer.start_as_current_span("admin_login"):

        if (
            request.form["username"] == ADMIN_USERNAME and
            request.form["password"] == ADMIN_PASSWORD
        ):
            session["logged_in"] = True
            logger.info("Admin login successful")
        else:
            logger.warning("Admin login failed")

    return redirect(url_for("index"))


@app.route("/logout")
def logout():

    with tracer.start_as_current_span("admin_logout"):
        session.clear()

    return redirect(url_for("index"))


@app.route("/pick", methods=["POST"])
def pick():

    if not session.get("logged_in"):
        return redirect(url_for("index"))

    with tracer.start_as_current_span("pick_winner"):

        try:

            with pg_conn.cursor() as cur:

                cur.execute("""
                    SELECT id, name, phone
                    FROM luckydraw
                    WHERE state='not-participated'
                    FOR UPDATE;
                """)

                rows = cur.fetchall()

                if not rows:
                    pg_conn.rollback()
                    return redirect(url_for("index"))

                _, name, phone = random.choice(rows)

                cur.execute("""
                    INSERT INTO luckydraw_winner
                    (name, phone, picked_at)
                    VALUES (%s, %s, %s);
                """, (name, phone, datetime.utcnow()))

                cur.execute("""
                    UPDATE luckydraw
                    SET state='participated'
                    WHERE state='not-participated';
                """)

            pg_conn.commit()

            logger.info("Winner picked: %s", name)

        except Exception:

            pg_conn.rollback()
            logger.exception("Winner selection failed")

    return redirect(url_for("index"))


@app.route("/health")
def health():
    return {"status": "UP"}


# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":

    logger.info("Winner service running on port 5000")

    app.run(
        host="0.0.0.0",
        port=5000,
        use_reloader=False
    )
