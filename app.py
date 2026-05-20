import os
import re
import json
import uuid
import threading
from datetime import datetime
from functools import wraps
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, send_file, abort,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-generate-a-32-char-string!")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
JOBS_FILE = os.path.join(OUTPUTS_DIR, "jobs.json")

job_store: dict = {}
job_store_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_outputs():
    os.makedirs(OUTPUTS_DIR, exist_ok=True)


def _save_jobs_to_disk():
    _ensure_outputs()
    try:
        with open(JOBS_FILE, "w") as f:
            json.dump(job_store, f, indent=2, default=str)
    except Exception:
        pass


def _load_jobs_from_disk() -> dict:
    _ensure_outputs()
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


# ---------------------------------------------------------------------------
# Startup: load history and mark stale running jobs as failed
# ---------------------------------------------------------------------------

job_store = _load_jobs_from_disk()
for _jid, _job in job_store.items():
    if _job.get("status") == "running":
        _job["status"] = "failed"
        _job["error"] = "Job interrupted by server restart"
_save_jobs_to_disk()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        expected = os.environ.get("APP_PASSWORD", "")
        if expected and password == expected:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Wrong password")
    return render_template("login.html", error=None)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def index():
    job_id = request.args.get("job_id")
    return render_template("index.html", job_id=job_id)


@app.route("/run", methods=["POST"])
@login_required
def run_job():
    query = request.form.get("query", "").strip()
    cities_raw = request.form.get("cities", "").strip()
    try:
        max_per_city = int(request.form.get("max_per_city", 500))
    except ValueError:
        max_per_city = 500

    cities_list = [c.strip() for c in cities_raw.splitlines() if c.strip()]
    if not query or not cities_list:
        return redirect(url_for("index"))

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    with job_store_lock:
        job_store[job_id] = {
            "job_id": job_id,
            "query": query,
            "cities_submitted": len(cities_list),
            "max_per_city": max_per_city,
            "status": "running",
            "progress": f"0/{len(cities_list)} cities complete",
            "cities_completed": 0,
            "cities_succeeded": 0,
            "cities_failed": 0,
            "failed_cities": [],
            "per_city_results": [],
            "total_before_dedup": 0,
            "total_after_dedup": 0,
            "total_duplicates_removed": 0,
            "runtime": "0h 0m",
            "download_url": None,
            "started_at": now,
            "completed_at": None,
            "error": None,
        }
        _save_jobs_to_disk()

    thread = threading.Thread(
        target=_run_batch_safe,
        args=(query, cities_list, max_per_city, job_id),
        daemon=True,
        name=f"job-{job_id[:8]}",
    )
    thread.start()

    return redirect(f"/?job_id={job_id}")


def _run_batch_safe(query, cities_list, max_per_city, job_id):
    try:
        from batch_runner import run_batch
        run_batch(query, cities_list, max_per_city, job_id, job_store, job_store_lock)
    except Exception as e:
        with job_store_lock:
            job_store[job_id]["status"] = "failed"
            job_store[job_id]["error"] = str(e)
            _save_jobs_to_disk()


@app.route("/status/<job_id>")
@login_required
def status(job_id):
    with job_store_lock:
        job = job_store.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/download/<job_id>")
@login_required
def download(job_id):
    with job_store_lock:
        job = job_store.get(job_id)
    if not job or job.get("status") != "done":
        abort(404)

    filepath = os.path.join(OUTPUTS_DIR, f"{job_id}_master.csv")
    if not os.path.exists(filepath):
        abort(404)

    slug = lambda s: re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    download_name = f"{slug(job['query'])}_{job_id[:8]}.csv"
    return send_file(filepath, as_attachment=True, download_name=download_name)


@app.route("/jobs")
@login_required
def jobs():
    with job_store_lock:
        all_jobs = list(job_store.values())
    all_jobs.sort(key=lambda j: j.get("started_at", ""), reverse=True)
    return render_template("jobs.html", jobs=all_jobs)


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
