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

job_store: dict = {}
job_store_lock = threading.Lock()

from storage import save_jobs as _save_jobs_to_disk, load_jobs as _load_jobs_from_disk


# ---------------------------------------------------------------------------
# Startup: load history and mark stale running/stopping jobs as failed
# ---------------------------------------------------------------------------

job_store = _load_jobs_from_disk()
for _jid, _job in job_store.items():
    if _job.get("status") in ("running", "stopping"):
        _job["status"] = "failed"
        _job["error"] = "Job interrupted by server restart"
        _job.pop("cancel_requested", None)
_save_jobs_to_disk(job_store)


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
    queries_raw = request.form.get("queries", "").strip()
    cities_raw = request.form.get("cities", "").strip()
    try:
        max_per_city = int(request.form.get("max_per_city", 500))
    except ValueError:
        max_per_city = 500

    queries = [q.strip() for q in queries_raw.splitlines() if q.strip()][:10]
    cities_list = [c.strip() for c in cities_raw.splitlines() if c.strip()]

    if not queries or not cities_list:
        return redirect(url_for("index"))

    # All (query, city) combinations — this is the unit of parallel work
    combinations = [(q, c) for q in queries for c in cities_list]

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    with job_store_lock:
        job_store[job_id] = {
            "job_id": job_id,
            "queries": queries,
            "queries_submitted": len(queries),
            "cities_submitted": len(cities_list),
            "total_combinations": len(combinations),
            "max_per_city": max_per_city,
            "status": "running",
            "progress": f"0/{len(combinations)} combinations complete",
            "combinations_completed": 0,
            "combinations_succeeded": 0,
            "combinations_failed": 0,
            "failed_combinations": [],
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
        _save_jobs_to_disk(job_store)

    thread = threading.Thread(
        target=_run_batch_safe,
        args=(combinations, max_per_city, job_id),
        daemon=True,
        name=f"job-{job_id[:8]}",
    )
    thread.start()

    return redirect(f"/?job_id={job_id}")


def _run_batch_safe(combinations, max_per_city, job_id):
    try:
        from batch_runner import run_batch
        run_batch(combinations, max_per_city, job_id, job_store, job_store_lock)
    except Exception as e:
        with job_store_lock:
            job_store[job_id]["status"] = "failed"
            job_store[job_id]["error"] = str(e)
            _save_jobs_to_disk(job_store)


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
    queries = job.get("queries", [])
    label = slug(queries[0]) if queries else "leads"
    download_name = f"{label}_{job_id[:8]}.csv"
    return send_file(filepath, as_attachment=True, download_name=download_name)


@app.route("/cancel/<job_id>", methods=["POST"])
@login_required
def cancel_job(job_id):
    with job_store_lock:
        job = job_store.get(job_id)
        if not job or job.get("status") != "running":
            return jsonify({"ok": False, "error": "Job not running"}), 400
        job["cancel_requested"] = True
        job["status"] = "stopping"
        _save_jobs_to_disk(job_store)
    return jsonify({"ok": True})


@app.route("/delete/<job_id>", methods=["POST"])
@login_required
def delete_job(job_id):
    with job_store_lock:
        job = job_store.pop(job_id, None)
        if job:
            _save_jobs_to_disk(job_store)

    if job:
        # Remove master CSV and per-city CSV directory
        import shutil
        master = os.path.join(OUTPUTS_DIR, f"{job_id}_master.csv")
        job_dir = os.path.join(OUTPUTS_DIR, job_id)
        try:
            if os.path.exists(master):
                os.remove(master)
            if os.path.isdir(job_dir):
                shutil.rmtree(job_dir)
        except Exception:
            pass

    return jsonify({"ok": True})


@app.route("/jobs")
@login_required
def jobs():
    with job_store_lock:
        all_jobs = list(job_store.values())
    all_jobs.sort(key=lambda j: j.get("started_at", ""), reverse=True)
    return render_template("jobs.html", jobs=all_jobs)


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
