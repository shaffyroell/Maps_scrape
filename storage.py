import os
import json
import threading

import requests as _http

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
JOBS_FILE = os.path.join(OUTPUTS_DIR, "jobs.json")

_UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
_UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
_UPSTASH_KEY = "maps_scraper_jobs"


def _upstash_available() -> bool:
    return bool(_UPSTASH_URL and _UPSTASH_TOKEN)


def _upstash_write(payload: str) -> None:
    """Fire-and-forget Upstash write (called from a daemon thread)."""
    try:
        resp = _http.post(
            _UPSTASH_URL,
            headers={"Authorization": f"Bearer {_UPSTASH_TOKEN}"},
            json=["SET", _UPSTASH_KEY, payload],
            timeout=8,
        )
        if resp.status_code != 200:
            import sys
            print(f"[storage] Upstash write failed: HTTP {resp.status_code} "
                  f"deny={resp.headers.get('x-deny-reason','?')} body={resp.text[:200]}",
                  file=sys.stderr)
    except Exception as e:
        import sys
        print(f"[storage] Upstash write exception: {e}", file=sys.stderr)


def save_jobs(job_store: dict) -> None:
    """Write job store to local file (sync) and Upstash (async)."""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    try:
        payload = json.dumps(job_store, default=str)
        with open(JOBS_FILE, "w") as f:
            f.write(payload)
    except Exception:
        payload = None

    if _upstash_available() and payload:
        threading.Thread(target=_upstash_write, args=(payload,), daemon=True).start()


def load_jobs() -> dict:
    """Load job store — Upstash first (survives redeploys), then local file."""
    if _upstash_available():
        try:
            resp = _http.post(
                _UPSTASH_URL,
                headers={"Authorization": f"Bearer {_UPSTASH_TOKEN}"},
                json=["GET", _UPSTASH_KEY],
                timeout=8,
            )
            if resp.status_code == 200:
                result = resp.json().get("result")
                if result:
                    return json.loads(result)
            else:
                import sys
                print(f"[storage] Upstash read failed: HTTP {resp.status_code} "
                      f"deny={resp.headers.get('x-deny-reason','?')} body={resp.text[:200]}",
                      file=sys.stderr)
        except Exception as e:
            import sys
            print(f"[storage] Upstash read exception: {e}", file=sys.stderr)

    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}
