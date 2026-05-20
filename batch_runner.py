import os
import csv
import re
import sys
import json
import shutil
import subprocess
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
JOBS_FILE = os.path.join(OUTPUTS_DIR, "jobs.json")
MAPS_LEADS_PATH = os.path.join(BASE_DIR, "maps_leads.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _predict_csv_name(query: str, city: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"{_slug(query)}_{_slug(city)}_{date_str}.csv"


def _format_runtime(start_time: datetime) -> str:
    elapsed = int((datetime.utcnow() - start_time).total_seconds())
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    return f"{h}h {m}m"


def _save_jobs(job_store: dict) -> None:
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    try:
        with open(JOBS_FILE, "w") as f:
            json.dump(job_store, f, indent=2, default=str)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Worker (runs in a thread; subprocess does the actual scraping)
# ---------------------------------------------------------------------------

def _process_city(args: tuple) -> dict:
    query, city, max_per_city = args

    expected_csv = os.path.join(BASE_DIR, _predict_csv_name(query, city))

    # Remove stale CSV to avoid reading previous run's data
    if os.path.exists(expected_csv):
        try:
            os.remove(expected_csv)
        except OSError:
            pass

    try:
        proc = subprocess.run(
            [sys.executable, MAPS_LEADS_PATH,
             "--query", query, "--city", city, "--max", str(max_per_city)],
            capture_output=True,
            text=True,
            timeout=300,
        )

        time.sleep(2)  # inter-city rate-limit courtesy

        if proc.returncode != 0 or not os.path.exists(expected_csv):
            return {"city": city, "status": "failed", "leads": 0,
                    "csv_path": None, "place_ids": []}

        with open(expected_csv, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if not rows:
            return {"city": city, "status": "failed", "leads": 0,
                    "csv_path": expected_csv, "place_ids": []}

        place_ids = [r.get("place_id", "") for r in rows]
        return {
            "city": city,
            "status": "done",
            "leads": len(rows),
            "csv_path": expected_csv,
            "place_ids": place_ids,
        }

    except subprocess.TimeoutExpired:
        return {"city": city, "status": "failed", "leads": 0,
                "csv_path": None, "place_ids": [], "error": "timeout"}
    except Exception as e:
        return {"city": city, "status": "failed", "leads": 0,
                "csv_path": None, "place_ids": [], "error": str(e)}


# ---------------------------------------------------------------------------
# Main batch function
# ---------------------------------------------------------------------------

def run_batch(
    query: str,
    cities_list: list,
    max_per_city: int,
    job_id: str,
    job_store: dict,
    lock,
) -> None:
    start_time = datetime.utcnow()
    job_dir = os.path.join(OUTPUTS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    total = len(cities_list)
    # city -> result dict; preserves latest status per city
    per_city: dict[str, dict] = {}
    seen_place_ids: set[str] = set()
    city_csv_paths: dict[str, str] = {}   # city -> moved CSV path
    completed_first_pass = 0

    # ------------------------------------------------------------------
    def _handle_result(result: dict, is_retry: bool = False) -> None:
        city = result["city"]

        if result["status"] == "done" and result["leads"] > 0:
            place_ids = result.get("place_ids", [])
            dupes = sum(1 for pid in place_ids if pid and pid in seen_place_ids)
            seen_place_ids.update(pid for pid in place_ids if pid)

            csv_src = result.get("csv_path")
            if csv_src and os.path.exists(csv_src):
                dest = os.path.join(job_dir, os.path.basename(csv_src))
                try:
                    shutil.move(csv_src, dest)
                    city_csv_paths[city] = dest
                except OSError:
                    city_csv_paths[city] = csv_src

            per_city[city] = {
                "city": city,
                "leads": result["leads"],
                "duplicates_removed": dupes,
                "status": "retried" if is_retry else "done",
            }
        else:
            # Only overwrite if not already succeeded (e.g. retry after success)
            if city not in per_city or per_city[city]["status"] == "failed":
                per_city[city] = {
                    "city": city,
                    "leads": 0,
                    "duplicates_removed": 0,
                    "status": "failed",
                }

    def _update_store(completed_count: int) -> None:
        results_list = list(per_city.values())
        # preserve original city order
        city_index = {c: i for i, c in enumerate(cities_list)}
        ordered = sorted(results_list, key=lambda r: city_index.get(r["city"], 9999))

        succeeded = sum(1 for r in results_list if r["status"] in ("done", "retried"))
        failed = sum(1 for r in results_list if r["status"] == "failed")
        failed_list = [r["city"] for r in results_list if r["status"] == "failed"]

        with lock:
            job_store[job_id].update({
                "cities_completed": completed_count,
                "cities_succeeded": succeeded,
                "cities_failed": failed,
                "failed_cities": failed_list,
                "per_city_results": ordered,
                "progress": f"{completed_count}/{total} cities complete",
                "runtime": _format_runtime(start_time),
            })
            _save_jobs(job_store)

    # ------------------------------------------------------------------
    # First pass — 4 parallel workers
    # ------------------------------------------------------------------
    args_list = [(query, city, max_per_city) for city in cities_list]

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(_process_city, args): args[1]
            for args in args_list
        }
        for future in as_completed(future_map):
            city = future_map[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"city": city, "status": "failed", "leads": 0,
                          "csv_path": None, "place_ids": [], "error": str(e)}

            _handle_result(result, is_retry=False)
            completed_first_pass += 1
            _update_store(completed_first_pass)

    # ------------------------------------------------------------------
    # Retry failed cities once
    # ------------------------------------------------------------------
    failed_cities = [city for city, r in per_city.items() if r["status"] == "failed"]

    if failed_cities:
        retry_args = [(query, city, max_per_city) for city in failed_cities]

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_map = {
                executor.submit(_process_city, args): args[1]
                for args in retry_args
            }
            for future in as_completed(future_map):
                city = future_map[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {"city": city, "status": "failed", "leads": 0,
                              "csv_path": None, "place_ids": [], "error": str(e)}

                _handle_result(result, is_retry=True)
                _update_store(completed_first_pass)  # total count stays same during retries

    # ------------------------------------------------------------------
    # Merge all city CSVs → master file, dedup by place_id
    # ------------------------------------------------------------------
    all_rows: list[dict] = []
    seen_merge_ids: set[str] = set()
    total_before_dedup = 0

    for csv_path in city_csv_paths.values():
        if os.path.exists(csv_path):
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    total_before_dedup += 1
                    pid = row.get("place_id", "")
                    if pid and pid not in seen_merge_ids:
                        seen_merge_ids.add(pid)
                        all_rows.append(row)
                    elif not pid:
                        all_rows.append(row)

    total_after_dedup = len(all_rows)
    total_dupes = total_before_dedup - total_after_dedup

    master_path = os.path.join(OUTPUTS_DIR, f"{job_id}_master.csv")

    if all_rows:
        fieldnames = list(all_rows[0].keys())
        with open(master_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)
    else:
        open(master_path, "w").close()

    # ------------------------------------------------------------------
    # Final job update
    # ------------------------------------------------------------------
    runtime_str = _format_runtime(start_time)
    now = datetime.utcnow().isoformat()

    final_results = list(per_city.values())
    city_index = {c: i for i, c in enumerate(cities_list)}
    ordered_final = sorted(final_results, key=lambda r: city_index.get(r["city"], 9999))
    succeeded_final = sum(1 for r in final_results if r["status"] in ("done", "retried"))
    failed_final = sum(1 for r in final_results if r["status"] == "failed")
    failed_list_final = [r["city"] for r in final_results if r["status"] == "failed"]

    with lock:
        job_store[job_id].update({
            "status": "done",
            "cities_completed": total,
            "cities_succeeded": succeeded_final,
            "cities_failed": failed_final,
            "failed_cities": failed_list_final,
            "per_city_results": ordered_final,
            "total_before_dedup": total_before_dedup,
            "total_after_dedup": total_after_dedup,
            "total_duplicates_removed": total_dupes,
            "download_url": f"/download/{job_id}",
            "completed_at": now,
            "runtime": runtime_str,
            "progress": f"{total}/{total} cities complete",
        })
        _save_jobs(job_store)
