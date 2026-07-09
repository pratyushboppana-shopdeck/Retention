#!/usr/bin/env python3
"""Refresh data.json from Metabase card 11419.

Authenticates with email/password (from env / GitHub secrets), runs the saved
card, and writes the CSV plus a timestamp into data.json. Standard library only.
"""
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

MB_URL = os.environ.get("METABASE_URL", "https://metabase.kaip.in").rstrip("/")
EMAIL = os.environ["METABASE_USER_EMAIL"]
PASSWORD = os.environ["METABASE_PASSWORD"]
CARD_ID = int(os.environ.get("METABASE_CARD_ID", "11419"))
OUT = os.environ.get("OUT_FILE", "data.json")


def _post(path, payload, headers=None, expect_json=True):
    data = json.dumps(payload).encode() if payload is not None else b""
    req = urllib.request.Request(MB_URL + path, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read().decode("utf-8", "replace")
    return json.loads(body) if expect_json else body


def main():
    # 1) session
    session = _post("/api/session", {"username": EMAIL, "password": PASSWORD})["id"]
    # 2) run the card, ask for CSV
    csv = _post(
        f"/api/card/{CARD_ID}/query/csv",
        None,
        headers={"X-Metabase-Session": session},
        expect_json=False,
    ).strip()

    if "Cohort" not in csv.splitlines()[0]:
        print("Unexpected CSV header:", csv.splitlines()[0], file=sys.stderr)
        sys.exit(1)

    snap = {
        "source": f"Metabase card {CARD_ID} — 3K Retention Curve- base data",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "csv": csv,
    }
    with open(OUT, "w") as f:
        json.dump(snap, f, indent=2)
        f.write("\n")
    print(f"Wrote {OUT}: {len(csv.splitlines())-1} data rows, synced {snap['generated_at']}")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} from Metabase: {e.read().decode('utf-8','replace')[:300]}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"Refresh failed: {e}", file=sys.stderr)
        sys.exit(1)
