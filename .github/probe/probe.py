"""One-off, read-only schema probe (delete after use). Prints what the dashboard needs to know
about seller statuses and team moves; touches no data."""
import json, os, sys, urllib.request, urllib.parse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import refresh as R

s = R.login()

def api(path, body=None):
    req = urllib.request.Request(R.MB_URL + path, data=json.dumps(body).encode() if body else None,
                                 method="POST" if body else "GET")
    req.add_header("Content-Type", "application/json"); req.add_header("X-Metabase-Session", s)
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read())

def show(title, fn):
    print(f"\n===== {title}")
    try:
        out = fn()
        print(out if isinstance(out, str) else json.dumps(out, indent=1, default=str)[:6000])
    except Exception as e:  # noqa: BLE001
        msg = e.read().decode("utf-8", "replace")[:600] if hasattr(e, "read") else str(e)
        print("ERROR:", msg)

def bq(sql):
    return R.run_csv(s, sql)[:6000]

# 1. the Metabase table the owner linked (database 23, table 3462)
meta = {}
def t3462():
    m = api("/api/table/3462/query_metadata"); meta.update(m)
    return {"table": m.get("name"), "schema": m.get("schema"), "db": (m.get("db") or {}).get("engine"),
            "fields": [(f["id"], f["name"], f.get("base_type")) for f in m.get("fields", [])]}
show("table 3462 metadata", t3462)

def dist(name):
    f = next((f for f in meta.get("fields", []) if f["name"] == name), None)
    if not f: return f"no field {name}"
    q = {"database": 23, "type": "query", "query": {"source-table": 3462, "aggregation": [["count"]],
         "breakout": [["field", f["id"], None]], "order-by": [["desc", ["aggregation", 0]]], "limit": 60}}
    return api("/api/dataset", q)["data"]["rows"]
for n in ["seller_account_status", "seller_internal_status"]:
    show(f"t3462 distribution of {n}", lambda n=n: dist(n))
def both():
    fa = next((f for f in meta.get("fields", []) if f["name"] == "seller_account_status"), None)
    fi = next((f for f in meta.get("fields", []) if f["name"] == "seller_internal_status"), None)
    q = {"database": 23, "type": "query", "query": {"source-table": 3462, "aggregation": [["count"]],
         "breakout": [["field", fa["id"], None], ["field", fi["id"], None]], "order-by": [["desc", ["aggregation", 0]]], "limit": 80}}
    return api("/api/dataset", q)["data"]["rows"]
show("t3462 account x internal status", both)
for n in ["updatedAt", "updated_at", "status_updated_at", "internal_status_updated_at"]:
    pass

# 2. the same fields in BigQuery
show("BQ sellers status-like columns", lambda: bq("""
SELECT column_name, data_type FROM nushop.INFORMATION_SCHEMA.COLUMNS
WHERE table_name='sellers' AND REGEXP_CONTAINS(LOWER(column_name), r'status|serve|revival|churn|hyper|pause|state')"""))
show("BQ sellers account x internal status", lambda: bq("""
SELECT seller_account_status, seller_internal_status, COUNT(*) n FROM nushop.sellers GROUP BY 1,2 ORDER BY n DESC LIMIT 60"""))

# 3. where moves are logged
show("BQ changeslogs columns", lambda: bq("""
SELECT column_name, data_type FROM nushop.INFORMATION_SCHEMA.COLUMNS WHERE table_name='changeslogs'"""))
show("BQ changeslogs subcategories, last 60d", lambda: bq("""
SELECT subcategory, COUNT(*) n FROM nushop.changeslogs
WHERE DATE(createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND createdat >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY))
GROUP BY 1 ORDER BY n DESC LIMIT 120"""))
show("BQ changeslogs status-ish transitions, last 60d", lambda: bq("""
SELECT subcategory, initial_value, final_value, COUNT(*) n FROM nushop.changeslogs
WHERE DATE(createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND createdat >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY))
  AND REGEXP_CONTAINS(LOWER(subcategory), r'status|serve|revival|churn|hyper|pause|state|team')
GROUP BY 1,2,3 ORDER BY n DESC LIMIT 120"""))
show("BQ workboard task types for revival/hypercare/self-serve/churn, last 90d", lambda: bq("""
SELECT type, sub_type, COUNT(*) n FROM nushop.workboard_tasks
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(type,''),' ',IFNULL(sub_type,''))), r'revival|hyper|self|churn|serve|unassign')
GROUP BY 1,2 ORDER BY n DESC LIMIT 80"""))
show("BQ seller_managers columns", lambda: bq("""
SELECT column_name, data_type FROM nushop.INFORMATION_SCHEMA.COLUMNS WHERE table_name='seller_managers'"""))
show("BQ seller_managers manager_type counts", lambda: bq("""
SELECT manager_type, COUNT(*) n FROM nushop.seller_managers GROUP BY 1 ORDER BY n DESC"""))
