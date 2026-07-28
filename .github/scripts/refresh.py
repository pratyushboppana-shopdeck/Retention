#!/usr/bin/env python3
"""Refresh data.json from Metabase (BigQuery), standard library only.

Produces two datasets on the canonical go-live >1000 base:
  * curve  -> per weekly-cohort W1..W7 3K-retention (feeds the chart)
  * diag   -> per-seller diagnosis for May-2026-onward cohorts (feeds the drill-down):
              weekly spend sp1..sp7, reason bucket, and current GC/GM/KAM (card 7753)
Reason buckets follow the agreed framework (cards 11435/11610/12049/12206), precedence
1->7, median cutoffs for performance & RTO. Only rewrites data.json when the data changes.
"""
import json, os, sys, urllib.request, urllib.error, urllib.parse, csv as csvmod, io
from datetime import datetime, timezone

MB_URL = os.environ.get("METABASE_URL", "https://metabase.kaip.in").rstrip("/")
EMAIL = os.environ["METABASE_USER_EMAIL"]
PASSWORD = os.environ["METABASE_PASSWORD"]
DB = int(os.environ.get("METABASE_DB", "6"))
OUT = os.environ.get("OUT_FILE", "data.json")

BUCKETS = ["Ad-account / platform block", "Payment / funding block", "No activation / no demand",
           "Poor ad performance", "Fulfilment / RTO", "Elective pause / no refill", "Other / unclassified"]
BIDX = {b: i for i, b in enumerate(BUCKETS)}

CURVE_SQL = r"""
WITH golive AS (
  SELECT seller_id, MIN(start_date) AS golive_date, FORMAT_DATE('%G-W%V', MIN(start_date)) AS golive_iso_week
  FROM nushop.gc_view_3 WHERE marketing_spend > 1000 AND team_mapping = 'HIT'
  GROUP BY seller_id
  HAVING MIN(start_date) >= DATE '2025-11-01' AND MIN(start_date) <= DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
),
weekly_spend AS (
  SELECT cs.seller_id, cs.golive_iso_week, DATE_DIFF(gv.start_date, cs.golive_date, ISOWEEK) AS rel_week, SUM(gv.marketing_spend) AS week_spend
  FROM golive cs JOIN nushop.gc_view_3 gv ON cs.seller_id = gv.seller_id
  WHERE DATE_DIFF(gv.start_date, cs.golive_date, ISOWEEK) BETWEEN 1 AND 7 GROUP BY 1,2,3
),
flags AS (
  SELECT seller_id, golive_iso_week,
    MAX(IF(rel_week=1 AND week_spend>=3000,1,0)) s1, MAX(IF(rel_week=2 AND week_spend>=3000,1,0)) s2,
    MAX(IF(rel_week=3 AND week_spend>=3000,1,0)) s3, MAX(IF(rel_week=4 AND week_spend>=3000,1,0)) s4,
    MAX(IF(rel_week=5 AND week_spend>=3000,1,0)) s5, MAX(IF(rel_week=6 AND week_spend>=3000,1,0)) s6,
    MAX(IF(rel_week=7 AND week_spend>=3000,1,0)) s7
  FROM weekly_spend GROUP BY 1,2
),
cohort_counts AS (
  SELECT golive_iso_week, COUNT(*) golives, DATE_DIFF(CURRENT_DATE(), MIN(golive_date), ISOWEEK) weeks_elapsed
  FROM golive GROUP BY golive_iso_week
)
SELECT c.golive_iso_week AS Cohort, c.golives AS `go-lives`,
  IF(c.weeks_elapsed>1, ROUND(100*COALESCE(SUM(f.s1),0)/c.golives,2), NULL) W1,
  IF(c.weeks_elapsed>2, ROUND(100*COALESCE(SUM(f.s2),0)/c.golives,2), NULL) W2,
  IF(c.weeks_elapsed>3, ROUND(100*COALESCE(SUM(f.s3),0)/c.golives,2), NULL) W3,
  IF(c.weeks_elapsed>4, ROUND(100*COALESCE(SUM(f.s4),0)/c.golives,2), NULL) W4,
  IF(c.weeks_elapsed>5, ROUND(100*COALESCE(SUM(f.s5),0)/c.golives,2), NULL) W5,
  IF(c.weeks_elapsed>6, ROUND(100*COALESCE(SUM(f.s6),0)/c.golives,2), NULL) W6,
  IF(c.weeks_elapsed>7, ROUND(100*COALESCE(SUM(f.s7),0)/c.golives,2), NULL) W7
FROM cohort_counts c LEFT JOIN flags f USING (golive_iso_week)
GROUP BY c.golive_iso_week, c.golives, c.weeks_elapsed ORDER BY c.golive_iso_week
"""

DIAG_SQL = r"""
WITH golive AS (
  SELECT seller_id, MIN(start_date) AS golive_date, FORMAT_DATE('%G-W%V', MIN(start_date)) AS gw
  FROM nushop.gc_view_3 WHERE marketing_spend > 1000 AND team_mapping='HIT'
  GROUP BY seller_id
  HAVING DATE_TRUNC(MIN(start_date),ISOWEEK) >= DATE '2026-05-04'
     AND DATE_TRUNC(MIN(start_date),ISOWEEK) < DATE_TRUNC(CURRENT_DATE(),ISOWEEK)
),
sw AS (
  SELECT g.seller_id,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=1,v.marketing_spend,0)) sp1,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=2,v.marketing_spend,0)) sp2,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=3,v.marketing_spend,0)) sp3,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=4,v.marketing_spend,0)) sp4,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=5,v.marketing_spend,0)) sp5,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=6,v.marketing_spend,0)) sp6,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK)=7,v.marketing_spend,0)) sp7,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK) BETWEEN 0 AND 3,v.rtos,0)) rto03,
    SUM(IF(DATE_DIFF(v.start_date,g.golive_date,ISOWEEK) BETWEEN 0 AND 3,v.total_orders,0)) ord03
  FROM golive g JOIN nushop.gc_view_3 v ON v.seller_id=g.seller_id GROUP BY 1
),
ord AS (
  SELECT g.seller_id, COUNT(*) n_orders_02,
    SUM(oi.selling_price*oi.quantity+oi.cod_charge+oi.delivery_fees-oi.total_discount) gmv02
  FROM golive g JOIN nushop.orderitems oi ON oi.seller_id=g.seller_id
  WHERE DATE(oi.createdat,'Asia/Kolkata')>=DATE '2026-04-15'
    AND oi.seller_last_status NOT IN ('initiated','enqueued','invalid') AND oi.awb_no!='None' AND oi.in_house_status!='awb_expired'
    AND DATE_DIFF(DATE(oi.createdat,'Asia/Kolkata'),g.golive_date,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1
),
fb AS (
  SELECT g.seller_id, SUM(f.spend) fb_sp, SUM(f.impressions) imp, SUM(f.clicks) clk
  FROM golive g JOIN fb_marketings.fb_marketing_insights f ON f.seller_id=g.seller_id
  WHERE f.breakdown_key IS NULL AND DATE(f.spend_date,'Asia/Kolkata')>=DATE '2026-04-15'
    AND DATE_DIFF(DATE(f.spend_date,'Asia/Kolkata'),g.golive_date,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1
),
adtix AS (
  SELECT g.seller_id,1 ad_impact FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE t.source='crm_initiated' AND t.created_by IS NOT NULL AND DATE(t.created_at)>=DATE '2026-04-01'
    AND t.sub_type IN ('ad_account_suspension','ad_account_blocked','business_manager_verification','business_manager_restricted','pixel_inactive','page_restricted','ad_account_hacked','business_manager_access','account_restricted','account_permanently_restricted','page_unpublished','ad_account_has_limit')
    AND DATE_DIFF(DATE(t.created_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)<=2
    AND (t.completed_at IS NULL OR DATE_DIFF(DATE(t.completed_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)>=3) GROUP BY 1
),
paytix AS (
  SELECT g.seller_id,1 pay_impact FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE t.source='crm_initiated' AND t.created_by IS NOT NULL AND DATE(t.created_at)>=DATE '2026-04-01'
    AND t.sub_type IN ('add_funds','payment_failed','change_payment_method','payment_processing','transactions_failure')
    AND DATE_DIFF(DATE(t.created_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)<=2
    AND (t.completed_at IS NULL OR DATE_DIFF(DATE(t.completed_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)>=3) GROUP BY 1
),
fbdis AS (
  SELECT g.seller_id, MAX(1) disabled,
    MAX(IF(h.reason='RISK_PAYMENT' OR LOWER(h.detailed_reason_text) LIKE '%payment%',1,0)) dis_payment
  FROM golive g JOIN fb_marketings.fb_ad_account_block_history h ON h.seller_id=g.seller_id
  WHERE h.ad_account_issues='DISABLED' AND DATE_DIFF(DATE(h.created_at,'Asia/Kolkata'),g.golive_date,ISOWEEK) BETWEEN 0 AND 3 GROUP BY 1
),
mgr AS (SELECT seller_id, MAX(gc_name) gc, MAX(gm_name) gm, MAX(kam_name) kam FROM `blitzscale-prod-project.analytics.seller_console_metrics_summary` GROUP BY 1),
base AS (
  SELECT g.seller_id, g.gw,
    ROUND(sw.sp1) sp1,ROUND(sw.sp2) sp2,ROUND(sw.sp3) sp3,ROUND(sw.sp4) sp4,ROUND(sw.sp5) sp5,ROUND(sw.sp6) sp6,ROUND(sw.sp7) sp7,
    IF(g.golive_date<=DATE_SUB(CURRENT_DATE(),INTERVAL 28 DAY),1,0) w3_mature,
    IF(sw.sp3>=3000,1,0) retained_w3,
    COALESCE(o.n_orders_02,0) orders02, COALESCE(o.gmv02,0) gmv02,
    SAFE_DIVIDE(fb.fb_sp,o.gmv02) sgmv, SAFE_DIVIDE(sw.rto03,sw.ord03) rto_rate,
    COALESCE(a.ad_impact,0) ad_impact, COALESCE(p.pay_impact,0) pay_impact,
    COALESCE(d.disabled,0) disabled, COALESCE(d.dis_payment,0) dis_payment,
    COALESCE(NULLIF(m.gc,''),'-') gc, COALESCE(NULLIF(m.gm,''),'-') gm, COALESCE(NULLIF(m.kam,''),'-') kam
  FROM golive g LEFT JOIN sw USING(seller_id) LEFT JOIN ord o USING(seller_id) LEFT JOIN fb USING(seller_id)
  LEFT JOIN adtix a USING(seller_id) LEFT JOIN paytix p USING(seller_id) LEFT JOIN fbdis d USING(seller_id) LEFT JOIN mgr m USING(seller_id)
),
med AS (SELECT APPROX_QUANTILES(sgmv,2)[OFFSET(1)] m_sgmv, APPROX_QUANTILES(rto_rate,2)[OFFSET(1)] m_rto FROM base WHERE w3_mature=1 AND retained_w3=0)
SELECT b.seller_id, b.gw AS golive_iso_week, b.sp1,b.sp2,b.sp3,b.sp4,b.sp5,b.sp6,b.sp7,
  CASE
    WHEN (b.disabled=1 AND b.dis_payment=0) OR b.ad_impact=1 THEN 'Ad-account / platform block'
    WHEN b.dis_payment=1 OR b.pay_impact=1 THEN 'Payment / funding block'
    WHEN b.orders02=0 AND b.gmv02<=0 THEN 'No activation / no demand'
    WHEN b.sgmv > med.m_sgmv THEN 'Poor ad performance'
    WHEN b.rto_rate > med.m_rto THEN 'Fulfilment / RTO'
    WHEN b.orders02>0 THEN 'Elective pause / no refill'
    ELSE 'Other / unclassified'
  END bucket,
  b.gc, b.gm, b.kam
FROM base b CROSS JOIN med ORDER BY b.gw, b.seller_id
"""


def login():
    body = json.dumps({"username": EMAIL, "password": PASSWORD}).encode()
    req = urllib.request.Request(MB_URL + "/api/session", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["id"]


def run_csv(session, sql):
    """Run native SQL, return CSV text (no row cap via the /csv endpoint)."""
    query = {"database": DB, "type": "native", "native": {"query": sql}, "parameters": []}
    data = urllib.parse.urlencode({"query": json.dumps(query)}).encode()
    req = urllib.request.Request(MB_URL + "/api/dataset/csv", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("X-Metabase-Session", session)
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read().decode("utf-8", "replace")


def parse_diag(csv_text):
    rows = list(csvmod.DictReader(io.StringIO(csv_text)))
    out = []
    for r in rows:
        def gi(k):
            try:
                return int(round(float(r.get(k) or 0)))
            except ValueError:
                return 0
        out.append({
            "s": r["seller_id"], "w": r["golive_iso_week"],
            "sp": [gi("sp1"), gi("sp2"), gi("sp3"), gi("sp4"), gi("sp5"), gi("sp6"), gi("sp7")],
            "b": BIDX.get((r.get("bucket") or "").strip(), 6),
            "gc": r.get("gc") or "-", "gm": r.get("gm") or "-", "kam": r.get("kam") or "-",
        })
    return out


def main():
    session = login()
    curve_csv = run_csv(session, CURVE_SQL).strip()
    if "Cohort" not in curve_csv.splitlines()[0]:
        print("Unexpected curve header:", curve_csv.splitlines()[0], file=sys.stderr); sys.exit(1)
    diag = parse_diag(run_csv(session, DIAG_SQL))
    if not diag:
        print("Diagnosis query returned no rows", file=sys.stderr); sys.exit(1)

    payload = {"csv": curve_csv, "buckets": BUCKETS, "diagnosis": diag}
    # only rewrite when the DATA changed (ignore the timestamp), to avoid commit noise
    try:
        with open(OUT) as f:
            old = json.load(f)
        if old.get("csv") == payload["csv"] and old.get("diagnosis") == payload["diagnosis"] and old.get("buckets") == payload["buckets"]:
            print(f"No data change ({len(diag)} sellers, {len(curve_csv.splitlines())-1} cohorts) - leaving {OUT}."); return
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    out = {"source": "Metabase go-live>1000 base (card 11419 curve) + per-seller diagnosis (cards 11435/11610/12049/12206 + 7753)",
           "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           **payload}
    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":")); f.write("\n")
    print(f"Wrote {OUT}: {len(curve_csv.splitlines())-1} curve cohorts, {len(diag)} diagnosis sellers.")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} from Metabase: {e.read().decode('utf-8','replace')[:400]}", file=sys.stderr); sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"Refresh failed: {e}", file=sys.stderr); sys.exit(1)
