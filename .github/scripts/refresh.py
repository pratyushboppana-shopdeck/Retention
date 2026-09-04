#!/usr/bin/env python3
"""Refresh data.json from Metabase (BigQuery), standard library only.

Produces two datasets on the canonical go-live >1000 base:
  * curve  -> per weekly-cohort W1..W7 3K-retention (feeds the chart)
  * diag   -> per-seller diagnosis for all charted cohorts (Nov-2025 onward; feeds the drill-down):
              weekly spend sp1..sp7, reason bucket, and current GC/GM/KAM (card 7753)
Reason buckets follow the agreed framework (cards 11435/11610/12049/12206), precedence
1->7, median cutoffs for performance & RTO. Only rewrites data.json when the data changes.
"""
import json, os, re, sys, urllib.request, urllib.error, urllib.parse, csv as csvmod, io
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
  HAVING MIN(start_date) >= DATE '2025-11-01'
     AND DATE_TRUNC(MIN(start_date),ISOWEEK) < DATE_TRUNC(CURRENT_DATE(),ISOWEEK)
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
SELECT c.golive_iso_week AS Cohort, c.golives AS `go-lives`, c.weeks_elapsed AS weeks_elapsed,
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
  HAVING MIN(start_date) >= DATE '2025-11-01'
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
  WHERE DATE(oi.createdat,'Asia/Kolkata')>=DATE '2025-10-15'
    AND oi.seller_last_status NOT IN ('initiated','enqueued','invalid') AND oi.awb_no!='None' AND oi.in_house_status!='awb_expired'
    AND DATE_DIFF(DATE(oi.createdat,'Asia/Kolkata'),g.golive_date,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1
),
fb AS (
  SELECT g.seller_id, SUM(f.spend) fb_sp, SUM(f.impressions) imp, SUM(f.clicks) clk
  FROM golive g JOIN fb_marketings.fb_marketing_insights f ON f.seller_id=g.seller_id
  WHERE f.breakdown_key IS NULL AND DATE(f.spend_date,'Asia/Kolkata')>=DATE '2025-10-15'
    AND DATE_DIFF(DATE(f.spend_date,'Asia/Kolkata'),g.golive_date,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1
),
-- Ad-account block: a blocking ticket created <= W3 that is NOT resolved by W3.
-- Resolution = status 'completed' AND completed within W3. status 'pending'/'closed' = unresolved
-- (per business rule). The FB fb_ad_account_block_history (card 12049) is NOT used as a driver
-- here: it flags DISABLED even for sellers actively spending (stale/secondary-account noise).
adtix AS (
  SELECT g.seller_id,1 ad_impact FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE t.source='crm_initiated' AND t.created_by IS NOT NULL AND DATE(t.created_at)>=DATE '2025-09-01'
    AND t.sub_type IN ('ad_account_suspension','ad_account_blocked','business_manager_verification','business_manager_restricted','pixel_inactive','page_restricted','ad_account_hacked','business_manager_access','account_restricted','account_permanently_restricted','page_unpublished','ad_account_has_limit')
    AND DATE_DIFF(DATE(t.created_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)<=3
    AND (t.status!='completed' OR DATE_DIFF(DATE(t.completed_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)>3) GROUP BY 1
),
paytix AS (
  SELECT g.seller_id,1 pay_impact FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE t.source='crm_initiated' AND t.created_by IS NOT NULL AND DATE(t.created_at)>=DATE '2025-09-01'
    AND t.sub_type IN ('payment_failed','transactions_failure','change_payment_method')
    AND DATE_DIFF(DATE(t.created_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)<=3
    AND (t.status!='completed' OR DATE_DIFF(DATE(t.completed_at,'Asia/Kolkata'),g.golive_date,ISOWEEK)>3) GROUP BY 1
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
    COALESCE(NULLIF(m.gc,''),'-') gc, COALESCE(NULLIF(m.gm,''),'-') gm, COALESCE(NULLIF(m.kam,''),'-') kam
  FROM golive g LEFT JOIN sw USING(seller_id) LEFT JOIN ord o USING(seller_id) LEFT JOIN fb USING(seller_id)
  LEFT JOIN adtix a USING(seller_id) LEFT JOIN paytix p USING(seller_id) LEFT JOIN mgr m USING(seller_id)
),
med AS (SELECT APPROX_QUANTILES(sgmv,2)[OFFSET(1)] m_sgmv, APPROX_QUANTILES(rto_rate,2)[OFFSET(1)] m_rto FROM base WHERE w3_mature=1 AND retained_w3=0)
SELECT b.seller_id, b.gw AS golive_iso_week, b.sp1,b.sp2,b.sp3,b.sp4,b.sp5,b.sp6,b.sp7,
  CASE
    WHEN b.ad_impact=1 THEN 'Ad-account / platform block'
    WHEN b.pay_impact=1 THEN 'Payment / funding block'
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


def run_card_csv(session, card_id):
    """Run a saved Metabase card, return CSV text. Used for the incentive-team
    rosters, which live in cards rather than in BigQuery."""
    req = urllib.request.Request(MB_URL + f"/api/card/{card_id}/query/csv", data=b"", method="POST")
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


# Go-live GC/GM (point-in-time, card 11381 attribution) x W1-W7 3K retention (>1000 base),
# aggregated per (go-live ISO week, role, person). Feeds the "GC & GM retention" tab.
GCGM_SQL = r"""
WITH
total_spend AS (
  SELECT seller_id, DATE(date) spend_date, spend/1.18 spend FROM `nushop.marketing_spends`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(),INTERVAL 9000 DAY) AND DATE(date) <= DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) AND marketing_channel!='whatsapp'
  UNION ALL SELECT seller_id, spend_date, spend FROM `nushop.google_marketing_insights_master` WHERE breakdown_key IS NULL AND spend_date >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
  UNION ALL SELECT seller_id, DATE(spend_date,"Asia/Kolkata"), spend FROM `fb_marketings.fb_marketing_insights` WHERE breakdown_key IS NULL AND DATE(spend_date,"Asia/Kolkata") >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
),
daily_spend AS (SELECT seller_id, spend_date, SUM(spend) marketing_spend FROM total_spend GROUP BY 1,2),
go_live AS (SELECT seller_id, MIN(spend_date) go_live_date FROM daily_spend WHERE marketing_spend>=100 GROUP BY 1),
sellers AS (SELECT * FROM go_live WHERE go_live_date >= DATE '2026-01-01'),
ev AS (SELECT DISTINCT seller_id, CASE WHEN subcategory LIKE '%growth_consultant%' THEN 'GC' ELSE 'GM' END role,
   IF(REGEXP_CONTAINS(initial_value,r'^[0-9a-f]{24}$'),initial_value,NULL) prev_id,
   IF(REGEXP_CONTAINS(final_value,r'^[0-9a-f]{24}$'),final_value,NULL) new_id, createdat
  FROM nushop.changeslogs WHERE createdat >= TIMESTAMP('2025-07-01') AND (subcategory LIKE '%growth_consultant%' OR subcategory LIKE '%growth_manager%') AND seller_id IN (SELECT seller_id FROM sellers)),
ev2 AS (SELECT * FROM ev WHERE prev_id IS NOT NULL OR new_id IS NOT NULL),
cutoffs AS (SELECT seller_id, TIMESTAMP(go_live_date,'Asia/Kolkata') cutoff, TIMESTAMP(DATE_ADD(go_live_date,INTERVAL 1 DAY),'Asia/Kolkata') grace_end FROM sellers),
ce AS (SELECT c.seller_id evt_key, c.cutoff, c.grace_end, e.role, e.prev_id, e.new_id, e.createdat FROM cutoffs c JOIN ev2 e USING(seller_id)),
b AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC, new_id IS NULL) rn FROM ce WHERE createdat < cutoff) WHERE rn=1),
b_held AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC) rn FROM ce WHERE createdat<cutoff AND new_id IS NOT NULL) WHERE rn=1),
a AS (SELECT evt_key, role, COALESCE(prev_id, IF(createdat<grace_end,new_id,NULL)) person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat ASC, prev_id IS NULL) rn FROM ce WHERE createdat>=cutoff) WHERE rn=1),
resolved AS (SELECT k.evt_key, k.role, COALESCE(b.person_id, IF(b.evt_key IS NULL, a.person_id, NULL), bh.person_id) person_id
  FROM (SELECT DISTINCT evt_key, role FROM ce UNION DISTINCT SELECT seller_id, r FROM cutoffs CROSS JOIN UNNEST(['GC','GM']) r) k
  LEFT JOIN b USING(evt_key,role) LEFT JOIN b_held bh USING(evt_key,role) LEFT JOIN a USING(evt_key,role)),
gl AS (SELECT s.seller_id, gc.person_id golive_gc_id, gm.person_id golive_gm_id FROM sellers s
  LEFT JOIN resolved gc ON gc.evt_key=s.seller_id AND gc.role='GC' LEFT JOIN resolved gm ON gm.evt_key=s.seller_id AND gm.role='GM'),
mygl AS (SELECT seller_id, MIN(start_date) gd, FORMAT_DATE('%G-W%V',MIN(start_date)) gw FROM nushop.gc_view_3 WHERE marketing_spend>1000 AND team_mapping='HIT' GROUP BY 1 HAVING MIN(start_date)>=DATE '2026-01-01'),
wk AS (SELECT m.seller_id, m.gw, DATE_DIFF(CURRENT_DATE(), m.gd, ISOWEEK) we,
   SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=1,v.marketing_spend,0)) s1, SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=2,v.marketing_spend,0)) s2,
   SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=3,v.marketing_spend,0)) s3, SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=4,v.marketing_spend,0)) s4,
   SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=5,v.marketing_spend,0)) s5, SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=6,v.marketing_spend,0)) s6,
   SUM(IF(DATE_DIFF(v.start_date,m.gd,ISOWEEK)=7,v.marketing_spend,0)) s7
   FROM mygl m JOIN nushop.gc_view_3 v ON v.seller_id=m.seller_id GROUP BY 1,2,3),
enr AS (SELECT w.gw, w.we, IF(w.s1>=3000,1,0) r1,IF(w.s2>=3000,1,0) r2,IF(w.s3>=3000,1,0) r3,IF(w.s4>=3000,1,0) r4,IF(w.s5>=3000,1,0) r5,IF(w.s6>=3000,1,0) r6,IF(w.s7>=3000,1,0) r7,
   NULLIF(TRIM(CONCAT(COALESCE(u1.first_name,''),' ',COALESCE(u1.last_name,''))),'') gc,
   NULLIF(TRIM(CONCAT(COALESCE(u2.first_name,''),' ',COALESCE(u2.last_name,''))),'') gm
   FROM wk w LEFT JOIN gl ON gl.seller_id=w.seller_id LEFT JOIN nushop.users u1 ON gl.golive_gc_id=u1._id LEFT JOIN nushop.users u2 ON gl.golive_gm_id=u2._id)
SELECT gw, MAX(we) we, 'GC' role, gc name, COUNT(*) n, SUM(r1) r1,SUM(r2) r2,SUM(r3) r3,SUM(r4) r4,SUM(r5) r5,SUM(r6) r6,SUM(r7) r7 FROM enr WHERE gc IS NOT NULL GROUP BY gw,gc
UNION ALL
SELECT gw, MAX(we) we, 'GM' role, gm name, COUNT(*) n, SUM(r1),SUM(r2),SUM(r3),SUM(r4),SUM(r5),SUM(r6),SUM(r7) FROM enr WHERE gm IS NOT NULL GROUP BY gw,gm
ORDER BY role, gw, name
"""


def parse_gcgm(csv_text):
    rows = list(csvmod.DictReader(io.StringIO(csv_text)))
    weeks, gc, gm = {}, [], []
    for r in rows:
        gw = r["gw"]
        try:
            weeks[gw] = int(r["we"])
        except (TypeError, ValueError):
            continue
        rec = [gw, (r["name"] or "").strip(), int(r["n"])] + [int(r["r%d" % k]) for k in range(1, 8)]
        (gc if r["role"] == "GC" else gm).append(rec)
    return {"weeks": weeks, "gc": gc, "gm": gm}


# Per-seller go-live GC/GM + OPEN POINTERS (card 10550 tasks, blocking types only, + SOS),
# last 3 months of go-lives. Feeds the GC/GM tab's seller drill-down.
GCGMS_SQL = r"""
WITH
total_spend AS (
  SELECT seller_id, DATE(date) spend_date, spend/1.18 spend FROM `nushop.marketing_spends`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(),INTERVAL 9000 DAY) AND DATE(date) <= DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) AND marketing_channel!='whatsapp'
  UNION ALL SELECT seller_id, spend_date, spend FROM `nushop.google_marketing_insights_master` WHERE breakdown_key IS NULL AND spend_date >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
  UNION ALL SELECT seller_id, DATE(spend_date,"Asia/Kolkata"), spend FROM `fb_marketings.fb_marketing_insights` WHERE breakdown_key IS NULL AND DATE(spend_date,"Asia/Kolkata") >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
),
daily_spend AS (SELECT seller_id, spend_date, SUM(spend) marketing_spend FROM total_spend GROUP BY 1,2),
go_live AS (SELECT seller_id, MIN(spend_date) go_live_date FROM daily_spend WHERE marketing_spend>=100 GROUP BY 1),
sellers AS (SELECT * FROM go_live WHERE go_live_date >= DATE '2026-01-01'),
ev AS (SELECT DISTINCT seller_id, CASE WHEN subcategory LIKE '%growth_consultant%' THEN 'GC' ELSE 'GM' END role,
   IF(REGEXP_CONTAINS(initial_value,r'^[0-9a-f]{24}$'),initial_value,NULL) prev_id,
   IF(REGEXP_CONTAINS(final_value,r'^[0-9a-f]{24}$'),final_value,NULL) new_id, createdat
  FROM nushop.changeslogs WHERE createdat >= TIMESTAMP('2025-07-01') AND (subcategory LIKE '%growth_consultant%' OR subcategory LIKE '%growth_manager%') AND seller_id IN (SELECT seller_id FROM sellers)),
ev2 AS (SELECT * FROM ev WHERE prev_id IS NOT NULL OR new_id IS NOT NULL),
cutoffs AS (SELECT seller_id, TIMESTAMP(go_live_date,'Asia/Kolkata') cutoff, TIMESTAMP(DATE_ADD(go_live_date,INTERVAL 1 DAY),'Asia/Kolkata') grace_end FROM sellers),
ce AS (SELECT c.seller_id evt_key, c.cutoff, c.grace_end, e.role, e.prev_id, e.new_id, e.createdat FROM cutoffs c JOIN ev2 e USING(seller_id)),
b AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC, new_id IS NULL) rn FROM ce WHERE createdat < cutoff) WHERE rn=1),
b_held AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC) rn FROM ce WHERE createdat<cutoff AND new_id IS NOT NULL) WHERE rn=1),
a AS (SELECT evt_key, role, COALESCE(prev_id, IF(createdat<grace_end,new_id,NULL)) person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat ASC, prev_id IS NULL) rn FROM ce WHERE createdat>=cutoff) WHERE rn=1),
resolved AS (SELECT k.evt_key, k.role, COALESCE(b.person_id, IF(b.evt_key IS NULL, a.person_id, NULL), bh.person_id) person_id
  FROM (SELECT DISTINCT evt_key, role FROM ce UNION DISTINCT SELECT seller_id, r FROM cutoffs CROSS JOIN UNNEST(['GC','GM']) r) k
  LEFT JOIN b USING(evt_key,role) LEFT JOIN b_held bh USING(evt_key,role) LEFT JOIN a USING(evt_key,role)),
gl AS (SELECT s.seller_id, gc.person_id golive_gc_id, gm.person_id golive_gm_id FROM sellers s
  LEFT JOIN resolved gc ON gc.evt_key=s.seller_id AND gc.role='GC' LEFT JOIN resolved gm ON gm.evt_key=s.seller_id AND gm.role='GM'),
mygl AS (SELECT seller_id, MIN(start_date) gd FROM nushop.gc_view_3 WHERE marketing_spend>1000 AND team_mapping='HIT' GROUP BY 1
  HAVING DATE_TRUNC(MIN(start_date),ISOWEEK) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 MONTH),ISOWEEK)),
tasks AS (
  SELECT DISTINCT t.id, t.seller_id, t.type, t.sub_type, t.status, DATE(t.created_at,'Asia/Kolkata') cdate,
    CASE WHEN LOWER(u.role) LIKE '%growth-consultant%' THEN 'GC'
         WHEN LOWER(u.role) LIKE '%key-account-manager%' THEN 'KAM'
         WHEN LOWER(u.role) LIKE '%growth-manager%' THEN 'GM'
         WHEN LOWER(u.role) LIKE '%account-manager%' THEN 'AM'
         WHEN LOWER(u.role) LIKE '%category-lead%' THEN 'CL'
         WHEN LOWER(u.role) LIKE '%finance-manager%' THEN 'FM'
         WHEN t.assignee IS NULL THEN 'Unassigned' ELSE 'Ops' END abkt,
    IF(t.type IN ('facebook_ad_account','business_manager','facebook_page','pixel','suspension',
       'access_management','access_management_google','payment','postpaid','shopdeck_postpaid','shopdeck_prepaid',
       'catalogue','campaign','create_assets','data_flow_mismatch','personal_facebook_account',
       'hit_seller_shipping_issues','leadership_support_escalation','account_dashboard','seller_request_google'),1,0) is_block
  FROM nushop.workboard_tasks t LEFT JOIN nushop.users u ON t.assignee=u._id
  -- UTC bounds first: they match PARTITION BY DATE(created_at) and prune. The IST
  -- pair below is what actually defines the window; it cannot prune on its own.
  WHERE DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 201 DAY)
    AND DATE(t.created_at) <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(t.created_at,'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 200 DAY)
    AND DATE(t.created_at,'Asia/Kolkata') <= CURRENT_DATE('Asia/Kolkata')
),
open_t AS (
  SELECT t.seller_id, t.sub_type, t.status, t.abkt, t.is_block,
    DATE_DIFF(CURRENT_DATE(), t.cdate, DAY) age, IF(t.status='pending',0,1) pri
  FROM mygl m JOIN tasks t ON t.seller_id=m.seller_id
  WHERE t.status!='completed' AND t.cdate >= DATE_SUB(m.gd, INTERVAL 14 DAY)
),
tx AS (
  SELECT seller_id,
    STRING_AGG(IF(is_block=1, CONCAT(sub_type,'|',status,'|',abkt,'|',CAST(age AS STRING)), NULL), ';' ORDER BY pri, age LIMIT 5) ops,
    SUM(is_block) n_block, SUM(IF(is_block=1 AND status='pending',1,0)) n_block_pending,
    COUNT(*) n_all
  FROM open_t GROUP BY seller_id
),
sos AS (SELECT m.seller_id, COUNT(*) n_sos, SUBSTR(ANY_VALUE(r.comment),0,110) sos_text
  FROM mygl m JOIN nushop.seller_app_requests r ON r.seller_id=m.seller_id
  WHERE r.request_type IN ('sos','leadership_escalation')
    AND DATE(r.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 200 DAY)
    AND DATE(r.created_at) >= DATE_SUB(m.gd, INTERVAL 14 DAY)
  GROUP BY 1)
SELECT m.seller_id,
  NULLIF(TRIM(CONCAT(COALESCE(u1.first_name,''),' ',COALESCE(u1.last_name,''))),'') golive_gc,
  NULLIF(TRIM(CONCAT(COALESCE(u2.first_name,''),' ',COALESCE(u2.last_name,''))),'') golive_gm,
  COALESCE(tx.n_block,0) n_open, COALESCE(tx.n_block_pending,0) n_pending, COALESCE(tx.ops,'') ops,
  COALESCE(tx.n_all,0) n_all,
  COALESCE(sos.n_sos,0) n_sos, COALESCE(sos.sos_text,'') sos_text
FROM mygl m
LEFT JOIN gl ON gl.seller_id=m.seller_id
LEFT JOIN nushop.users u1 ON gl.golive_gc_id=u1._id
LEFT JOIN nushop.users u2 ON gl.golive_gm_id=u2._id
LEFT JOIN tx ON tx.seller_id=m.seller_id
LEFT JOIN sos ON sos.seller_id=m.seller_id
ORDER BY m.seller_id
"""


def parse_gcgms(csv_text):
    """names[] + compact rows: [seller, gcIdx, gmIdx, nBlock, nBlockPending, ops, nSos, sosText, nAll]"""
    rows = list(csvmod.DictReader(io.StringIO(csv_text)))
    names, nidx, out = [], {}, []

    def ni(x):
        x = (x or "").strip()
        if not x:
            return -1
        if x not in nidx:
            nidx[x] = len(names); names.append(x)
        return nidx[x]

    for r in rows:
        gc, gm = ni(r.get("golive_gc")), ni(r.get("golive_gm"))
        if gc < 0 and gm < 0:
            continue                      # not selectable by GC or GM
        def gi(k):
            try:
                return int(float(r.get(k) or 0))
            except ValueError:
                return 0
        out.append([r["seller_id"], gc, gm, gi("n_open"), gi("n_pending"),
                    (r.get("ops") or "")[:400], gi("n_sos"), (r.get("sos_text") or "")[:110], gi("n_all")])
    return {"names": names, "rows": out}


# TS SOP task + call metrics per (assignee, ISO week), with GM rollup key (card 12960).
# Rows carry SUMS so any week/person aggregation in the UI stays exact.
TSSOP_SQL = r"""
WITH gm_map AS (
  SELECT poc_id, gm_id, gm_name FROM (
    SELECT poc_id, gm_id, gm_name, ROW_NUMBER() OVER (PARTITION BY poc_id ORDER BY COUNT(*) DESC, gm_id) rn FROM (
      SELECT gc_id poc_id, gm_id, gm_name FROM `blitzscale-prod-project.analytics.seller_console_metrics_summary` WHERE gc_id IS NOT NULL AND gm_id IS NOT NULL
      UNION ALL SELECT kam_id, gm_id, gm_name FROM `blitzscale-prod-project.analytics.seller_console_metrics_summary` WHERE kam_id IS NOT NULL AND gm_id IS NOT NULL
      UNION ALL SELECT key_account_executive_id, gm_id, gm_name FROM `blitzscale-prod-project.analytics.seller_console_metrics_summary` WHERE key_account_executive_id IS NOT NULL AND gm_id IS NOT NULL
    ) GROUP BY poc_id, gm_id, gm_name) WHERE rn=1),
enriched AS (
  SELECT t.id, t.assignee, CONCAT(u.first_name,' ',u.last_name) AS assignee_name,
    FORMAT_DATE('%G-W%V', DATE(DATETIME(t.completion_date,'Asia/Kolkata'))) AS year_week,
    CASE WHEN t.status='completed' THEN 1 ELSE 0 END AS is_completed,
    CASE WHEN t.status='completed' AND t.completed_at IS NOT NULL AND t.completion_date IS NOT NULL
      AND DATETIME(t.completed_at,'Asia/Kolkata') <= DATETIME(t.completion_date,'Asia/Kolkata') THEN 1 ELSE 0 END AS is_sla_met,
    CASE
      WHEN LOWER(u.role) LIKE '%growth-consultant%' THEN
        CASE WHEN t.assignee IN ('665808d287d33ad7792197ac','64bab294f16a3b0012bfd92d','6989b58ffd2a43b9aa331060','6953c1772582b77a1cce9104','6953c18e6c33c17b98290e6e') THEN 'hc_gc'
             WHEN t.assignee IN ('67c6bb3a5433937ca56affab','625029514d863202de34887e','6a041aaf34455a21f5fd5e44') THEN 'revival_gc'
             ELSE 'gc' END
      WHEN LOWER(u.role) LIKE '%key-account-manager%' THEN 'kam'
      WHEN LOWER(u.role) LIKE '%growth-manager%' THEN 'gm'
      WHEN LOWER(u.role) LIKE '%category-lead%' THEN 'cl'
      WHEN LOWER(u.role) LIKE '%marketing-operations%' OR LOWER(u.role) LIKE '%markops%' THEN 'markops'
      WHEN LOWER(u.role) LIKE '%growth-lead%' THEN 'gl'
      WHEN u.role IS NULL THEN 'unknown' ELSE 'other' END AS assignee_bucket
  FROM nushop.workboard_tasks t LEFT JOIN nushop.users u ON t.assignee=u._id
  WHERE DATE(t.created_at) >= '2026-01-01' AND t.source='troubleshoot_action' AND t.sub_type='troubleshoot_sop'),
call_agg AS (
  SELECT e.assignee, e.year_week,
    COUNT(*) calls_attempted, COUNTIF(d.duration>0) calls_connected,
    COUNTIF(q.acc IS NOT NULL OR q.sat IS NOT NULL OR q.ton IS NOT NULL) calls_scored,
    SUM(IF(d.duration>0,d.duration,0)) dur_sum,
    SUM(q.acc) acc_sum, COUNTIF(q.acc IS NOT NULL) acc_n,
    SUM(q.sat) sat_sum, COUNTIF(q.sat IS NOT NULL) sat_n,
    SUM(q.ton) ton_sum, COUNTIF(q.ton IS NOT NULL) ton_n
  FROM enriched e
  JOIN nushop.exotel_calls c ON c.entity_id=e.id AND c.entity='workboard' AND c.created_at >= TIMESTAMP('2025-12-25')
  JOIN nushop.exotel_call_details d ON d.sid=c.exotel_call_sid AND d.created_at >= TIMESTAMP('2025-12-25')
  CROSS JOIN UNNEST([STRUCT(
    NULLIF(SAFE_CAST(JSON_VALUE(d.call_quality_score,'$.accuracy_of_answers') AS FLOAT64),-1) AS acc,
    NULLIF(SAFE_CAST(JSON_VALUE(d.call_quality_score,'$.seller_satisfaction') AS FLOAT64),-1) AS sat,
    NULLIF(SAFE_CAST(JSON_VALUE(d.call_quality_score,'$.tonality_and_communication') AS FLOAT64),-1) AS ton)]) q
  GROUP BY 1,2)
SELECT e.assignee_name, COALESCE(g.gm_name,'(no GM mapped)') gm_name, e.assignee_bucket, e.year_week,
  COUNT(*) tasks, SUM(e.is_completed) completed, SUM(e.is_sla_met) sla_met,
  COALESCE(ANY_VALUE(ca.calls_attempted),0) attempted, COALESCE(ANY_VALUE(ca.calls_connected),0) connected,
  COALESCE(ANY_VALUE(ca.calls_scored),0) scored, COALESCE(ANY_VALUE(ca.dur_sum),0) dur_sum,
  COALESCE(ANY_VALUE(ca.acc_sum),0) acc_sum, COALESCE(ANY_VALUE(ca.acc_n),0) acc_n,
  COALESCE(ANY_VALUE(ca.sat_sum),0) sat_sum, COALESCE(ANY_VALUE(ca.sat_n),0) sat_n,
  COALESCE(ANY_VALUE(ca.ton_sum),0) ton_sum, COALESCE(ANY_VALUE(ca.ton_n),0) ton_n
FROM enriched e
LEFT JOIN call_agg ca ON ca.assignee=e.assignee AND ca.year_week=e.year_week
LEFT JOIN gm_map g ON g.poc_id=e.assignee
WHERE e.year_week IS NOT NULL
  AND CAST(REPLACE(e.year_week,'-W','') AS INT64) <= CAST(FORMAT_DATE('%G%V', CURRENT_DATE('Asia/Kolkata')) AS INT64)
GROUP BY 1,2,3,4 ORDER BY 3,1,4
"""

# Go-lives and unassignments per (ISO week, role, person) — point-in-time GC/GM (card 12438).
UNASSIGN_SQL = r"""
WITH
total_spend AS (
  SELECT seller_id, DATE(date) spend_date, spend/1.18 spend FROM `nushop.marketing_spends`
    WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(),INTERVAL 9000 DAY) AND DATE(date) <= DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) AND marketing_channel!='whatsapp'
  UNION ALL SELECT seller_id, spend_date, spend FROM `nushop.google_marketing_insights_master` WHERE breakdown_key IS NULL AND spend_date >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
  UNION ALL SELECT seller_id, DATE(spend_date,"Asia/Kolkata"), spend FROM `fb_marketings.fb_marketing_insights` WHERE breakdown_key IS NULL AND DATE(spend_date,"Asia/Kolkata") >= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
),
daily_spend AS (SELECT seller_id, spend_date, SUM(spend) marketing_spend FROM total_spend GROUP BY 1,2),
go_live AS (SELECT seller_id, MIN(spend_date) go_live_date FROM daily_spend WHERE marketing_spend>=100 GROUP BY 1),
sellers AS (SELECT * FROM go_live WHERE go_live_date >= DATE '2026-01-01'),
unassign AS (
  SELECT seller_id, id revival_task_id, DATE(created_at,'Asia/Kolkata') unassignment_date,
    ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY created_at) cycle_no
  FROM nushop.workboard_tasks
  WHERE type='seller_revival' AND sub_type='seller_revival_primary_task' AND DATE(created_at,'Asia/Kolkata') <= CURRENT_DATE()),
ev AS (SELECT DISTINCT seller_id, CASE WHEN subcategory LIKE '%growth_consultant%' THEN 'GC' ELSE 'GM' END role,
   IF(REGEXP_CONTAINS(initial_value,r'^[0-9a-f]{24}$'),initial_value,NULL) prev_id,
   IF(REGEXP_CONTAINS(final_value,r'^[0-9a-f]{24}$'),final_value,NULL) new_id, createdat
  FROM nushop.changeslogs WHERE createdat >= TIMESTAMP('2025-07-01')
    AND (subcategory LIKE '%growth_consultant%' OR subcategory LIKE '%growth_manager%') AND seller_id IN (SELECT seller_id FROM sellers)),
ev2 AS (SELECT * FROM ev WHERE prev_id IS NOT NULL OR new_id IS NOT NULL),
cutoffs AS (
  SELECT seller_id, 'GOLIVE' evt, CAST(NULL AS STRING) revival_task_id, go_live_date evt_date,
    TIMESTAMP(go_live_date,'Asia/Kolkata') cutoff, TIMESTAMP(DATE_ADD(go_live_date,INTERVAL 1 DAY),'Asia/Kolkata') grace_end
  FROM sellers
  UNION ALL
  SELECT u.seller_id,'UNASSIGN',u.revival_task_id,u.unassignment_date,
    TIMESTAMP(u.unassignment_date,'Asia/Kolkata'), TIMESTAMP(u.unassignment_date,'Asia/Kolkata')
  FROM unassign u JOIN sellers s USING (seller_id)),
ce AS (SELECT CONCAT(c.evt,'|',c.seller_id,'|',IFNULL(c.revival_task_id,'')) evt_key, c.cutoff, c.grace_end, e.role, e.prev_id, e.new_id, e.createdat
  FROM cutoffs c JOIN ev2 e USING (seller_id)),
b AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC, new_id IS NULL) rn FROM ce WHERE createdat<cutoff) WHERE rn=1),
b_held AS (SELECT evt_key, role, new_id person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat DESC) rn FROM ce WHERE createdat<cutoff AND new_id IS NOT NULL) WHERE rn=1),
a AS (SELECT evt_key, role, COALESCE(prev_id, IF(createdat<grace_end,new_id,NULL)) person_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY evt_key,role ORDER BY createdat ASC, prev_id IS NULL) rn FROM ce WHERE createdat>=cutoff) WHERE rn=1),
resolved AS (SELECT k.evt_key, k.role, COALESCE(b.person_id, IF(b.evt_key IS NULL, a.person_id, NULL), bh.person_id) person_id
  FROM (SELECT DISTINCT evt_key, role FROM ce UNION DISTINCT SELECT CONCAT(evt,'|',seller_id,'|',IFNULL(revival_task_id,'')), r FROM cutoffs CROSS JOIN UNNEST(['GC','GM']) r) k
  LEFT JOIN b USING (evt_key,role) LEFT JOIN b_held bh USING (evt_key,role) LEFT JOIN a USING (evt_key,role)),
golive_ev AS (
  SELECT s.seller_id, s.go_live_date evt_date, 'golive' kind, gc.person_id gc_id, gm.person_id gm_id
  FROM sellers s
  LEFT JOIN resolved gc ON gc.evt_key=CONCAT('GOLIVE|',s.seller_id,'|') AND gc.role='GC'
  LEFT JOIN resolved gm ON gm.evt_key=CONCAT('GOLIVE|',s.seller_id,'|') AND gm.role='GM'),
unassign_ev AS (
  SELECT u.seller_id, u.unassignment_date evt_date, 'unassign' kind, gc.person_id gc_id, gm.person_id gm_id
  FROM unassign u JOIN sellers s USING (seller_id)
  LEFT JOIN resolved gc ON gc.evt_key=CONCAT('UNASSIGN|',u.seller_id,'|',u.revival_task_id) AND gc.role='GC'
  LEFT JOIN resolved gm ON gm.evt_key=CONCAT('UNASSIGN|',u.seller_id,'|',u.revival_task_id) AND gm.role='GM'),
allev AS (SELECT * FROM golive_ev UNION ALL SELECT * FROM unassign_ev),
long AS (
  SELECT FORMAT_DATE('%G-W%V', evt_date) year_week, kind, 'GC' role, gc_id person_id FROM allev
  UNION ALL
  SELECT FORMAT_DATE('%G-W%V', evt_date), kind, 'GM', gm_id FROM allev)
SELECT l.year_week, l.role, l.kind,
  COALESCE(NULLIF(TRIM(CONCAT(COALESCE(u.first_name,''),' ',COALESCE(u.last_name,''))),''),'(unattributed)') person,
  COUNT(*) n
FROM long l LEFT JOIN nushop.users u ON u._id=l.person_id
GROUP BY 1,2,3,4 ORDER BY 1,2,3,4
"""


def parse_tssop(csv_text):
    rows = list(csvmod.DictReader(io.StringIO(csv_text)))
    names, nidx, out = [], {}, []

    def ni(x):
        x = (x or "").strip() or "(unknown)"
        if x not in nidx:
            nidx[x] = len(names); names.append(x)
        return nidx[x]

    def i(v):
        try: return int(float(v or 0))
        except ValueError: return 0

    def f(v):
        try: return round(float(v or 0), 1)
        except ValueError: return 0

    for r in rows:
        out.append([ni(r["assignee_name"]), ni(r["gm_name"]), r["assignee_bucket"], r["year_week"],
                    i(r["tasks"]), i(r["completed"]), i(r["sla_met"]), i(r["attempted"]), i(r["connected"]),
                    i(r["scored"]), i(r["dur_sum"]), f(r["acc_sum"]), i(r["acc_n"]),
                    f(r["sat_sum"]), i(r["sat_n"]), f(r["ton_sum"]), i(r["ton_n"])])
    return {"names": names, "rows": out}


def parse_unassign(csv_text):
    rows = list(csvmod.DictReader(io.StringIO(csv_text)))
    names, nidx, out = [], {}, []

    def ni(x):
        x = (x or "").strip() or "(unattributed)"
        if x not in nidx:
            nidx[x] = len(names); names.append(x)
        return nidx[x]

    for r in rows:
        try: n = int(float(r.get("n") or 0))
        except ValueError: n = 0
        out.append([r["year_week"], r["role"], r["kind"], ni(r["person"]), n])
    return {"names": names, "rows": out}


# Actual underlying metrics per go-live ISO week (RTO rate, spend/GMV) — the two
# "actual metric" rows under the reason matrix. These are NOT % of go-lives.
WEEKACT_SQL = r"""
WITH golive AS (
  SELECT seller_id, MIN(start_date) gd, FORMAT_DATE('%G-W%V', MIN(start_date)) gw
  FROM nushop.gc_view_3 WHERE marketing_spend>1000 AND team_mapping='HIT' GROUP BY 1
  HAVING MIN(start_date) >= DATE '2025-11-01'),
sw AS (
  SELECT g.seller_id, g.gw,
    SUM(IF(DATE_DIFF(v.start_date,g.gd,ISOWEEK) BETWEEN 0 AND 3, v.rtos,0)) rto03,
    SUM(IF(DATE_DIFF(v.start_date,g.gd,ISOWEEK) BETWEEN 0 AND 3, v.total_orders,0)) ord03
  FROM golive g JOIN nushop.gc_view_3 v ON v.seller_id=g.seller_id GROUP BY 1,2),
o AS (
  SELECT g.seller_id,
    SUM(oi.selling_price*oi.quantity+oi.cod_charge+oi.delivery_fees-oi.total_discount) gmv02
  FROM golive g JOIN nushop.orderitems oi ON oi.seller_id=g.seller_id
  WHERE DATE(oi.createdat,'Asia/Kolkata')>=DATE '2025-10-01'
    AND oi.seller_last_status NOT IN ('initiated','enqueued','invalid') AND oi.awb_no!='None' AND oi.in_house_status!='awb_expired'
    AND DATE_DIFF(DATE(oi.createdat,'Asia/Kolkata'),g.gd,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1),
f AS (
  SELECT g.seller_id, SUM(fb.spend) fb02
  FROM golive g JOIN fb_marketings.fb_marketing_insights fb ON fb.seller_id=g.seller_id
  WHERE fb.breakdown_key IS NULL AND DATE(fb.spend_date,'Asia/Kolkata')>=DATE '2025-10-01'
    AND DATE_DIFF(DATE(fb.spend_date,'Asia/Kolkata'),g.gd,ISOWEEK) BETWEEN 0 AND 2 GROUP BY 1),
s AS (
  SELECT sw.gw, sw.seller_id, SAFE_DIVIDE(sw.rto03,sw.ord03) rto_rate, sw.rto03, sw.ord03,
    SAFE_DIVIDE(f.fb02,o.gmv02) sgmv, f.fb02, o.gmv02
  FROM sw LEFT JOIN o USING(seller_id) LEFT JOIN f USING(seller_id))
SELECT gw AS year_week,
  COUNT(*) n,
  ROUND(100*APPROX_QUANTILES(rto_rate,2)[OFFSET(1)],1) rto_median,
  ROUND(100*SAFE_DIVIDE(SUM(rto03),SUM(ord03)),1) rto_agg,
  ROUND(APPROX_QUANTILES(sgmv,2)[OFFSET(1)],3) sgmv_median,
  ROUND(SAFE_DIVIDE(SUM(fb02),SUM(gmv02)),3) sgmv_agg
FROM s GROUP BY gw ORDER BY gw
"""


def parse_weekact(csv_text):
    out = {}
    for r in csvmod.DictReader(io.StringIO(csv_text)):
        def f(k):
            try:
                v = r.get(k)
                return float(v) if v not in ("", None) else None
            except ValueError:
                return None
        try:
            n = int(float(r.get("n") or 0))
        except ValueError:
            n = 0
        if n < 5:              # skip trivially small weeks
            continue
        out[r["year_week"]] = {"rto": f("rto_median"), "rtoAgg": f("rto_agg"),
                               "sgmv": f("sgmv_median"), "sgmvAgg": f("sgmv_agg"), "n": n}
    return out


# Task SLA adherence by creation day / task type / assignee, for GC and KAM.
# Anchored on CREATION date because the SLA clock starts there, which also lets a
# task that was never completed and is now past SLA count as a breach ("stuck")
# rather than vanishing from the denominator the way a completion-date view does.
TASKSLA_SQL = r"""
WITH base AS (
  SELECT t.id, t.type, t.sub_type, t.title, t.status AS task_status,
    t.created_at, t.completed_at, t.sla_in_min, t.completion_date,
    DATE(t.created_at,'Asia/Kolkata') AS cd,
    REGEXP_REPLACE(TRIM(CONCAT(COALESCE(u.first_name,''),' ',COALESCE(u.last_name,''))),r'\s+',' ') AS nm,
    LOWER(TRIM(u.email)) AS em,
    CASE WHEN LOWER(u.role) LIKE '%growth-consultant%'   THEN 'GC'
         WHEN LOWER(u.role) LIKE '%key-account-manager%' THEN 'KAM' END AS role
  FROM nushop.workboard_tasks t
  JOIN nushop.sellers s ON t.seller_id = s._id AND s.seller_account_status='hit' AND s.user_type='seller'
  LEFT JOIN nushop.users u ON t.assignee = u._id
  -- workboard_tasks is PARTITION BY DATE(created_at) with require_partition_filter.
  -- The IST predicate below does NOT match that partition expression, so on its own
  -- it scans every partition (~23 GB, which blew the daily quota). These two UTC
  -- bounds match it exactly and prune; they are a day wider so the IST window is
  -- always fully contained.
  WHERE DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 91 DAY)
    AND DATE(t.created_at) <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(t.created_at,'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 90 DAY)
    AND DATE(t.created_at,'Asia/Kolkata') <= CURRENT_DATE('Asia/Kolkata')
),
labelled AS (
  SELECT *, CASE
    WHEN role='GC' AND type='callback' AND sub_type='schedule_call'                  THEN 'Callback'
    WHEN role='GC' AND type='callback' AND sub_type='early_retention_call'           THEN 'Retention Call'
    WHEN role='GC' AND type='seller_callback_management'                             THEN 'Seller Callback'
    WHEN role='GC' AND type='troubleshoot_action' AND sub_type='troubleshoot_sop'    THEN 'TS SOP Call'
    WHEN role='GC' AND type='seller_poc_handover'                                    THEN 'POC Handover'
    WHEN role='GC' AND type='other_request' AND title LIKE '[Post-call]%'            THEN 'Commitment Task'
    WHEN role='KAM' AND type='notification' AND sub_type='notify_key_account_manager_kam' THEN 'KAM Notification'
    WHEN role='KAM' AND type='seller_poc_handover'                                   THEN 'Rapport Building Call'
    WHEN role='KAM' AND type='hit_seller_shipping_issues' AND sub_type='o2s_breach_kam' THEN 'O2S Breach (KAM)'
    WHEN role='KAM' AND type='hit_seller_shipping_issues' AND sub_type='o2s_breach'  THEN 'O2S Breach'
    WHEN role='KAM' AND type='hit_seller_shipping_issues' AND sub_type='seller_first_pickup' THEN 'First Pickup'
    WHEN role='KAM' AND type='hit_seller_shipping_issues'                            THEN 'Shipping Issue (other)'
    WHEN role='KAM' AND type='callback'                                              THEN 'Callback'
    WHEN role='KAM' AND type='seller_callback_management'                            THEN 'Seller Callback'
    WHEN role='KAM' AND type='catalogue_website'                                     THEN 'Catalogue Request'
    WHEN role='KAM' AND type='go_live_call'                                          THEN 'Go-live Call'
    WHEN role='KAM' AND type='seller_tts_kam_churn_call'                             THEN 'Churn Call'
    WHEN role='KAM' AND type='troubleshoot_action'                                   THEN 'Troubleshoot'
    WHEN role='KAM' AND type='leadership_support_escalation'                         THEN 'Escalation'
    WHEN role='KAM' AND type='shipping_operations'                                   THEN 'Shipping Ops'
    WHEN role='KAM' AND type='finance_payments'                                      THEN 'Finance'
    WHEN role='KAM' AND type='account_dashboard'                                     THEN 'Account / Dashboard'
    WHEN role='KAM' AND type IN ('other_request','others','other')                   THEN 'Other request'
    WHEN role='KAM'                                                                  THEN 'Other'
  END AS task
  FROM base WHERE role IS NOT NULL AND nm != ''
),
scored AS (
  -- One explicit due TIMESTAMP per task, so every outcome below is a plain comparison.
  --   TS SOP Call            -> creation + 48h (retention team's definition)
  --   seller_callback_primary_task -> the platform's own completion_date. This task is a
  --     SCHEDULED callback: its due date is when the seller asked to be called, and it
  --     matches creation + sla_in_min for only 0.1% of rows (median gap 18.8h). Scoring
  --     it off creation measures the schedule, not the GC.
  --   everything else        -> creation + its own sla_in_min
  -- completion_date is never EARLIER than creation + sla_in_min anywhere in this data,
  -- so this can only ever relax a bar, never tighten one.
  SELECT role, cd, task, nm, em, task_status, created_at, completed_at,
    CASE
      WHEN task = 'TS SOP Call'
        THEN TIMESTAMP_ADD(created_at, INTERVAL 2880 MINUTE)
      WHEN sub_type = 'seller_callback_primary_task' AND completion_date IS NOT NULL
        THEN completion_date
      ELSE TIMESTAMP_ADD(created_at, INTERVAL sla_in_min MINUTE)
    END AS due_ts
  FROM labelled WHERE task IS NOT NULL
),
cls AS (
  SELECT *, CASE
    WHEN task_status='completed' AND completed_at <= due_ts THEN 'on_time'
    WHEN task_status='completed'                            THEN 'late'
    WHEN CURRENT_TIMESTAMP() > due_ts                       THEN 'stuck'
    ELSE 'pending' END AS outcome,
    CASE
      WHEN task_status='completed' AND completed_at > due_ts
        THEN TIMESTAMP_DIFF(completed_at, due_ts, MINUTE)
      WHEN task_status != 'completed' AND CURRENT_TIMESTAMP() > due_ts
        THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), due_ts, MINUTE)
      ELSE 0 END AS over_min
  FROM scored
)
SELECT role, FORMAT_DATE('%Y-%m-%d', cd) AS d, task, nm, ANY_VALUE(em) AS em,
  COUNT(*) AS created,
  COUNTIF(outcome='on_time') AS on_time,
  COUNTIF(outcome='late')    AS late,
  COUNTIF(outcome='stuck')   AS stuck,
  COUNTIF(outcome='pending') AS pending,
  CAST(ROUND(SUM(over_min)) AS INT64) AS over_min_sum
FROM cls GROUP BY role, d, task, nm ORDER BY role, d, task, nm
"""


# ---------------------------------------------------------------------------
# GC team membership, mirroring the incentive engine's own sources:
#   card 12101 -> core GC roster (GM <-> GC), carries emails
#   card 12100 -> 1k-5k growth leads
#   card 11911 -> revival submitters
# Hypercare has no card of its own; the incentive engine reads it from the
# People sheet's team column, which this job has no credentials for. The five
# below are GM Aaruni Vaidya's team, taken from card 12477. Treat as a manual
# assertion and revisit if hypercare membership changes.
HYPERCARE_EMAILS = {
    "nikita.sinha@blitzscale.co", "sadiya.rajgoli@blitzscale.co",
    "tanaya.gore@blitzscale.co", "sargunpreet.singh@blitzscale.co",
    "dev.vashisth@blitzscale.co",
}


def _norm_name(s):
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _pick(row, *names):
    """First non-empty value among the given column names. Compares on a key with
    every non-alphanumeric stripped, because a card's CSV headers are display names
    ("1k 5k Gl Email Id") while its result columns are snake_case."""
    want = [re.sub(r"[^a-z0-9]", "", n.lower()) for n in names]
    norm = {re.sub(r"[^a-z0-9]", "", (k or "").lower()): (v or "").strip()
            for k, v in row.items()}
    for w in want:
        if norm.get(w):
            return norm[w]
    return ""


def build_team_map(session):
    """email/name -> Core GC | Revival GC | Hypercare GC | 1-5K GL.
    Returns ({email: cat}, {normalised name: cat}). Specific teams are loaded before
    the core roster so a GC who appears in both lands in the specific one, matching
    the incentive engine (revival submitters are forced onto the revival team).
    A card outage marks people Unmapped rather than failing the refresh."""
    by_email, by_name = {}, {}

    def add(cat, name, email):
        if email:
            by_email.setdefault(email.strip().lower(), cat)
        if name:
            by_name.setdefault(_norm_name(name), cat)

    def load(card, cat, name_cols, email_cols):
        try:
            rows = list(csvmod.DictReader(io.StringIO(run_card_csv(session, card))))
            n = 0
            for r in rows:
                nm, em = _pick(r, *name_cols), _pick(r, *email_cols)
                if nm or em:
                    add(cat, nm, em); n += 1
            print(f"  card {card} -> {cat}: {n}/{len(rows)} rows mapped")
        except Exception as e:                                   # noqa: BLE001
            print(f"  ! card {card} ({cat}) failed: {e}")

    load(12100, "1-5K GL",    ["1k_5k_gl", "gl", "name"],
                              ["1k_5k_gl_email_id", "gl_email_id", "glemail"])
    load(11911, "Revival GC", ["submitted_by", "gc", "gc_name"], ["gc_email", "email"])
    load(12101, "Core GC",    ["core_gc", "gc"],
                              ["core_gc_email_id", "gc_email_id", "gcemail"])

    for e in HYPERCARE_EMAILS:                                   # overrides the core roster
        by_email[e] = "Hypercare GC"
    return by_email, by_name


def parse_tasksla(csv_text, team_by_email=None, team_by_name=None):
    """Index days/tasks/people per role and emit compact integer rows."""
    roles = {}
    days = []
    day_ix = {}
    for r in csvmod.DictReader(io.StringIO(csv_text)):
        role = r["role"]
        if role not in roles:
            roles[role] = {"tasks": [], "people": [], "_t": {}, "_p": {}, "rows": []}
        R = roles[role]
        d = r["d"]
        if d not in day_ix:
            day_ix[d] = len(days); days.append(d)
        t = r["task"]
        if t not in R["_t"]:
            R["_t"][t] = len(R["tasks"]); R["tasks"].append(t)
        n = r["nm"]
        if n not in R["_p"]:
            R["_p"][n] = len(R["people"]); R["people"].append(n)
            R.setdefault("emails", []).append((r.get("em") or "").strip().lower())

        def i(k):
            try:
                return int(float(r.get(k) or 0))
            except ValueError:
                return 0
        R["rows"].append([day_ix[d], R["_t"][t], R["_p"][n],
                          i("created"), i("on_time"), i("late"), i("stuck"),
                          i("pending"), i("over_min_sum")])
    tbe = team_by_email or {}
    tbn = team_by_name or {}
    for role, R in roles.items():
        ems = R.pop("emails", [])
        if role == "GC":
            cats = []
            for i, nm in enumerate(R["people"]):
                e = ems[i] if i < len(ems) else ""
                cats.append(tbe.get(e) or tbn.get(_norm_name(nm)) or "Unmapped")
            R["cats"] = cats
        else:
            R["cats"] = [""] * len(R["people"])

    order = sorted(range(len(days)), key=lambda k: days[k])
    remap = {old: new for new, old in enumerate(order)}
    for R in roles.values():
        R.pop("_t"); R.pop("_p")
        for row in R["rows"]:
            row[0] = remap[row[0]]
        R["rows"].sort()
    return {"days": [days[k] for k in order], "roles": roles}


def main():
    session = login()
    curve_csv = run_csv(session, CURVE_SQL).strip()
    if "Cohort" not in curve_csv.splitlines()[0]:
        print("Unexpected curve header:", curve_csv.splitlines()[0], file=sys.stderr); sys.exit(1)
    diag = parse_diag(run_csv(session, DIAG_SQL))
    if not diag:
        print("Diagnosis query returned no rows", file=sys.stderr); sys.exit(1)
    gcgm = parse_gcgm(run_csv(session, GCGM_SQL))
    gcgms = parse_gcgms(run_csv(session, GCGMS_SQL))
    tssop = parse_tssop(run_csv(session, TSSOP_SQL))
    unas = parse_unassign(run_csv(session, UNASSIGN_SQL))
    weekact = parse_weekact(run_csv(session, WEEKACT_SQL))
    tbe, tbn = build_team_map(session)
    tasksla = parse_tasksla(run_csv(session, TASKSLA_SQL), tbe, tbn)
    _gc = tasksla["roles"].get("GC", {})
    _un = sum(1 for c in _gc.get("cats", []) if c == "Unmapped")
    print(f"  team map: {len(tbe)} emails, {len(tbn)} names; {_un} GC(s) unmapped of {len(_gc.get('people', []))}")

    payload = {"csv": curve_csv, "buckets": BUCKETS, "diagnosis": diag, "gcgm": gcgm, "gcgmSellers": gcgms, "tsSop": tssop, "unassign": unas, "weekActuals": weekact, "taskSla": tasksla}
    # only rewrite when the DATA changed (ignore the timestamp), to avoid commit noise
    try:
        with open(OUT) as f:
            old = json.load(f)
        if (old.get("csv") == payload["csv"] and old.get("diagnosis") == payload["diagnosis"]
                and old.get("buckets") == payload["buckets"] and old.get("gcgm") == payload["gcgm"]
                and old.get("gcgmSellers") == payload["gcgmSellers"]
                and old.get("tsSop") == payload["tsSop"] and old.get("unassign") == payload["unassign"]
                and old.get("weekActuals") == payload["weekActuals"]
                and old.get("taskSla") == payload["taskSla"]):
            print(f"No data change ({len(diag)} sellers, {len(curve_csv.splitlines())-1} cohorts) - leaving {OUT}."); return
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    out = {"source": "Metabase go-live>1000 base (card 11419 curve) + per-seller diagnosis (cards 11435/11610/12049/12206 + 7753)",
           "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           **payload}
    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":")); f.write("\n")
    print(f"Wrote {OUT}: {len(curve_csv.splitlines())-1} curve cohorts, {len(diag)} diagnosis sellers, "
          f"{len(gcgm['gc'])+len(gcgm['gm'])} GC/GM rows, {len(gcgms['rows'])} GC/GM sellers, "
          f"{len(tssop['rows'])} TS-SOP rows, {len(unas['rows'])} unassign rows, {len(weekact)} week-actual rows, "
          f"{sum(len(v['rows']) for v in tasksla['roles'].values())} task-SLA rows.")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} from Metabase: {e.read().decode('utf-8','replace')[:400]}", file=sys.stderr); sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"Refresh failed: {e}", file=sys.stderr); sys.exit(1)
