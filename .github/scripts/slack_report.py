#!/usr/bin/env python3
"""Post the HIT retention reports to Slack.

  python slack_report.py daily    -- restart list, every morning 09:30 IST
  python slack_report.py weekly   -- W3 retention diagnostic, Tuesdays

Both read Metabase with the same credentials the snapshot refresh uses. Add --dry-run
to print the message and skip Slack entirely.

Two deliberate choices worth knowing:
  * The daily is anchored on YESTERDAY. Today's FB spend is still landing, so "did not
    spend today" would flag half the book every morning.
  * Sellers with no growth_consultant in seller_managers are attributed to their GM, so
    they land on someone's desk instead of an "unassigned" line nobody owns.
"""
import csv as csvmod
import io
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

MB_URL = os.environ.get("METABASE_URL", "https://metabase.kaip.in").rstrip("/")
EMAIL = os.environ.get("METABASE_USER_EMAIL", "")
PASSWORD = os.environ.get("METABASE_PASSWORD", "")
DB = int(os.environ.get("METABASE_DB_ID", "6"))
SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_ID", "")

IST = timezone(timedelta(hours=5, minutes=30))


def today_ist():
    return datetime.now(IST).date()


# --------------------------------------------------------------------------- Metabase
def login():
    body = json.dumps({"username": EMAIL, "password": PASSWORD}).encode()
    req = urllib.request.Request(MB_URL + "/api/session", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["id"]


def run_csv(session, sql):
    query = {"database": DB, "type": "native", "native": {"query": sql}, "parameters": []}
    data = urllib.parse.urlencode({"query": json.dumps(query)}).encode()
    req = urllib.request.Request(MB_URL + "/api/dataset/csv", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("X-Metabase-Session", session)
    with urllib.request.urlopen(req, timeout=600) as r:
        return r.read().decode("utf-8", "replace")


def rows_of(csv_text):
    return list(csvmod.DictReader(io.StringIO(csv_text)))


# ------------------------------------------------------------------------------ Slack
def slack_api(method, payload, token=None):
    req = urllib.request.Request(f"https://slack.com/api/{method}",
                                 data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Authorization", f"Bearer {token or SLACK_TOKEN}")
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.loads(r.read())
    if not out.get("ok"):
        raise RuntimeError(f"slack {method} failed: {out.get('error')} {out}")
    return out


def post_message(text):
    out = slack_api("chat.postMessage",
                    {"channel": SLACK_CHANNEL, "text": text, "unfurl_links": False})
    return out["ts"]


def upload_csv(filename, content, title, thread_ts=None):
    """files.upload is retired; this is the getUploadURLExternal -> complete flow."""
    raw = content.encode()
    q = urllib.parse.urlencode({"filename": filename, "length": len(raw)})
    req = urllib.request.Request(f"https://slack.com/api/files.getUploadURLExternal?{q}",
                                 method="GET")
    req.add_header("Authorization", f"Bearer {SLACK_TOKEN}")
    with urllib.request.urlopen(req, timeout=60) as r:
        step1 = json.loads(r.read())
    if not step1.get("ok"):
        raise RuntimeError(f"getUploadURLExternal failed: {step1.get('error')}")

    put = urllib.request.Request(step1["upload_url"], data=raw, method="POST")
    with urllib.request.urlopen(put, timeout=120) as r:
        r.read()

    payload = {"files": [{"id": step1["file_id"], "title": title}],
               "channel_id": SLACK_CHANNEL}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return slack_api("files.completeUploadExternal", payload)


# ------------------------------------------------------------------------------- SQL
DAILY_SQL = r"""
WITH
golive AS (
  SELECT seller_id, MIN(start_date) AS gd, FORMAT_DATE('%G-W%V', MIN(start_date)) AS gw
  FROM nushop.gc_view_3
  WHERE marketing_spend > 1000 AND team_mapping = 'HIT'
  GROUP BY seller_id
  HAVING MIN(start_date) >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 27 DAY)
),
yday AS (SELECT DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY) d),
spend_y AS (
  SELECT g.seller_id, SUM(fb.spend) sp_yday
  FROM golive g JOIN fb_marketings.fb_marketing_insights fb ON fb.seller_id = g.seller_id
  WHERE fb.breakdown_key IS NULL
    AND DATE(fb.spend_date,'Asia/Kolkata') = (SELECT d FROM yday)
  GROUP BY 1),
spend_7 AS (
  SELECT g.seller_id, COUNT(DISTINCT DATE(fb.spend_date,'Asia/Kolkata')) days_spent_7d
  FROM golive g JOIN fb_marketings.fb_marketing_insights fb ON fb.seller_id = g.seller_id
  WHERE fb.breakdown_key IS NULL
    AND DATE(fb.spend_date,'Asia/Kolkata')
        BETWEEN DATE_SUB((SELECT d FROM yday), INTERVAL 6 DAY) AND (SELECT d FROM yday)
    AND fb.spend > 0
  GROUP BY 1),
wkspend AS (
  SELECT g.seller_id,
    SUM(IF(DATE_TRUNC(DATE(fb.spend_date,'Asia/Kolkata'),ISOWEEK)
           =DATE_TRUNC(CURRENT_DATE('Asia/Kolkata'),ISOWEEK), fb.spend,0)) sp_this_wk,
    SUM(fb.spend) sp_life
  FROM golive g JOIN fb_marketings.fb_marketing_insights fb ON fb.seller_id = g.seller_id
  WHERE fb.breakdown_key IS NULL
    AND DATE(fb.spend_date,'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 40 DAY)
  GROUP BY 1),
ts_act AS (
  SELECT g.seller_id, COUNT(*) n_ts_actions, MAX(DATE(a.createdat,'Asia/Kolkata')) last_ts_action
  FROM golive g JOIN `blitzscale-prod-project.nushop.troubleshoots_actions_completions` a
    ON a.seller_id = g.seller_id
  WHERE DATE(a.createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY)
    AND DATE(a.createdat,'Asia/Kolkata') >= g.gd
  GROUP BY 1),
blk AS (
  SELECT g.seller_id, COUNT(*) n_open_blocks,
         STRING_AGG(DISTINCT t.sub_type, ', ' LIMIT 3) block_types,
         MIN(DATE(t.created_at,'Asia/Kolkata')) oldest_block_raised
  FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id = g.seller_id
  WHERE DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND t.source='crm_initiated' AND t.created_by IS NOT NULL
    AND t.sub_type IN ('ad_account_suspension','ad_account_blocked','business_manager_verification',
       'ad_account_not_spending','business_manager_restricted','pixel_inactive','page_restricted',
       'ad_account_hacked','business_manager_access','account_restricted','account_not_spending',
       'account_permanently_restricted','page_unpublished')
    AND t.status != 'completed'
  GROUP BY 1),
-- Ownership: seller_managers first, then the console summary, which carries a GC for
-- 26 of the 27 sellers seller_managers has no GC for. Without the second source the
-- daily post's biggest "owner" was an unowned bucket nobody would pick up.
mgr AS (
  -- seller_console_metrics_summary writes '-' (not NULL, not '') when a role is vacant,
  -- so a plain NULLIF on empty string lets the placeholder through as a name.
  SELECT COALESCE(sm.seller_id, sc.seller_id) seller_id,
    COALESCE(NULLIF(NULLIF(TRIM(sm.gc),''),'-'), NULLIF(NULLIF(TRIM(sc.gc2),''),'-')) gc,
    COALESCE(NULLIF(NULLIF(TRIM(sm.gm),''),'-'), NULLIF(NULLIF(TRIM(sc.gm2),''),'-')) gm,
    NULLIF(NULLIF(TRIM(sc.kam2),''),'-') kam
  FROM (
    SELECT s.seller_id,
      MAX(IF(s.manager_type='growth_consultant', REGEXP_REPLACE(TRIM(CONCAT(COALESCE(u.first_name,''),' ',COALESCE(u.last_name,''))),r'\s+',' '), NULL)) gc,
      MAX(IF(s.manager_type='growth_manager',    REGEXP_REPLACE(TRIM(CONCAT(COALESCE(u.first_name,''),' ',COALESCE(u.last_name,''))),r'\s+',' '), NULL)) gm
    FROM nushop.seller_managers s LEFT JOIN nushop.users u ON s.manager_id = u._id
    GROUP BY 1) sm
  FULL OUTER JOIN (
    SELECT seller_id, MAX(gc_name) gc2, MAX(gm_name) gm2, MAX(kam_name) kam2
    FROM `blitzscale-prod-project.analytics.seller_console_metrics_summary` GROUP BY 1) sc
  USING(seller_id)),
-- 14-day spending trend for THIS cohort, so a single day's rate is readable in context
days AS (SELECT dt FROM UNNEST(GENERATE_DATE_ARRAY(
           DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 7 DAY),
           DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY))) dt),
sp_day AS (
  SELECT DATE(fb.spend_date,'Asia/Kolkata') dt, fb.seller_id, SUM(fb.spend) s
  FROM fb_marketings.fb_marketing_insights fb JOIN golive g ON g.seller_id=fb.seller_id
  WHERE fb.breakdown_key IS NULL
    AND DATE(fb.spend_date,'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 7 DAY)
  GROUP BY 1,2),
trend AS (
  SELECT FORMAT_DATE('%m-%d %a', d.dt) day,
    ROUND(100*SAFE_DIVIDE(COUNTIF(g.gd<=d.dt AND COALESCE(sp_day.s,0)>0), COUNTIF(g.gd<=d.dt)),1) pct
  FROM days d CROSS JOIN golive g
  LEFT JOIN sp_day ON sp_day.dt=d.dt AND sp_day.seller_id=g.seller_id
  GROUP BY d.dt ORDER BY d.dt)
SELECT
  CASE
    WHEN COALESCE(b.n_open_blocks,0) > 0        THEN '2_BLOCKED'
    WHEN COALESCE(sy.sp_yday,0) > 0             THEN '4_SPENDING'
    WHEN COALESCE(ta.n_ts_actions,0) = 0        THEN '3_TS_PENDING'
    ELSE                                             '1_RESTART'
  END                                               AS action_bucket,
  g.gw AS golive_week,
  DATE_DIFF(CURRENT_DATE('Asia/Kolkata'), g.gd, ISOWEEK) AS rel_week_now,
  g.seller_id,
  sel.display_name AS seller,
  -- Ownership: GC, else GM. A seller with neither is not on the managed track --
  -- that is a SELF SERVE account, not an unassigned one, so it is labelled rather
  -- than falling through to the KAM (who owns the account, not its growth).
  COALESCE(m.gc,
           CONCAT('(GM) ', m.gm),
           'Self serve') AS owner,
  m.gc, m.gm, m.kam,
  CAST(ROUND(COALESCE(sy.sp_yday,0)) AS INT64)      AS spend_yesterday,
  COALESCE(s7.days_spent_7d,0)                      AS days_spent_last_7,
  CAST(ROUND(COALESCE(ws.sp_this_wk,0)) AS INT64)   AS spend_this_week,
  CAST(ROUND(COALESCE(ws.sp_life,0)) AS INT64)      AS spend_last_40d,
  IF(COALESCE(ta.n_ts_actions,0) > 0,'yes','NO')    AS ts_done,
  COALESCE(ta.n_ts_actions,0)                       AS ts_actions_done,
  ta.last_ts_action,
  COALESCE(b.n_open_blocks,0)                       AS open_blocks,
  b.block_types, b.oldest_block_raised,
  (SELECT STRING_AGG(FORMAT('%s=%.1f', day, pct), ';' ORDER BY day) FROM trend) AS trend_blob
FROM golive g
LEFT JOIN spend_y sy USING(seller_id)
LEFT JOIN spend_7 s7 USING(seller_id)
LEFT JOIN wkspend ws USING(seller_id)
LEFT JOIN ts_act  ta USING(seller_id)
LEFT JOIN blk     b  USING(seller_id)
LEFT JOIN mgr     m  USING(seller_id)
LEFT JOIN (SELECT _id, display_name FROM nushop.sellers) sel ON sel._id = g.seller_id
ORDER BY action_bucket, days_spent_last_7 DESC, spend_last_40d DESC
"""


# Leading indicators per go-live week. The young cohorts have no W3 yet, but S/GMV,
# zero-GMV, TS mix and block rate are all readable from week 0 -- and they track W3
# well (W32: S/GMV 0.66 + blocked 21% -> W3 34.3%; W28: 0.55 + 6.7% -> 57.5%). Two
# mature weeks are carried alongside as the benchmark to read the young ones against.
DAILY_COHORT_SQL = r"""
WITH golive AS (
  SELECT seller_id, MIN(start_date) gd, FORMAT_DATE('%G-W%V', MIN(start_date)) gw
  FROM nushop.gc_view_3 WHERE marketing_spend>1000 AND team_mapping='HIT'
  GROUP BY 1
  HAVING MIN(start_date) >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 48 DAY)
     AND DATE_TRUNC(MIN(start_date),ISOWEEK) < DATE_TRUNC(CURRENT_DATE('Asia/Kolkata'),ISOWEEK)
),
sw AS (
  SELECT g.seller_id, g.gw, g.gd,
    SUM(IF(rw=0,ms,0)) sp0, SUM(IF(rw=3,ms,0)) sp3, SUM(IF(rw=0,gmvv,0)) gmv0
  FROM (SELECT g.seller_id,g.gw,g.gd, DATE_DIFF(v.start_date,g.gd,ISOWEEK) rw,
               COALESCE(v.marketing_spend,0) ms, COALESCE(v.true_gmv,0) gmvv
        FROM golive g JOIN nushop.gc_view_3 v ON v.seller_id=g.seller_id) g
  GROUP BY 1,2,3),
tsa AS (
  SELECT g.seller_id,
    MAX(IF(r.system_user_id='64d156455a26610014b266a9',1,0)) auto, MAX(1) any_ts
  FROM golive g JOIN nushop.troubleshoot_workflow_report r ON r.seller_id=g.seller_id
  WHERE DATE(r.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 55 DAY)
    AND DATE(r.created_at,'Asia/Kolkata') >= g.gd
  GROUP BY 1),
blk AS (
  SELECT DISTINCT g.seller_id
  FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 55 DAY)
    AND t.source='crm_initiated' AND t.created_by IS NOT NULL AND t.status!='completed'
    AND t.sub_type IN ('ad_account_suspension','ad_account_blocked','business_manager_verification',
       'ad_account_not_spending','business_manager_restricted','pixel_inactive','page_restricted',
       'ad_account_hacked','business_manager_access','account_restricted','account_not_spending',
       'account_permanently_restricted','page_unpublished'))
SELECT sw.gw AS wk,
  DATE_DIFF(CURRENT_DATE('Asia/Kolkata'), DATE_TRUNC(MIN(sw.gd),ISOWEEK), WEEK) wks,
  COUNT(*) n,
  IF(DATE_DIFF(CURRENT_DATE('Asia/Kolkata'), DATE_TRUNC(MIN(sw.gd),ISOWEEK), WEEK)>3,
     ROUND(100*AVG(IF(sw.sp3>=3000,1,0)),1), NULL) w3_ret,
  ROUND(APPROX_QUANTILES(SAFE_DIVIDE(sw.sp0,sw.gmv0),2)[OFFSET(1)],2) sgmv_w0,
  ROUND(100*AVG(IF(sw.gmv0<=0,1,0)),1) pct_0gmv_w0,
  ROUND(100*AVG(COALESCE(tsa.auto,0)),1) pct_ts_auto,
  ROUND(100*AVG(IF(tsa.any_ts IS NULL,1,0)),1) pct_no_ts,
  ROUND(100*AVG(IF(blk.seller_id IS NULL,0,1)),1) pct_blocked_now
FROM sw LEFT JOIN tsa USING(seller_id) LEFT JOIN blk USING(seller_id)
GROUP BY 1 ORDER BY 1
"""


WEEKLY_SQL = r"""
WITH
golive AS (
  SELECT seller_id, MIN(start_date) AS golive_date,
         FORMAT_DATE('%G-W%V', MIN(start_date)) AS gw
  FROM nushop.gc_view_3
  WHERE marketing_spend>1000 AND team_mapping='HIT'
  GROUP BY seller_id
  HAVING MIN(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
     AND DATE_TRUNC(MIN(start_date), ISOWEEK) < DATE_TRUNC(CURRENT_DATE(), ISOWEEK)
),
sw AS (
  SELECT g.seller_id, g.gw, g.golive_date,
    SUM(IF(rw=0,ms,0)) sp0, SUM(IF(rw=1,ms,0)) sp1, SUM(IF(rw=2,ms,0)) sp2, SUM(IF(rw=3,ms,0)) sp3,
    SUM(IF(rw=0,gmvv,0)) gmv0, SUM(IF(rw=1,gmvv,0)) gmv1, SUM(IF(rw=2,gmvv,0)) gmv2,
    SUM(IF(rw BETWEEN 0 AND 3, rtov,0)) rto03, SUM(IF(rw BETWEEN 0 AND 3, ordv,0)) ord03
  FROM (
    SELECT g.seller_id, g.gw, g.golive_date,
           DATE_DIFF(v.start_date, g.golive_date, ISOWEEK) rw,
           COALESCE(v.marketing_spend,0) ms, COALESCE(v.true_gmv,0) gmvv,
           COALESCE(v.total_orders,0) ordv, COALESCE(v.rtos,0) rtov
    FROM golive g JOIN nushop.gc_view_3 v ON v.seller_id=g.seller_id
  ) g GROUP BY 1,2,3),
ad AS (
  SELECT g.seller_id, 1 AS ad_impact
  FROM golive g JOIN nushop.workboard_tasks t ON t.seller_id=g.seller_id
  WHERE DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
    AND t.source='crm_initiated' AND t.created_by IS NOT NULL
    AND t.sub_type IN ('ad_account_suspension','ad_account_blocked','business_manager_verification',
       'ad_account_not_spending','business_manager_restricted','pixel_inactive','page_restricted',
       'ad_account_hacked','business_manager_access','account_restricted','account_not_spending',
       'account_permanently_restricted','page_unpublished')
    AND DATE_DIFF(DATE(t.created_at,'Asia/Kolkata'), g.golive_date, ISOWEEK) <= 3
    AND NOT (t.status='completed' AND t.completed_at IS NOT NULL
             AND DATE_DIFF(DATE(t.completed_at,'Asia/Kolkata'), g.golive_date, ISOWEEK) <= 3)
  GROUP BY 1),
o2s AS (
  SELECT g.seller_id,
    AVG(IF(o.pickup_time IS NOT NULL, TIMESTAMP_DIFF(o.pickup_time,o.createdat,HOUR)/24.0, NULL)) o2s02
  FROM golive g JOIN nushop.orderitems o ON o.seller_id=g.seller_id
  WHERE DATE(o.createdat,'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
    AND o.seller_last_status NOT IN ('initiated','invalid','enqueued')
    AND o.awb_no!='None' AND o.in_house_status!='awb_expired'
    AND DATE_DIFF(DATE(o.createdat,'Asia/Kolkata'), g.golive_date, ISOWEEK) BETWEEN 0 AND 2
  GROUP BY 1),
lead AS (
  SELECT seller_id,
    CASE WHEN offer_raw IN ('Zero(0) SD','0% Commission 1st Month','0% Commission for First 30 Days') THEN 'Zero(SD)'
         WHEN offer_raw IN ('2K+5K','₹2K Deposit + ₹5K Marketing') THEN '2K+5K'
         WHEN offer_raw IN ('5K+5K','₹5K Deposit + ₹5K Marketing') THEN '5K+5K'
         WHEN offer_raw IN ('5k SD post 10k Spend','₹5K Credit After ₹10K Spend') THEN '5k post10k'
         WHEN offer_raw IN ('5k SD post 15k Spend','₹5K Credit After ₹15K Spend') THEN '5k post15k'
         WHEN offer_raw IN ('5k+0% Commission','₹5K Credit + 0% Commission Combo') THEN '5k+0%'
         WHEN offer_raw = 'NA' THEN 'NA'
         WHEN offer_raw IS NULL OR offer_raw='' THEN 'No lead' ELSE 'Other' END offer
  FROM (
    SELECT REGEXP_REPLACE(TRIM(Seller_ID__c),r'^"|"$','') seller_id, TRIM(Offers__c) offer_raw,
      ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(TRIM(Seller_ID__c),r'^"|"$','')
        ORDER BY COALESCE(DATE(TIMESTAMP(Website_Form_Filled_D_T__c),'Asia/Kolkata'),
                          DATE(TIMESTAMP(CreatedDate),'Asia/Kolkata')) DESC) rn
    FROM `blitzscale-prod-project.salesforce.Lead`
    WHERE Seller_ID__c IS NOT NULL AND LOWER(TRIM(Seller_ID__c)) NOT IN ('na','n/a','null','')
  ) WHERE rn=1),
fact AS (
  SELECT sw.seller_id, sw.gw, sw.golive_date,
    IF(sw.sp3>=3000,1,0) r3,
    IF(sw.gmv0<=0,1,0) zero_gmv_w0,
    SAFE_DIVIDE(sw.sp0,sw.gmv0) sgmv0, SAFE_DIVIDE(sw.sp2,sw.gmv2) sgmv2,
    COALESCE(ad.ad_impact,0) ad_impact,
    IF(o2s.o2s02>3,1,0) o2s_bad, o2s.o2s02,
    sw.rto03, sw.ord03,
    COALESCE(lead.offer,'No lead') offer
  FROM sw LEFT JOIN ad USING(seller_id) LEFT JOIN o2s USING(seller_id) LEFT JOIN lead USING(seller_id)),
wk AS (SELECT gw, COUNT(*) n_wk, DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(MIN(golive_date),ISOWEEK), WEEK) wks
       FROM fact GROUP BY 1),
mo AS (SELECT gw, offer b, COUNT(*) n, AVG(r3) ret3 FROM fact GROUP BY 1,2),
mo2 AS (SELECT mo.gw,
    STRING_AGG(FORMAT('%s %d%%(%d%%)', mo.b, CAST(ROUND(100*mo.n/w.n_wk) AS INT64),
      CAST(ROUND(100*mo.ret3) AS INT64)), ' · ' ORDER BY mo.n DESC LIMIT 4) offer_mix
  FROM mo JOIN wk w USING(gw) WHERE w.wks>3 GROUP BY 1)
SELECT f.gw AS golive_week, MAX(w.wks) weeks_elapsed, COUNT(*) cohort_n,
  IF(MAX(w.wks)>3, ROUND(100*AVG(r3),1), NULL) w3_ret,
  ROUND(100*AVG(zero_gmv_w0),1) pct_0gmv_w0,
  IF(MAX(w.wks)>2, ROUND(100*AVG(IF(o2s02 IS NULL,NULL,o2s_bad)),1), NULL) pct_o2s_gt_3d,
  IF(MAX(w.wks)>3, ROUND(100*SAFE_DIVIDE(SUM(rto03),SUM(ord03)),1), NULL) rto_rate,
  IF(MAX(w.wks)>3, ROUND(100*AVG(ad_impact),1), NULL) pct_ad_block,
  IF(MAX(w.wks)>0, ROUND(APPROX_QUANTILES(sgmv0,2)[OFFSET(1)],2), NULL) sgmv_w0,
  IF(MAX(w.wks)>2, ROUND(APPROX_QUANTILES(sgmv2,2)[OFFSET(1)],2), NULL) sgmv_w2,
  ANY_VALUE(mo2.offer_mix) offer_mix
FROM fact f JOIN wk w USING(gw) LEFT JOIN mo2 USING(gw)
GROUP BY 1 ORDER BY 1
"""


# ---------------------------------------------------------------------------- render
def spark(vals, lo=68.0, hi=80.0):
    ch = "▁▂▃▄▅▆▇█"
    span = max(0.1, hi - lo)
    return "".join(ch[min(7, max(0, int((v - lo) / span * 8)))] for v in vals)


def fmt_daily(rows, cohort=None):
    yday = today_ist() - timedelta(days=1)
    n = len(rows)
    if not n:
        return "*🔁 HIT restart list* — no live sellers in the last 4 go-live weeks.", None

    def bucket(p):
        return [r for r in rows if r["action_bucket"] == p]
    restart, blocked = bucket("1_RESTART"), bucket("2_BLOCKED")
    tsp, ok = bucket("3_TS_PENDING"), bucket("4_SPENDING")
    hot = [r for r in restart if int(r["days_spent_last_7"] or 0) >= 5]
    cold = [r for r in restart if int(r["days_spent_last_7"] or 0) == 0]

    wk = defaultdict(lambda: [0, 0])
    for r in rows:
        wk[r["golive_week"]][0] += 1
        wk[r["golive_week"]][1] += (r["action_bucket"] == "4_SPENDING")

    ob = [r["oldest_block_raised"] for r in blocked if r.get("oldest_block_raised")]
    oldest = (yday - date.fromisoformat(min(ob))).days if ob else 0
    owners = Counter(r["owner"] or "unowned" for r in restart)
    money = sum(int(r["spend_last_40d"] or 0) for r in restart)

    tr = []
    blob = rows[0].get("trend_blob") or ""
    for part in blob.split(";"):
        if "=" in part:
            d, v = part.rsplit("=", 1)
            try:
                tr.append((d, float(v)))
            except ValueError:
                pass

    L = [f"*🔁 HIT restart list — {yday.strftime('%a %d %b')}*",
         f"_{n} sellers live · go-live weeks {min(wk)[-3:]}–{max(wk)[-3:]}_", "",
         "```",
         f"Spending yesterday  {len(ok):5d}  {100*len(ok)/n:5.1f}%",
         f"RESTART NOW         {len(restart):5d}  {100*len(restart)/n:5.1f}%   no block · TS done · didn't spend",
         f"Blocked (ops)       {len(blocked):5d}  {100*len(blocked)/n:5.1f}%   oldest open {oldest}d",
         f"TS pending          {len(tsp):5d}  {100*len(tsp)/n:5.1f}%",
         "```"]
    if tr:
        note = "  _(Sundays run ~4pp low)_" if yday.weekday() == 6 else ""
        L.append(f"*Spending rate, last 7 days*  {spark([v for _, v in tr])}  {tr[-1][1]:.0f}%{note}")
    L += ["",
          f"*→ Call first:* {len(hot)} of the {len(restart)} spent 5+ of last 7 days then stopped.",
          f"{len(cold)} are fully cold (0/7). ₹{money/100000:.1f}L of 40-day spend idle in this bucket.",
          "", "*Still spending, by go-live week*", "```"]
    for k in sorted(wk):
        t, s = wk[k]
        L.append(f"{k[-3:]}  {s:3d}/{t:3d}  {100*s/t:3.0f}%  {'█'*round(18*s/t)}")
    L.append("```")
    if cohort:
        mature = [c for c in cohort if c.get("w3_ret")]
        young = [c for c in cohort if not c.get("w3_ret")]
        L += ["", "*Leading indicators — where the young cohorts are heading*", "```",
              "wk    n   S/GMV  0GMV  TSauto  noTS  blkd |    W3"]
        for c in cohort[-6:]:
            def p(k):
                v = c.get(k)
                return f"{float(v):.0f}%" if v not in (None, "", "-") else "   -"
            sg = c.get("sgmv_w0")
            sg = f"{float(sg):.2f}" if sg else "  - "
            w3 = c.get("w3_ret")
            w3 = f"{float(w3):.1f}%" if w3 else "   -"
            L.append(f"{c['wk'][-3:]}  {c['n']:>4}   {sg}  {p('pct_0gmv_w0'):>4} "
                     f"{p('pct_ts_auto'):>6} {p('pct_no_ts'):>5} {p('pct_blocked_now'):>5} | {w3:>6}")
        L.append("```")
        # benchmark the young weeks against what the mature ones actually delivered
        if mature and young:
            base = sorted(float(m["sgmv_w0"]) for m in mature if m.get("sgmv_w0"))
            med = base[len(base)//2] if base else None
            worst = max(young, key=lambda c: float(c.get("sgmv_w0") or 0))
            if med and float(worst.get("sgmv_w0") or 0) > med:
                ref = min(mature, key=lambda m: float(m.get("w3_ret") or 99))
                L.append(f"⚠ *{worst['wk'][-3:]}* S/GMV {float(worst['sgmv_w0']):.2f} is above the "
                         f"mature median {med:.2f} — {ref['wk'][-3:]} ran "
                         f"{float(ref['sgmv_w0']):.2f} and landed at {ref['w3_ret']}% W3.")
            noTS = max(young, key=lambda c: float(c.get("pct_no_ts") or 0))
            if float(noTS.get("pct_no_ts") or 0) >= 15:
                L.append(f"⚠ *{noTS['wk'][-3:]}* has {float(noTS['pct_no_ts']):.0f}% with no TS run yet.")

    top = owners.most_common(5)
    L.append("*Restarts by owner*  " + " · ".join(f"{o} {c}" for o, c in top) +
             (f"  _(+{len(owners)-5} more)_" if len(owners) > 5 else ""))
    L.append("_No GC falls through to (GM); no GC and no GM = Self serve._")
    return "\n".join(L), restart


def fmt_weekly(rows):
    rows = [r for r in rows if r.get("w3_ret")]
    if not rows:
        return "*📊 W3 retention* — no mature cohort yet."
    rows = rows[-5:]
    last = rows[-1]
    prev = rows[-2] if len(rows) > 1 else None
    d = ""
    if prev and prev.get("w3_ret"):
        delta = float(last["w3_ret"]) - float(prev["w3_ret"])
        d = f"  {'▲' if delta>=0 else '▼'} {delta:+.1f}pp vs {prev['golive_week'][-3:]}"
    L = [f"*📊 W3 retention — week of {today_ist().strftime('%d %b')}*",
         f"Latest mature *{last['golive_week']}: {last['w3_ret']}%* (n={last['cohort_n']}){d}", "",
         "```",
         "wk    n     W3    0GMV  O2S>3d   RTO  AdBlk  S/GMV W0→W2"]
    for r in rows:
        def g(k, s=""):
            return (r.get(k) or "-") + s
        def r2(k):                      # keep the trailing zero so the column lines up
            v = r.get(k)
            try:
                return f"{float(v):.2f}"
            except (TypeError, ValueError):
                return "  - "
        L.append(f"{r['golive_week'][-3:]}  {r['cohort_n']:>4}  {g('w3_ret'):>5}% "
                 f"{g('pct_0gmv_w0'):>6}% {g('pct_o2s_gt_3d'):>6}% {g('rto_rate'):>5}% "
                 f"{g('pct_ad_block'):>5}%  {r2('sgmv_w0')}→{r2('sgmv_w2')}")
    L.append("```")
    worst = max(rows, key=lambda r: float(r.get("pct_ad_block") or 0))
    if float(worst.get("pct_ad_block") or 0) > 15:
        L.append(f"⚠ *{worst['golive_week'][-3:]}* ad-block {worst['pct_ad_block']}% "
                 f"— well above the {len(rows)}-week norm.")
    if last.get("offer_mix"):
        L.append(f"*Offer mix {last['golive_week'][-3:]}*  {last['offer_mix']}  _share(W3 ret)_")
    L.append("_All W0–W3 drivers stay blank until the cohort has aged into them._")
    return "\n".join(L)


def to_csv(rows, cols):
    buf = io.StringIO()
    w = csvmod.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "daily").lower()
    dry = "--dry-run" in sys.argv
    if mode not in ("daily", "weekly"):
        sys.exit("usage: slack_report.py [daily|weekly] [--dry-run]")
    if not (EMAIL and PASSWORD):
        sys.exit("METABASE_USER_EMAIL / METABASE_PASSWORD not set")

    session = login()
    if mode == "daily":
        rows = rows_of(run_csv(session, DAILY_SQL))
        cohort = rows_of(run_csv(session, DAILY_COHORT_SQL))
        text, restart = fmt_daily(rows, cohort)
    else:
        rows = rows_of(run_csv(session, WEEKLY_SQL))
        text, restart = fmt_weekly(rows), None

    print(text)
    if dry:
        print(f"\n[dry-run] {len(rows)} rows; nothing sent to Slack.")
        return
    if not (SLACK_TOKEN and SLACK_CHANNEL):
        sys.exit("SLACK_BOT_TOKEN / SLACK_CHANNEL_ID not set")

    ts = post_message(text)
    if mode == "daily" and restart:
        yday = today_ist() - timedelta(days=1)
        cols = ["action_bucket", "golive_week", "rel_week_now", "seller_id", "seller",
                "owner", "gc", "gm", "kam", "spend_yesterday", "days_spent_last_7",
                "spend_this_week", "spend_last_40d", "ts_done", "ts_actions_done",
                "last_ts_action", "open_blocks", "block_types", "oldest_block_raised"]
        # the whole cohort, call-order first -- GCs filter it themselves
        upload_csv(f"restart_list_{yday}.csv", to_csv(rows, cols),
                   f"Restart list {yday} — {len(restart)} to call", thread_ts=ts)
    print(f"\nposted to {SLACK_CHANNEL}")


if __name__ == "__main__":
    main()
