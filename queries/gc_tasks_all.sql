-- GC task SLA: one row per task, every field behind the "GC task SLA" tab.
-- Metabase db 6 (BigQuery). Same scoring as TASKSLA_SQL / STUCK_SQL in .github/scripts/refresh.py,
-- so totals tie out to the dashboard for the same window.
--
-- Edit the two dates in `win` AND the two literal dates in the workboard_tasks WHERE clause
-- (BigQuery only prunes partitions on literals; without them this scans ~23 GB).
--
-- sla_status:
--   Completed on time      completed_at <= due
--   Completed beyond SLA   completed_at >  due
--   Stuck                  no completed_at and now > due
--   Open within SLA        no completed_at and not yet due
--   Callback is scored on CALL evidence (card 11230), not on closure:
--   Called in window / Missed (no call in window) / Window still open
--
-- Team / GM / CL come from card 12101 (csv_upload.core_gc_mapping). A GC not on it is 'Unmapped'.
WITH win AS (SELECT DATE '2026-10-01' AS d0, DATE '2026-10-13' AS d1),
base AS (
  SELECT t.id, t.seller_id, t.type, t.sub_type, t.title, t.status AS task_status,
    t.assignee, t.created_at, t.completed_at, t.sla_in_min, t.completion_date,
    COALESCE(NULLIF(TRIM(s.display_name),''),
             REGEXP_REPLACE(TRIM(CONCAT(COALESCE(s.first_name,''),' ',COALESCE(s.last_name,''))),r'\s+',' ')) AS seller_name,
    REGEXP_REPLACE(TRIM(CONCAT(COALESCE(u.first_name,''),' ',COALESCE(u.last_name,''))),r'\s+',' ') AS owner,
    LOWER(TRIM(u.email)) AS owner_email
  FROM nushop.workboard_tasks t
  JOIN nushop.sellers s ON t.seller_id = s._id AND s.seller_account_status='hit' AND s.user_type='seller'
  JOIN nushop.users u ON t.assignee = u._id AND LOWER(u.role) LIKE '%growth-consultant%'
  CROSS JOIN win
  -- literal UTC bounds (a day wide either side) so the partition filter prunes
  WHERE DATE(t.created_at) BETWEEN DATE '2026-09-29' AND DATE '2026-10-15'
    -- Callback is keyed on the seller's requested day, so allow tasks raised a little earlier
    AND DATE(t.created_at,'Asia/Kolkata') <= win.d1
),
labelled AS (
  SELECT *, CASE
    WHEN type='callback' AND sub_type='schedule_call' AND title LIKE '%Seller Requested Callback at%' THEN 'Callback'
    WHEN type='callback' AND sub_type='early_retention_call'                    THEN 'Retention Call'
    WHEN type='seller_callback_management'
         AND sub_type != 'seller_callback_overdue_sub_task'                     THEN 'Seller Callback'
    WHEN type='troubleshoot_action' AND sub_type='troubleshoot_sop'             THEN 'TS SOP Call'
    WHEN type='seller_poc_handover'                                             THEN 'POC Handover'
    WHEN type='other_request' AND title LIKE '[Post-call]%'                     THEN 'Commitment Task'
  END AS task_type
  FROM base WHERE owner != ''
),
scored AS (
  SELECT *,
    -- the day the task counts on: Callback on the seller's requested day, the rest on creation
    IF(task_type='Callback', DATE(completion_date,'Asia/Kolkata'), DATE(created_at,'Asia/Kolkata')) AS sla_day,
    CASE
      WHEN task_type='Callback'    THEN TIMESTAMP_ADD(completion_date, INTERVAL 120 MINUTE)   -- requested time + 2h grace
      WHEN task_type='TS SOP Call' THEN TIMESTAMP_ADD(created_at, INTERVAL 2880 MINUTE)      -- fixed 48h
      WHEN sub_type='seller_callback_primary_task' AND completion_date IS NOT NULL THEN completion_date
      ELSE TIMESTAMP_ADD(created_at, INTERVAL sla_in_min MINUTE)
    END AS due_ts
  FROM labelled WHERE task_type IS NOT NULL
),
inwin AS (SELECT s.* FROM scored s CROSS JOIN win WHERE s.sla_day BETWEEN win.d0 AND win.d1),
-- Callback call evidence, card 11230 / 10771: caller by call_from phone, seller four ways
gcraw AS (
  SELECT ec.entity_id, ecd.call_to, ecd.created_at, up1.user_id AS caller_id
  FROM nushop.exotel_calls ec
  JOIN nushop.exotel_call_details ecd ON ec.exotel_call_sid = ecd.sid
  JOIN nushop.userprofiles up1
    ON ABS(SAFE_CAST(up1.contact_number AS FLOAT64)) = SAFE_CAST(ecd.call_from AS FLOAT64)
  CROSS JOIN win
  WHERE ec.created_at  >= TIMESTAMP(DATE_SUB(win.d0, INTERVAL 3 DAY))
    AND ecd.created_at >= TIMESTAMP(DATE_SUB(win.d0, INTERVAL 3 DAY))
),
gccalls AS (
  SELECT caller_id, up2.user_id AS seller_id, CAST(NULL AS STRING) AS task_id, r.created_at
  FROM gcraw r JOIN nushop.userprofiles up2
    ON ABS(SAFE_CAST(up2.contact_number AS FLOAT64)) = SAFE_CAST(r.call_to AS FLOAT64)
  UNION ALL SELECT caller_id, r.entity_id, CAST(NULL AS STRING), r.created_at
  FROM gcraw r WHERE REGEXP_CONTAINS(r.entity_id, r'^[a-f0-9]{24}$')
  UNION ALL SELECT caller_id, CAST(NULL AS STRING), r.entity_id, r.created_at FROM gcraw r
  UNION ALL SELECT caller_id, cv.user_id, CAST(NULL AS STRING), r.created_at
  FROM gcraw r JOIN `seller_app_chat.conversations` cv
    ON cv.id = r.entity_id AND DATE(cv.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
),
called AS (
  SELECT s.id, MIN(c.created_at) AS first_call
  FROM inwin s JOIN gccalls c
    ON c.caller_id = s.assignee AND (c.seller_id = s.seller_id OR c.task_id = s.id)
   AND c.created_at >= s.created_at AND c.created_at <= s.due_ts
  WHERE s.task_type = 'Callback'
  GROUP BY 1
),
map AS (
  SELECT user_id, core_gc, gm, cl,
    CASE team_mapping WHEN 'core_gc' THEN 'Core GC' WHEN 'revival_gc' THEN 'Revival GC'
         WHEN 'hypercare_gc' THEN 'Hypercare GC' WHEN '1-5K' THEN '1-5K GL' ELSE team_mapping END AS team
  FROM `blitzscale-prod-project.csv_upload.core_gc_mapping`
)
SELECT
  s.id                                                         AS task_id,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', s.created_at, 'Asia/Kolkata') AS task_created_at,
  s.sla_day,
  s.task_type, s.type AS raw_type, s.sub_type, s.title,
  s.seller_id, s.seller_name,
  s.owner                                                      AS task_owner_gc,
  s.owner_email,
  COALESCE(m.team, 'Unmapped')                                 AS team,
  COALESCE(m.gm,   'Unmapped')                                 AS gm,
  COALESCE(m.cl,   'Unmapped')                                 AS cl,
  CAST(ROUND(TIMESTAMP_DIFF(s.due_ts, s.created_at, MINUTE)) AS INT64) AS sla_min,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', s.due_ts, 'Asia/Kolkata')       AS sla_due_at,
  s.task_status,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', s.completed_at, 'Asia/Kolkata') AS completed_at,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', cl.first_call, 'Asia/Kolkata')  AS callback_first_call_at,
  CASE
    WHEN s.task_type='Callback' AND cl.first_call IS NOT NULL      THEN 'Called in window'
    WHEN s.task_type='Callback' AND CURRENT_TIMESTAMP() > s.due_ts THEN 'Missed (no call in window)'
    WHEN s.task_type='Callback'                                    THEN 'Window still open'
    WHEN s.completed_at IS NOT NULL AND s.completed_at <= s.due_ts THEN 'Completed on time'
    WHEN s.completed_at IS NOT NULL                                THEN 'Completed beyond SLA'
    WHEN CURRENT_TIMESTAMP() > s.due_ts                            THEN 'Stuck'
    ELSE 'Open within SLA'
  END                                                          AS sla_status,
  CASE
    WHEN s.task_type='Callback' THEN NULL
    WHEN s.completed_at IS NOT NULL AND s.completed_at > s.due_ts THEN TIMESTAMP_DIFF(s.completed_at, s.due_ts, MINUTE)
    WHEN s.completed_at IS NULL AND CURRENT_TIMESTAMP() > s.due_ts THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), s.due_ts, MINUTE)
  END                                                          AS minutes_over_sla
FROM inwin s
LEFT JOIN called cl ON cl.id = s.id
LEFT JOIN map m ON m.user_id = s.assignee
ORDER BY gm, task_owner_gc, sla_status, minutes_over_sla DESC
