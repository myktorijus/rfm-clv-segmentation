-- initial registration week

WITH 
 reg_week AS (
  SELECT
   user_pseudo_id,
   DATE_TRUNC(DATE(TIMESTAMP_MICROS(MIN(event_timestamp))), WEEK) AS registration_week
  FROM 
   `turing_data_analytics.raw_events`
  WHERE
   DATE(TIMESTAMP_MICROS(event_timestamp)) < '2021-01-31' -- 2021-01-24 is last weekly cohort in the dataset
  GROUP BY
   user_pseudo_id
),
-- checking revenue per user

 user_purchases AS (
  SELECT
    user_pseudo_id,
    DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp)), WEEK) AS purchase_week,
    SUM(purchase_revenue_in_usd) AS revenue
  FROM
   `turing_data_analytics.raw_events`
  WHERE
   event_name = 'purchase'
  AND 
   TIMESTAMP_MICROS(event_timestamp) < '2021-01-31' -- 2021-01-24 is last weekly cohort in the dataset
  GROUP BY 
   user_pseudo_id, 
   purchase_week
),
-- user purchasing activity in each cohort

 cohort_purchases AS (
  SELECT
    rw.registration_week,
    DATE_DIFF(up.purchase_week, rw.registration_week, WEEK) AS week_number,
    up.revenue
  FROM
   user_purchases up
  JOIN 
   reg_week rw
  ON 
   up.user_pseudo_id = rw.user_pseudo_id
  WHERE 
   DATE_DIFF(up.purchase_week, rw.registration_week, WEEK) BETWEEN 0 AND 12
),
-- how much users are in the cohort

 cohort_sizes AS (
  SELECT
    registration_week,
    COUNT(DISTINCT user_pseudo_id) AS cohort_user_count
  FROM 
   reg_week
  GROUP BY 
   registration_week
)
-- Final query with pivoted data (max used as a neutral aggregation)

SELECT
 *
FROM (
  SELECT
   cp.registration_week,
   cp.week_number,
   ROUND(SUM(cp.revenue) / cs.cohort_user_count, 3) AS arpu
  FROM 
   cohort_purchases cp
  JOIN 
   cohort_sizes cs
  ON
   cp.registration_week = cs.registration_week
  GROUP BY
   cp.registration_week,
   cp.week_number,
   cs.cohort_user_count
)
PIVOT (
  MAX(arpu) FOR week_number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
)
ORDER BY
  registration_week;
