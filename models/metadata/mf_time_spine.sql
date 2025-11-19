{{
  config(
    materialized='table',
    schema='metadata'
  )
}}

-- MetricFlow Time Spine 模型
-- 生成从 2020-01-01 到 2030-12-31 的每日时间序列
-- 用于 MetricFlow 语义层的时间维度填充

SELECT
  date_day::DATE AS date_day,
  DATE_TRUNC('week', date_day)::DATE AS date_week,
  DATE_TRUNC('month', date_day)::DATE AS date_month,
  DATE_TRUNC('quarter', date_day)::DATE AS date_quarter,
  DATE_TRUNC('year', date_day)::DATE AS date_year
FROM (
  SELECT 
    DATE '2020-01-01' + INTERVAL (generate_series) DAY AS date_day
  FROM generate_series(0, 4017)  -- 约 11 年的天数 (2020-01-01 到 2030-12-31)
)

