Q1.What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring)
code=
with all_data as (
  select pickup_time, season
  from `alt-school-project-386517.uberdata_1.yellow`
  union all 
  select pickup_time, season
  from `alt-school-project-386517.uberdata_1.green`
  union all 
  select pickup_datetime, season
  from `alt-school-project-386517.uberdata_1.fhv`
)
select season, count(*) as pickup_count
from all_data
group by season
order by pickup_count desc
limit 1;
Ans_fig = Winter | 27870479

Q2 What period of the day has the highest pickup number

code WITH all_data AS (
  SELECT pickup_time
  FROM `alt-school-project-386517.uberdata_1.yellow`
  UNION ALL 
  SELECT pickup_time
  FROM `alt-school-project-386517.uberdata_1.green`
  UNION ALL 
  SELECT pickup_datetime
  FROM `alt-school-project-386517.uberdata_1.fhv`
)
SELECT 
  CASE 
    WHEN EXTRACT(HOUR FROM pickup_time) >= 0 AND EXTRACT(HOUR FROM pickup_time) < 6 THEN 'Night'
    WHEN EXTRACT(HOUR FROM pickup_time) >= 6 AND EXTRACT(HOUR FROM pickup_time) < 12 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM pickup_time) >= 12 AND EXTRACT(HOUR FROM pickup_time) < 18 THEN 'Afternoon'
    ELSE 'Evening'
  END AS period_of_day,
  COUNT(*) AS pickup_count
FROM all_data
GROUP BY period_of_day
ORDER BY pickup_count DESC
LIMIT 1

Ans_fig = Evening | 31413920


Q3.What day of the week (Monday- Sunday) has the highest pickup number

Ans code.
WITH all_data AS (
  SELECT pickup_time, season
  FROM `alt-school-project-386517.uberdata_1.yellow`
  UNION ALL
  SELECT pickup_time, season
  FROM `alt-school-project-386517.uberdata_1.green`
  UNION ALL
  SELECT pickup_datetime, season
  FROM `alt-school-project-386517.uberdata_1.fhv`
)
SELECT
  EXTRACT(DAYOFWEEK FROM pickup_time) AS day_of_week,
  COUNT(*) AS pickup_count
FROM all_data
GROUP BY day_of_week
ORDER BY pickup_count DESC
LIMIT 1;

Ans_fig = day_of_week 5 | pickup_count 14514830

Q4

ans code = SELECT zone, COUNT(*) AS pickup_count
FROM (
  SELECT CONCAT(CAST(PUlocationID AS STRING), '-', CAST(DOlocationID AS STRING)) AS zone
  FROM `alt-school-project-386517.uberdata_1.fhv`
)
GROUP BY zone
ORDER BY pickup_count DESC
LIMIT 1

Ans_fig = zone 61-61 | pickup_count 10104

Q5.
WITH combined_data AS (
  SELECT CONCAT(CAST(fhv.PUlocationID AS STRING), '-', CAST(fhv.DOlocationID AS STRING)) AS zone,
         COALESCE(y.total_amount, g.total_amount) AS total_amount
  FROM `alt-school-project-386517.uberdata_1.fhv` AS fhv
  LEFT JOIN `alt-school-project-386517.uberdata_1.yellow` AS y
    ON 1 = 1
  LEFT JOIN `alt-school-project-386517.uberdata_1.green` AS g
    ON 1 = 1
)
SELECT zone, SUM(total_amount) AS total_amount
FROM combined_data
GROUP BY zone
ORDER BY total_amount DESC
LIMIT 1;

