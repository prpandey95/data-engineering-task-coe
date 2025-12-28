--MRR growth over last 3 months

WITH distinct_months AS (
    SELECT DISTINCT
        to_date(concat(month, '-01')) AS month_date
    FROM workspace.gold.mrr_by_month_plan
),
last_3_months AS (
    SELECT month_date
    FROM (
        SELECT month_date,
               ROW_NUMBER() OVER (ORDER BY month_date DESC) AS rn
        FROM distinct_months
    ) t
    WHERE rn <= 3
),
recent_mrr AS (
    SELECT
        plan,
        month AS month_str,
        to_date(concat(month, '-01')) AS month_date,
        mrr_usd,
        LAG(mrr_usd, 2) OVER (PARTITION BY plan ORDER BY to_date(concat(month, '-01'))) AS mrr_2_months_ago
    FROM workspace.gold.mrr_by_month_plan
    WHERE to_date(concat(month, '-01')) IN (SELECT month_date FROM last_3_months)
)
SELECT
    plan,
    month_str AS month,
    mrr_usd AS mrr_latest,
    mrr_2_months_ago,
    mrr_usd - mrr_2_months_ago AS growth_absolute,
    ROUND((mrr_usd - mrr_2_months_ago) / mrr_2_months_ago, 4) AS growth_pct
FROM recent_mrr
WHERE mrr_2_months_ago IS NOT NULL
ORDER BY growth_absolute DESC
LIMIT 1;

--Churn by Country
WITH churned_customers AS (
    SELECT DISTINCT
        s.customer_id,
        c.country
    FROM workspace.silver.subscriptions_dedup s
    JOIN workspace.silver.customers c
        ON s.customer_id = c.customer_id
    WHERE s.end_date IS NOT NULL
)
SELECT
    country,
    COUNT(customer_id) AS churned_customers
FROM churned_customers
GROUP BY country
ORDER BY churned_customers DESC
LIMIT 1;
