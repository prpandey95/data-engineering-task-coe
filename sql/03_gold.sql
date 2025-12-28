CREATE SCHEMA IF NOT EXISTS workspace.gold;

-- select * from workspace.silver.payments;
-- select * from workspace.silver.subscriptions;

--select * from workspace.silver.payments_success_dedup;

--select distinct currency from workspace.silver.payments_success_dedup;

--MRR

CREATE OR REPLACE VIEW workspace.gold.mrr_by_month_plan AS
SELECT
    date_format(p.paid_at, 'yyyy-MM') AS month,
    s.plan,
    SUM(
        CASE
            WHEN p.currency = 'USD' THEN p.amount
            WHEN p.currency = 'PHP' THEN p.amount / 58.57
            ELSE 0
        END
    ) AS mrr_usd
FROM workspace.silver.payments_success_dedup p
JOIN workspace.silver.subscriptions_dedup s
    ON p.subscription_id = s.subscription_id
GROUP BY
    date_format(p.paid_at, 'yyyy-MM'),
    s.plan
ORDER BY
    month,
    plan;

-- describe table workspace.bronze.payments;

-- CREATE TABLE workspace.silver.payments_success_dedup_tbl
-- AS SELECT * FROM workspace.silver.payments_success_dedup;

-- SELECT *
-- FROM workspace.silver.payments_success_dedup
-- WHERE paid_at = '' or paid_at = ' ';

--select * from workspace.gold.mrr_by_month_plan;

--Churn Rate by Month
CREATE OR REPLACE VIEW workspace.gold.churn_rate_by_month AS
WITH months AS (
    -- Generate a continuous list of months covering the full subscription timeline
    SELECT
        explode(
            sequence(
                date_trunc('month', min(start_date)),
                date_trunc('month', max(coalesce(end_date, current_date()))),
                interval 1 month
            )
        ) AS month_start
    FROM workspace.silver.subscriptions_dedup
),

active_start AS (
    -- Customers active at the start of each month
    SELECT
        date_format(m.month_start, 'yyyy-MM') AS month,
        COUNT(DISTINCT s.customer_id) AS active_customers_start_of_month
    FROM months m
    JOIN workspace.silver.subscriptions_dedup s
        ON s.start_date < m.month_start
       AND (s.end_date IS NULL OR s.end_date >= m.month_start)
    GROUP BY date_format(m.month_start, 'yyyy-MM')
),

churned AS (
    -- Customers who churned during the month
    SELECT
        date_format(end_date, 'yyyy-MM') AS month,
        COUNT(DISTINCT customer_id) AS churned_customers
    FROM workspace.silver.subscriptions_dedup
    WHERE end_date IS NOT NULL
    GROUP BY date_format(end_date, 'yyyy-MM')
)

SELECT
    a.month,
    COALESCE(c.churned_customers, 0) AS churned_customers,
    a.active_customers_start_of_month,
    ROUND(
        COALESCE(c.churned_customers, 0) / a.active_customers_start_of_month,
        4
    ) AS churn_rate
FROM active_start a
LEFT JOIN churned c
    ON a.month = c.month
ORDER BY a.month;


--select * from workspace.gold.churn_rate_by_month;
--select * from workspace.silver.subscriptions_dedup;
--select * from workspace.silver.customers;
--select * from workspace.silver.payments_success_dedup;

--Top 10 Countries by Revenue

CREATE OR REPLACE VIEW workspace.gold.top_10_countries_by_revenue AS
SELECT
    c.country,
    SUM(
        CASE
            WHEN p.currency = 'USD' THEN p.amount
            WHEN p.currency = 'PHP' THEN p.amount / 58.57  -- convert to USD
            ELSE 0
        END
    ) AS total_revenue
FROM workspace.silver.payments_success_dedup p
JOIN workspace.silver.subscriptions_dedup s
    ON p.subscription_id = s.subscription_id
JOIN workspace.silver.customers c
    ON s.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC
LIMIT 10;

--select * from workspace.gold.top_10_countries_by_revenue;





