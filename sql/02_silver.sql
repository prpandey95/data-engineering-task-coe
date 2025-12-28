-- SELECT * FROM workspace.bronze.customers;
--signup_date
--start_date/end_date
--paid_at

CREATE SCHEMA IF NOT EXISTS workspace.silver;

CREATE OR REPLACE VIEW workspace.silver.customers AS
SELECT
    customer_id,
    country,
    TO_DATE(signup_date, 'yyyy-MM-dd') AS signup_date,
    `_rescued_data`,
    ingested_at,
    source_file
FROM workspace.bronze.customers;

-- select * from workspace.bronze.subscriptions;

CREATE OR REPLACE VIEW workspace.silver.subscriptions_dedup AS
SELECT
    subscription_id,
    customer_id,
    plan,
    TO_DATE(start_date, 'yyyy-MM-dd') AS start_date,
    TO_DATE(end_date, 'yyyy-MM-dd')   AS end_date,
    status,
    `_rescued_data`,
    ingested_at,
    source_file
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY subscription_id
            ORDER BY TO_DATE(start_date, 'yyyy-MM-dd') DESC
        ) AS rn
    FROM workspace.bronze.subscriptions
) t
WHERE rn = 1;

-- select count(*) from workspace.bronze.subscriptions;
-- select * from workspace.silver.subscriptions;
-- select subscription_id, count(*) 
-- from workspace.bronze.subscriptions 
-- group by subscription_id
-- having count(subscription_id)>1;

CREATE OR REPLACE VIEW workspace.silver.payments_success_dedup AS
SELECT
    payment_id,
    subscription_id,
    TO_TIMESTAMP(paid_at, 'yyyy-MM-dd HH:mm:ss') AS paid_at,
    amount,
    currency,
    payment_status,
    `_rescued_data`,
    ingested_at,
    source_file
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY TO_TIMESTAMP(paid_at, 'yyyy-MM-dd HH:mm:ss') DESC
        ) AS rn
    FROM workspace.bronze.payments
    WHERE payment_status = 'SUCCESS'
) t
WHERE rn = 1;

-- select count(*) from workspace.bronze.payments;
-- select * from workspace.silver.payments;
-- select count(*) from workspace.bronze.payments where payment_status = 'SUCCESS';
-- select payment_id, count(*) 
-- from workspace.bronze.payments 
-- where payment_status = 'SUCCESS'
-- group by payment_id
-- having count(payment_id)>1;

CREATE OR REPLACE VIEW workspace.silver.subscriptions_active_daily AS
SELECT
    subscription_id,
    customer_id,
    plan,
    status,
    active_date
FROM (
    SELECT
        subscription_id,
        customer_id,
        plan,
        status,
        EXPLODE(
            SEQUENCE(
                GREATEST(start_date, DATE_SUB(CURRENT_DATE(), 89)),
                LEAST(
                    COALESCE(end_date, CURRENT_DATE()),
                    CURRENT_DATE()
                )
            )
        ) AS active_date
    FROM workspace.silver.subscriptions
    WHERE start_date <= CURRENT_DATE()
      AND COALESCE(end_date, CURRENT_DATE()) >= DATE_SUB(CURRENT_DATE(), 89)
) t;

-- select * from workspace.silver.subscriptions_active_daily;

