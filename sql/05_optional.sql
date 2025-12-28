-- Time Travel
SELECT *
FROM workspace.bronze.subscriptions VERSION AS OF 3;

SELECT *
FROM workspace.silver.subscriptions TIMESTAMP AS OF '2025-01-01 10:00:00';

DESCRIBE HISTORY workspace.silver.subscriptions_dedup;



