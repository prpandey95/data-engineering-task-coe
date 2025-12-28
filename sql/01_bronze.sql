CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE TABLE bronze.customers
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingested_at,
    _metadata.file_path      AS source_file
FROM read_files(
    '/Volumes/workspace/default/raw_csv/customers.csv',
    format => 'csv',
    header => true
);


CREATE OR REPLACE TABLE bronze.subscriptions
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingested_at,
    _metadata.file_path AS source_file
FROM read_files(
    '/Volumes/workspace/default/raw_csv/subscriptions.csv',
    format => 'csv',
    header => true
);


CREATE OR REPLACE TABLE bronze.payments
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingested_at,
    _metadata.file_path      AS source_file
FROM read_files(
    '/Volumes/workspace/default/raw_csv/payments.csv',
    format => 'csv',
    header => true
);
