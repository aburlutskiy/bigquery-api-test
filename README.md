# Google BigQuery External Table Testing

This repository is intended for testing the consistency and correctness of Google BigQuery when dealing with external tables and queries. It focuses on scenarios where external table data might change during query execution and verifies that the query produces expected results despite those changes. Please note that the test data appears to be parameterized but doesn't seem to use the parameterization feature in this specific example.

## Test Steps:

The test performs the following steps:

```sql
WITH tq AS (SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY indicator ORDER BY value) AS rank,
                   SUM(value) OVER (PARTITION BY indicator) AS s
            FROM test_dataset.ml1800s)
SELECT indicator, s
FROM tq
WHERE rank = 1
ORDER BY indicator
LIMIT 5
```
* Initiates an analytic query to BG external table asynchronously using CompletableFuture
* In a loop, it periodically updates 'location' attribute to simulate changes to an external table while the query is running.
* It sleeps for 2 seconds between each update to avoid hitting Google API's thresholds
* After the query completes (or a timeout is reached), it performs several assertions to verify the test results:
  * It checks that the query didn't complete exceptionally.
  * It checks that the query wasn't canceled.
  * It checks whether the query output met the specified expectations using performQueryCommand.isMetExpectation().

Test Pre-Install steps:

* Make 18M csv data file by cloning a snippet of data
* Upload the latter to Google Cloud Storage
```sql
CREATE OR REPLACE EXTERNAL TABLE `test_dataset.ml1800` (
  id STRING,
  t INTEGER,
  indicator STRING,
  value INTEGER,
  c1 STRING,
  c2 STRING,
  c3 STRING,
  c4 STRING
)
OPTIONS (
  format ="CSV",
  uris = ['gs://sandbox-datalake/test_dataset/ml1800/*']
);
```
* Create an external table in BigQuery
