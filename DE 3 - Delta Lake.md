# Delta Lake

## 1. Definition
<details>
  
- Delta Lake: an open-source project that enables building a data lakehouse on top of existing cloud storage.
- Delta Lake is NOT:
  - Proprietary technology
  - Storage format
  - Storage medium
  - Database service or data warehouse
- Delta Lake IS:
  - Open Source
  - Builds upon standard data formats
  - Optimized for **cloud object storage**
  - Built for scalable metadata handling
- Objective of designing Delta Lake: quickly returning point query in most largest and changing dataset.
- Delta Lake brings **ACID** to object storage
  - ***Atomicity***: all transactions either succeed or fail completely.
  - ***Consistency***: guarantees relate to how a given state of the data is observed by simultaneous operations.
  - ***Isolation***: refer to how simultaneous operations conflict with one another. The isolation guarantees that Delta Lake provides do differ from other systems.
  - ***Durability***: commited changes are permanent.
- Problems solved by ACID:
  - Hard to append data -> _Consistency_
  - Modification of existing data difficult -> _Atomicity_
  - Jobs failling mid way -> _Atomicity_
  - Real-time operations hard -> _Atomicity_
  - Costly to keep historical data versions -> _Atomicity_
- Delta Lake is the **default format** for Tables created in Databricks.
  
```
-- By default
CREATE TABLE table USING DELTA
df.write.format("delta")
```
</details>

## 2. Schema and Tables

<details>

### Create Schema\Database
- If not specify location, Location of the first schema (database) is in the default location under `dbfs:/user/hive/warehouse/<schema_name>.db/`
```sql
CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;
```
- Check the metadata of the Schema will see `dbfs:/user/hive/warehouse/yzhu2_emfx_da_delp_default_location.db`
```sql
DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;
```

### Create Table and Insert Data to the Schema

```sql
USE ${da.schema_name}_default_location;  -- USE SCHEMA_NAME;

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;
```

### Look at Table Description/Metadata
```sql
DESCRIBE DETAIL managed_table;
```

### Drop Table - data deleted
- The table's directory and its log and data files are deleted. But the schema (database) directory remains.
```sql
DROP TABLE managed_table;
```

### Create External Table to the Schema
- Create an External/unmanaged table. 
```sql
USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);

-- Use LOCATION to define custom directory location
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 
```

### Drop the External Table - data retain
```sql
DROP TABLE external_table;
```
- The table definition no longer exists in the metastore, but the underlying data remain intact as **Parquet** file.
```python
%python 
tbl_path = f"{DA.paths.working_dir}/external_table"
files = dbutils.fs.ls(tbl_path)
display(files)
```

### Drop Schema
```sql
DROP SCHEMA ${da.schema_name}_default_location CASCADE;
```
</details>


## 3. Set Up Delta Tables

<details>

### Create Table As Select (CTAS)
- CTAS does not support manual schema declaration.
- CTAS are useful for external data ingestion with well-defined schema, such as Parquet & Tables.
```sql
CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;
```

### CTAS based on Temp View
```sql
CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta
```

### Generated Columns
- `date` is a generated column, Delta Lake auto-compute them.
```sql
CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")
```

### Merge statement
- With Delta table, no need to `REFRESH TABLE`.
```sql
SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *
```

### CHECK table Constraint
- `NOT NULL` constraint
- `CHECK` constraint
- `CHECK` constraint works like a `WHERE` clause
```sql
ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');
```

### Additional Options and Metadata
- `SELECT` build-in Spark SQL commands:
  - `current_timestamp()` records the timestamp when the logic is executed
  - `input_file_name()` records the source data file for each record in the table
- `CREATE TABLE`:
  - `COMMENT` added to allow for easier discovery of table contents
  - `LOCATION` specified, which will result in an external (rather than managed) table
  - `PARTITIONED BY` a date column; this means that the data from each data will exist within its own directory in the target storage location
- Most Delta Lake tables will **NOT** benefit from partitioning -> separate data files, result in a small files problem and prevent file compaction and efficient data skipping.
```sql
CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;
```
### Cloning Delta Lake Tables
- `DEEP CLONE` fully copies data and metadata from a source table.
```sql
CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases
```
- `SHALLOW CLONE` copies the Delta transaction logs, data doesn't move. Good for creating a copy of a table quickly to test out applying changes without the risk of modifying the current table.

</details>
