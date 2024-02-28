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

## 4. Load Data into Delta Lake

<details>

### Complete Overwrites - INSERT OVERWRITE
- Use overwrites to atomically replace all of the data in a table.
- Overwrite instead of delete or recreate:
  - Much faster because it doesn’t need to list the directory recursively or delete any files
  - Old version of the table still exists; can easily retrieve the old data using Time Travel
  - Atomic operation. Concurrent queries can still read the table while you are deleting the table
  - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
- `CREATE OR REPLACE TABLE` (CRAS) statements fully replace the contents of a table each time they execute. -> **Redefine** the contents.
```sql
CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`
```

- `INSERT OVERWRITE`, similar to CRAS, but
  - Can only overwrite an existing table, **NOT** create a new one like our CRAS statement
  - Can overwrite only with new records that match the current table **schema** -- and thus can be a "safer" technique for overwriting an existing table without disrupting downstream consumers
  - Can overwrite individual partitions
  - Will **fail** if we try to change the schema.
```sql
INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`
```

### Append Rows - INSERT INTO
- `INSERT INTO` to atomically append new rows to an existing Delta table.
- This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.
- Does **NOT** prevent inserting the same records multiple times -> re-executing will results in duplicate records.
```sql
INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`
```

### Merge Updates - MERGE
- Use the `MERGE` operation to update historic users data with **updated** emails and new users.
- Benefits of `MERGE`:
  - updates, inserts, and deletes are completed as a single transaction
  - multiple conditionals can be added in addition to matching fields
  - provides extensive options for implementing custom logic
- Below query update records if the current row has a NULL email and the new row does not. All unmatched records from the new batch will be inserted.
> MERGE INTO target a \
USING source b\
ON {merge_condition}\
WHEN MATCHED THEN {matched_action}\
WHEN NOT MATCHED THEN {not_matched_action}
```sql
-- Creat a view for the Updated Records

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- For any row that is not matching with the Updated records, insert the updated values and delete the old values

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *
```

### Incremental Load - COPY INTO
- `COPY INTO` incrementally ingest data from external systems.
- Expectations:
  - Data schema should be consistent.
  - Duplicate records should try to be excluded or handled downstream.
- Over time picking up new files in the source automatically.
```sql
COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET
```
</details>

## Versioning, Optimization, Vacuuming in Delta Lake

<details>

### Create a Delta Table with History
```sql
CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;
```

### Examine Table Details
```sql
DESCRIBE EXTENDED students;
DESCRIBE DETAIL students;
```

### Explore Delta Lake Files - Delta Log
- **Records** in Delta Lake tables are stored as data in **Parquet** files.
- **Transactions** to Delta Lake tables are recorded in the `_delta_log`.
- When checking `_delta_log`, each transaction results in a new JSON file.
```python
%python
display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))
```
- To check the transaction details.
```python
display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))
```
- To check what is performed in one particular transaction log `00000000000000000007.json`.
```python
%python
display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))
```

### Compacting Small Files and Indexing - OPTIMIZE
- Small files can be combined toward an optimal size using `OPTIMIZE`.
- `OPTIMIZE` will replace existing data files by combining records and rewriting the results.
- Specify the Z-order `ZORDER` for indexing will speed up the retrieval.
```sql
OPTIMIZE students
ZORDER BY id
```

### Versioning Time Travel - HISTORY
- Review table history
```sql
DESCRIBE HISTORY students
```
- Query the previous version of the table, **NOT** recreating the previous table by undoing transactions.
```sql
SELECT * 
FROM students VERSION AS OF 3
```

### Restore Previous Version
```sql
RESTORE TABLE students TO VERSION AS OF 8 
```

### Clean Up Stale Log files
- Databricks auto cleans stale log files (>30 days by default)
- If manually clean the log, use `VACUUM` operation.
- By default, `VACUUM` prevent you from deleting files less than 7 days.
- `RETAIN 0 HOURS` to keep only the current version.
- Use the `DRY RUN` version of vacuum to print out all records to be deleted
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- print out records to be deleted
VACUUM students RETAIN 0 HOURS DRY RUN;

-- delete the files
VACUUM students RETAIN 0 HOURS
```
</details>
