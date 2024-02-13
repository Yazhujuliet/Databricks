# DE 2 - ETL With Spark

## 1. Query File Directly
<details>
<Summary>Code</Summary>

### View list of Files in the Directory
``` python
%python
print(DA.paths.kafka_events)

files = dbutils.fs.ls(DA.paths.kafka_events)
display(files)
```

### Query Single File
- SELECT * FROM file_format.`/path/to/file`
- **Note**: It is using ` but not ' in the path.
- Select the content from the single 001.json file.
``` SQL
SELECT * FROM json.`${DA.paths.kafka_events}/001.json`
```

### Query a Directory of Files
- Select the whole directory and do not specify the file.
- Select the content from all 10 json files (assuming they have the same format and schema).
``` SQL
SELECT * FROM json.`${DA.paths.kafka_events}`
```

### Create View for Files
- Permanent View: As long as the user have access to the data and view, they can use this view definition.
``` SQL
CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`
```
- Temporary View: The view will only exist in the current SparkSession -> current notebook.
``` SQL
CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`
```
- CTE: Common table expressions is for short-lived, human-readable reference. Will only exist in the cell.
  - Need to run WITH and SELECT at the same time to query from the CTE.
``` SQL
WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json
```

### Extract Files as Raw String
- Extract text-based files JSON, CSV, TXT, TSV.
- Use `text` format to load each row of file as one single string column named `value`.
- Example: three columns in the original JOSN "key","value","topic" ->

|value  |
| ------------- |
| {"key":"1","value":"1","topic":"main"}  | 

### Extract the Raw Bytes and Metadata of a File
- Using `binaryFile` to query the metadata and the binary representation of the file contents.
- Specifically, the fields created will indicate the `path`, `modificationTime`, `length`, and `content`.
``` SQL
SELECT * FROM binaryFile.`${DA.paths.kafka_events}`
```
</details>

## 2. Query from External Sources (csv)
<details>
  <summary>code</summary>
  
### Create Table that read from External CSV 
- Direct query against csv will not work well.
- The header row will be extracted as a table row. All columns will be loaded as a single column and pipe-delimited (|).
``` SQL
-- NOT WORKING
SELECT * FROM csv.`${DA.paths.sales_csv}`
```

- To create Table from External Sources, Spark SQL DDL is used:
> CREATE TABLE table_identifier (col_name1 col_type1, ...) \
> USING data_source \
> OPTIONS (key1 = val1, key2 = val2, ...)\
> LOCATION = path

- We need to specify the following when create a Table using DDL for csv:
  - The column names and types
  - The file format
  - The delimiter used to separate fields
  - The presence of a header
  - The path to where this data is stored
``` sql
-- Spark SQL
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"
```

- To create in PySpark, wrap the SQL code in `spark.sql(f"""...""")` function. 
``` python
-- PySpark
%python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "{DA.paths.sales_csv}"
""")
```

### Notes: Describe Table Metadata
- No data has moved during table declaration.
- Similar to directly query the file and create a view, we just pointing to files stored in an external location.
- Make sure the Column Order is correct in the table declaration.
- Run  `DESCRIBE EXTENDED sales_csv` to show the metadata: `col_name`, `data_type`
```
DESCRIBE EXTENDED sales_csv
```

### Limits for Tables from External Data Source
- Tables in Delta Lake: will guarantee you always query the most recent version of the source data.
- Tables from External Source: you may query some older cached versions.
- If the External Source data is updated, we need to `REFRESH` the associated table to reflect the changes.
``` sql
REFRESH TABLE sales_csv
```
</details>

## 3. Extract data from SQL Database
<details>
  <summary>code</summary>

### JDBC connection with SQL Database
- General syntax for connection is:
> CREATE TABLE \
USING JDBC \
OPTIONS (\
    url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",\
    dbtable = "{jdbcDatabase}.table",\
    user = "{jdbcUsername}",\
    password = "{jdbcPassword}"
)

- Example of connecting to SQLite: (do not require a port, username, or pw)
``` sql
DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)
```

### Warning:
- Backend-configuration of the JDBC server assumes you run the notebook on a single-node cluster.
- If you run on a cluster with multiple workers, the client running in the executors will not be able to connect to the driver.

### Note:
SQL systems such as data warehouses will have custom drivers. Spark will interact with various external databases differently, but the two basic approaches can be summarized as either:
- Moving the entire source table(s) to Databricks and then executing logic on the currently active cluster
- Pushing down the query to the external SQL database and only transferring the results back to Databricks

In either case, working with very large datasets in external SQL databases can incur significant **Overhead** because of either:
- Network transfer latency associated with moving all data over the public internet
- Execution of query logic in source systems not optimized for big data queries
</details>
