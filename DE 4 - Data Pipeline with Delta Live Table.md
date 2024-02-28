# Data Pipelines

## 1. Medallion Architecture
<details>

### Metal Layers
1. **BRONZE**: Raw ingestion and history
2. **SILVER**: Filtered, cleaned, augmented
3. **GOLD**: Business-level aggregates

### Bronze Layer
- Raw copy of ingested data
- Replace traditional data lake
- Provides efficient storage and querying of full, unprocessed history of data

### Silver Layer
- Reduces data storage complexity, latency and redundancy
- Optimize ETL and analytic query performance
- Preserve grain of original data (NO aggregations)
- Eliminates duplicates
- Production schema enforced
- Data quality checks
- May contain more than one table

### Gold Layer
- Powers ML applications, reporting, dashboards, ad hoc analytics
- Refined views of data (WITH aggregations)
- Reduce strain on production systems
- Optimized query performance for business-critical data



</details>
  
