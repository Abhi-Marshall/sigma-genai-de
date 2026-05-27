# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it through bronze, silver, and gold layers, and finally computes merchant performance and daily summaries.

## Data Flow Diagram
```
+----------------+        +--------------------+        +---------------------+        +-------------------------+
| TRANSACTIONS   |        | bronze_transactions|        | silver_transactions |        | gold_merchant_performance|
| (Clean & Dirty)| -----> | (Raw Data)         | -----> | (Enriched Data)     | -----> | (Aggregated Merchant)  |
+----------------+        +--------------------+        +---------------------+        +-------------------------+
                                                                                     |
                                                                                     |
                                                                                     |
                                                                                     |
+----------------+        +--------------------+        +---------------------+        +-------------------------+
| TRANSACTIONS   |        | bronze_transactions|        | silver_transactions |        | gold_daily_summary       |
| (Clean & Dirty)| -----> | (Raw Data)         | -----> | (Enriched Data)     | -----> | (Daily Aggregates)       |
+----------------+        +--------------------+        +---------------------+        +-------------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a bronze-silver-gold approach to ensure data quality and enrichment at each stage.
- **Quality Flags**: Introduced quality flags in the silver layer to distinguish between clean and dirty data.
- **Aggregation**: Aggregations are performed in the gold layer to provide insights on merchant performance and daily summaries.
- **Data Enrichment**: Merchant details are joined in the silver layer to enrich transaction data.

## Known Limitations
- **Single-threaded Processing**: The pipeline processes data in a single thread, which may not be optimal for very large datasets.
- **No Error Handling**: The pipeline lacks robust error handling mechanisms, which could lead to data loss in case of failures.
- **Static Schema**: The schema is static and does not support dynamic changes in the data structure.
- **Limited Data Sources**: The pipeline currently only supports two data sources (clean and dirty transactions).

## Dependencies
- **DuckDB**: The database used to store and process data.
- **MERCHANTS**: A list of merchant details used for data enrichment.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing transaction records.