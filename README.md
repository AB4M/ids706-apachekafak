# ids706-apachekafak

## Dataset description & source
Main data: NYC TLC Yellow Taxi Trip Records (2023-01 … 2023-06), Parquet, multi-file (~1–2 GB total).<br/>
Lookup table: Taxi Zone Lookup (CSV) mapping LocationID → Borough.<br/>
Fields used: tpep_pickup_datetime, PULocationID, trip_distance, fare_amount, tip_amount, total_amount (+ lookup Borough).<br/>
Why this data: Large, columnar Parquet enables predicate/column pushdown and parallel reads; the tiny CSV enables a broadcast join; grouped aggregations exercise shuffle and Spark’s optimizer.<br/>

## Performance analysis
Plan & optimizations. The pipeline runs on Spark 3.x with Adaptive Query Execution. Early filters (2023 window, trip_distance > 0, fare_amount > 0) and column pruning are applied immediately after loading; in the Parquet scan, PushedFilters and a slim ReadSchema confirm pushdown. The zone lookup is joined via BroadcastHashJoin (with a small BroadcastExchange), avoiding large shuffles. Plans show AdaptiveSparkPlan, AQEShuffleRead (coalesced), and WholeStageCodegen.<br/>
Bottlenecks & mitigation. Runtime is dominated by multi-file Parquet decoding/scan and hash aggregation CPU. Shuffle remains small due to broadcast. We mitigate with early filtering and pruning, broadcasting the small dimension table, setting a reasonable spark.sql.shuffle.partitions, and writing partitioned Parquet (Snappy) to control small files and enable pruning on re-reads.<br/>

## Key findings
Borough mix: Manhattan consistently dominates pickups; Queens is a steady #2.<br/>
Monthly scale: The processed months show multi-million trips per month, with a stable top-3 composition.<br/>
Data quality: A small Unknown/N/A bucket remains (unmapped PULocationID), expected from a left join.<br/>
Cleaned metrics: Results are computed after removing invalid rows (non-positive distance/fare) and enriching with boroughs via a broadcast join, so aggregates reflect clean, filtered records rather than raw totals.<br/>
