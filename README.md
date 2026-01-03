RDD vs DataFrame Transformation Benchmarking Report

This activity evaluates the performance and analytical capabilities of Apache Spark using RDD and DataFrame APIs on an e-commerce transaction dataset. The dataset includes invoice details, product information, customer identifiers, quantities,
prices, and country-level sales data.
Initially, the dataset was loaded into Spark and cleaned by removing null values and invalid records. Columns were standardized to enable efficient querying and aggregation. Both RDDs and DataFrames were then used to perform analytical
transformations such as revenue calculation, grouping, sorting, and trend analysis.

Using the DataFrame API, revenue per country and monthly revenue trends were computed using built-in SQL functions. These operations were concise, readable, and executed efficiently. In contrast, the RDD API required explicit mapping and 
reduction logic, increasing code complexity and execution overhead. While RDDs provide low-level control, they lack automatic optimization.

Performance tuning was applied through caching and repartitioning. Repartitioning the dataset improved parallelism, and caching reduced recomputation overhead for repeated actions. Spark UI analysis revealed that DataFrame-based jobs executed 
with fewer stages and lower shuffle sizes compared to RDD jobs. This efficiency is due to Spark’s Catalyst Optimizer, which restructures logical plans, and Tungsten execution, which improves memory management.

RDD operations exhibited higher task counts and longer execution times, particularly during shuffle-heavy operations such as reduceByKey. DataFrame jobs showed improved CPU utilization and faster completion times. These findings confirm that 
DataFrames are more suitable for structured analytics on large datasets.

In conclusion, while RDDs offer flexibility for complex low-level transformations, DataFrames outperform them in analytical workloads due to optimization, ease of use, and better resource utilization. For large-scale data processing and 
business analytics, DataFrames are the preferred Spark abstraction.
