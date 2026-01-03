from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("RDD_vs_DataFrame_Benchmark").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()

# Load dataset
df = spark.read.csv("file:///home/hduser/Downloads/online_retail_II.csv",header=True,inferSchema=True)

df.printSchema()
df.show(5)

# Rename column (space causes issues)
df = df.withColumnRenamed("Customer ID", "CustomerID")

# Remove invalid rows
df_clean = df.dropna(subset=["Invoice", "StockCode", "Quantity", "Price", "CustomerID"])

# Keep only positive quantity and price
df_clean = df_clean.filter((col("Quantity") > 0) & (col("Price") > 0))

df_clean.cache()
from pyspark.sql.functions import sum
df_revenue_country = df_clean.withColumn("Revenue", col("Quantity") * col("Price")).groupBy("Country").agg(sum("Revenue").alias("TotalRevenue")).orderBy(col("TotalRevenue").desc())

df_revenue_country.show(10)


df_time = df_clean.withColumn("Year", year("InvoiceDate")).withColumn("Month", month("InvoiceDate"))

df_monthly = df_time.withColumn("Revenue", col("Quantity") * col("Price")).groupBy("Year", "Month").sum("Revenue").orderBy("Year", "Month")

df_monthly.show()

rdd = df_clean.rdd
rdd_country_revenue = (rdd.map(lambda row: (row.Country, row.Quantity * row.Price)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False))

for row in rdd_country_revenue.take(10):
    print(row)
rdd_product_sales = (rdd.map(lambda r: (r.Description, r.Quantity)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False))

rdd_product_sales.take(10)

print("Initial partitions:", df_clean.rdd.getNumPartitions())
df_repart = df_clean.repartition(8)
df_repart.cache()

df_repart.count()  # materialize cache
print("New partitions:", df_repart.rdd.getNumPartitions())

