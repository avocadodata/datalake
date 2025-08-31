# Getting Started with Delta Lake



Start your terminal with Apache Spark 3.5 and Delta Lake 3.1.0

```
spark-shell \
--packages io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

On the Spark shell: 

```
import io.delta.tables._
import org.apache.spark.sql.functions.
```

Store sample data into delta lake table format: 

```
val data = Seq(("1", "Kayako", "IN"), ("2", "Zomato", "IN"), ("3", "Google", "US"), ("4", "Tesla", "US"), ("5", "Nippon", "JP"))
val schema = Seq("id", "name", "country")
val rdd = spark.sparkContext.parallelize(data)
val df = rdd.toDF(schema:_*)

<!-- Let's store in Delta lake storage format -->
val basePath = "file:///Users/avocadodata/data/delta-lake/no-partition/getting-started/"

df.write.format("delta").mode("overwrite").save(basePath)

```

## Read data
You read data in your Delta table by specifying the basepath

```
val readdf = spark.read.format("delta").load(basePath)
readdf.show()
```

## Getting Started with Delta Lake - a partitioned table with an existing column:

```
val partitionBasePath = "file:///Users/avocadodata/data/delta-lake/partition/getting-started/"
<!-- Store in partition as country -->
df.write.format("delta").partitionBy("country").mode("overwrite").save(partitionBasePath)

```

# Getting Started with Delta Lake - A creation of a derived column that will serve as the partition column:

<!-- Derive the partition column from exist column -->
<!-- Like you want to create only 100 (here I will create 3) partition based on the id -->
// Cast column "id" to long
```
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
val partitionLabel = "_partition"
val cdf = df.withColumn("id", col("id").cast(LongType))
val fdf = cdf.withColumn(partitionLabel, col("id") % 3)
<!-- Store into delta lake format -->
val basePathCustomPartition = "file:///Users/avocadodata/data/delta-lake/cusotom_partition"
fdf.write.format("delta").partitionBy(partitionLabel).mode("overwrite").save(basePathCustomPartition)
```

# Contact US for data lake and consulting and implementation into your organization.

Email us at: avocado.datalake@gmail.com
