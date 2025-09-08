# Getting Started with Apache Iceberg

Start your terminal with Apache Spark 3.5 and Apache Iceberg 1.9.2

```
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/Users/avocadodata/data/apache-iceberg/warehouse \
    --conf spark.sql.defaultCatalog=local
```

## On the Spark shell: 

Store sample data into delta lake table format: 

```
val data = Seq(("1", "Kayako", "IN"), ("2", "Zomato", "IN"), ("3", "Google", "US"), ("4", "Tesla", "US"), ("5", "Nippon", "JP"))
val schema = Seq("id", "name", "country")
val rdd = spark.sparkContext.parallelize(data)
val df = rdd.toDF(schema:_*)
```

## DF show
```
scala> df.show(false)
+---+------+-------+
|id |name  |country|
+---+------+-------+
|1  |Kayako|IN     |
|2  |Zomato|IN     |
|3  |Google|US     |
|4  |Tesla |US     |
|5  |Nippon|JP     |
+---+------+-------+
```

# Let's store into Apache Iceberg table format, 

we have already provided the catalog as `local` and it does not needs path explicitly. 

```
df.writeTo("local.db.products").create()
```


## Read back the Apache Iceberg table
You read data in your Delta table by specifying the catalog `local`, schema (default `db`) and dataset name as `products`

### Using the Spark SQL
```
val icebergTable = spark.sql("SELECT * FROM local.db.products")
icebergTable.show(false)
```

### Using DataFrame API
```
spark.table("local.db.products").show(false)
```

### insert data into existing table
```
spark.sql("INSERT INTO local.db.products VALUES (6, 'Avocado', 'IN'), (7, 'NAUKRI', 'IN')")
```

### Read back 
```
scala> spark.table("local.db.products").show(false)
+---+-------+-------+
|id |name   |country|
+---+-------+-------+
|6  |Avocado|IN     |
|1  |Kayako |IN     |
|7  |NAUKRI |IN     |
|2  |Zomato |IN     |
|3  |Google |US     |
|4  |Tesla  |US     |
|5  |Nippon |JP     |
+---+-------+-------+
```

### Update the existing record syntax, Update the existing records 

Let's update the id: 4, country to IN

https://iceberg.apache.org/docs/latest/spark-writes/#update
```
spark.sql("UPDATE local.db.products SET country = 'IN' WHERE id = '4'")
spark.table("local.db.products").show(false)
```

### Updated using the MERGE INTO statements

Create a table: `temp_products` as 

```
val data = Seq(
  ("1", "KayakoUpdated", "IN"),
  ("5", "Nippon", "EU")
).toDF("id", "name", "country")
data.writeTo("local.db.temp_products").create()
spark.table("local.db.temp_products").show(false)
spark.sql("SELECT id, name, country FROM local.db.temp_products").show(false)
```

https://iceberg.apache.org/docs/latest/spark-writes/#merge-into-syntax

### MERGE INTO
MERGE INTO local.db.products t   -- a target table
USING (SELECT ...) s          -- the source updates
ON t.id = s.id                -- condition to find updates for target rows
WHEN MATCHED AND s.op = 'delete' THEN DELETE
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *

### In case of our, consider only updates
```
spark.sql("MERGE INTO local.db.products t USING (SELECT id, name, country FROM local.db.temp_products) s ON t.id = s.id  WHEN MATCHED THEN UPDATE SET t.name = s.name, t.country=s.country")
```

### Consider income table has updates of existing records and new records which we want to insert. 

Update two records and add two new records as well.
```
val data = Seq(
  ("1", "Kayako", "IN"),
  ("5", "Nippon", "IN"),
  ("8", "M&M", "IN"),
  ("9", "K&K", "US")
).toDF("id", "name", "country")
data.writeTo("local.db.temp_products2").create()
spark.table("local.db.temp_products2").show(false)
spark.sql("SELECT id, name, country FROM local.db.temp_products2").show(false)
```

### Updated the actual products tabale from above temp table
```
spark.sql("MERGE INTO local.db.products t USING (SELECT id, name, country FROM local.db.temp_products2) s ON t.id = s.id  WHEN MATCHED THEN UPDATE SET t.name = s.name, t.country=s.country WHEN NOT MATCHED THEN INSERT *")
```

```
spark.sql("MERGE INTO local.db.products t USING (SELECT id, name, country FROM local.db.temp_products2) s ON t.id = s.id  WHEN MATCHED THEN UPDATE SET t.name = s.name, t.country=s.country WHEN NOT MATCHED THEN INSERT *")
```

### Since update statements, providing each column, slightly looks odd, we can use * instead if we want to update all columns with the source table conent. 

```
val data = Seq(
  ("1", "Kayako", "US"),
  ("8", "M&M", "IN"),
  ("9", "K&K", "UK"),
  ("10", "L&L", "JP")
).toDF("id", "name", "country")
data.writeTo("local.db.temp_products3").create()
spark.table("local.db.temp_products3").show(false)
```
show
```
+---+------+-------+                                                            
|id |name  |country|
+---+------+-------+
|1  |Kayako|US     |
|8  |M&M   |IN     |
|9  |K&K   |UK     |
|10 |L&L   |JP     |
+---+------+-------+
```

### Updates with * in SET and INSERT * 
```
spark.sql("MERGE INTO local.db.products t USING (SELECT id, name, country FROM local.db.temp_products3) s ON t.id = s.id  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
```


### DELETE
In case of delete event and we want to capture in the Apache Iceberg table. In that case we need to store a extra flag in the temp table and then we can write the merge query as below: 
```
MERGE INTO local.db.products t   -- a target table
USING (SELECT ...) s          -- the source updates
ON t.id = s.id                -- condition to find updates for target rows, if composite key then all should be included.
WHEN MATCHED AND s.event = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
``` 

Let's create a temp table with op column, whee we can capture the nature of records. 

```
val data = Seq(
  ("1", "Kayako", "IN", "update"),
  ("8", "M&M", "IN", "delete"),
  ("10", "L&L", "JP", "delete"),
  ("11", "S&S", "JP", "insert"),
).toDF("id", "name", "country", "event")
data.writeTo("local.db.temp_products4").create()
spark.table("local.db.temp_products4").show(false)
```
Include this temp table in the merge query to update the main table as below: 

```
spark.sql("MERGE INTO local.db.products t USING (SELECT * FROM local.db.temp_products4) s ON t.id = s.id WHEN MATCHED AND s.event = 'delete' THEN DELETE WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
``` 


# Contact US
For Expert implementation of Apache Iceberg as an Data lake or Lake house in your organization using the Apache Spark + Scala/Python on any cloud provider:

Email us at: avocado.datalake@gmail.com

Or Visit us at: https://www.dwh.co.in/