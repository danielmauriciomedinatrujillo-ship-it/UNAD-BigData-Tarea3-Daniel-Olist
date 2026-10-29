from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, sum as _sum, count, month, year

spark = SparkSession.builder.appName("OlistBatch").getOrCreate()

orders = spark.read.csv("olist_orders_dataset.csv", header=True, inferSchema=True)
items = spark.read.csv("olist_order_items_dataset.csv", header=True, inferSchema=True)
products = spark.read.csv("olist_products_dataset.csv", header=True, inferSchema=True)
customers = spark.read.csv("olist_customers_dataset.csv", header=True, inferSchema=True)
translation = spark.read.csv("product_category_name_translation.csv", header=True, inferSchema=True)

orders = orders.filter(col("order_status") == "delivered")
orders = orders.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))
orders = orders.withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))
orders = orders.withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))

items_products = items.join(products, "product_id").join(translation, "product_category_name", "left")
items_products.groupBy("product_category_name_english").agg(_sum("price").alias("total_revenue")).orderBy(col("total_revenue").desc()).show(10)

orders.withColumn("year", year("order_purchase_timestamp")).withColumn("month", month("order_purchase_timestamp")).groupBy("year", "month").agg(count("order_id").alias("total_orders")).orderBy("year", "month").show()

orders_customers = orders.join(customers, "customer_id")
orders_customers.groupBy("customer_state").agg({"delivery_days": "avg"}).orderBy("avg(delivery_days)").show(10)
