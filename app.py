from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("PagilaAnalysis") \
    .config("spark.jars", "postgresql-42.7.8.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://localhost:5433/pagila"
connection_props = {
    "user": "postgres",
    "password": "secret",
    "driver": "org.postgresql.Driver"
}

film_df = spark.read.jdbc(url=jdbc_url, table="film", properties=connection_props)
category_df = spark.read.jdbc(url=jdbc_url, table="category", properties=connection_props)
film_category_df = spark.read.jdbc(url=jdbc_url, table="film_category", properties=connection_props)
rental_df = spark.read.jdbc(url=jdbc_url, table="rental", properties=connection_props)
inventory_df = spark.read.jdbc(url=jdbc_url, table="inventory", properties=connection_props)
film_actor_df = spark.read.jdbc(url=jdbc_url, table="film_actor", properties=connection_props)
actor_df = spark.read.jdbc(url=jdbc_url, table="actor", properties=connection_props)
payment_df = spark.read.jdbc(url=jdbc_url, table="payment", properties=connection_props)
customer_df = spark.read.jdbc(url=jdbc_url, table="customer", properties=connection_props)
address_df = spark.read.jdbc(url=jdbc_url, table="address", properties=connection_props)
city_df = spark.read.jdbc(url=jdbc_url, table="city", properties=connection_props)

q1_df = film_category_df.join(category_df, "category_id").groupBy("name").count().orderBy("count", ascending=False)
q2_df = rental_df.join(inventory_df, "inventory_id")\
                 .join(film_actor_df, "film_id")\
                 .join(actor_df, "actor_id")\
                 .groupBy("actor_id", "first_name", "last_name") .count().orderBy("count", ascending=False).limit(10)
q3_df = payment_df.join(rental_df, "rental_id")\
                 .join(inventory_df, "inventory_id")\
                 .join(film_category_df, "film_id")\
                 .join(category_df, "category_id")\
                 .groupBy("category_id", "name").sum("amount").orderBy("sum(amount)", ascending=False).limit(1)
q4_df = film_df.join(inventory_df, "film_id", "left_anti").select("title")
q5_df = actor_df.join(film_actor_df, "actor_id")\
                 .join(film_category_df, "film_id")\
                 .join(category_df, "category_id")\
                 .filter(F.col("name") == "Children").groupBy("actor_id", "first_name", "last_name").count().orderBy("count", ascending=False).limit(3)
q6_df = customer_df.join(address_df, "address_id")\
                 .join(city_df, "city_id")\
                 .groupBy("city")\
                 .agg(
                       F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_customers"),
                       F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
                   )\
                 .orderBy(F.desc("inactive_customers"))

rental_hours_df = rental_df.withColumn(
    "rental_hours",
    (F.unix_timestamp("return_date") - F.unix_timestamp("rental_date")) / 3600
)
joined_df = rental_hours_df.join(inventory_df, "inventory_id") \
                           .join(film_df, "film_id") \
                           .join(film_category_df, "film_id") \
                           .join(category_df, "category_id") \
                           .join(customer_df, "customer_id") \
                           .join(address_df, "address_id") \
                           .join(city_df, "city_id")
a_movies_df = joined_df.filter(F.col("title").startswith("A"))
dash_cities_df = joined_df.filter(F.col("city").contains("-"))

a_result = a_movies_df.groupBy("city", "name") \
                      .agg(F.sum("rental_hours").alias("total_hours")) \
                      .orderBy(F.desc("total_hours")) \
                      .groupBy("city") \
                      .agg(F.first("name").alias("top_category"),
                           F.first("total_hours").alias("hours"))

dash_result = dash_cities_df.groupBy("city", "name") \
                            .agg(F.sum("rental_hours").alias("total_hours")) \
                            .orderBy(F.desc("total_hours")) \
                            .groupBy("city") \
                            .agg(F.first("name").alias("top_category"),
                                 F.first("total_hours").alias("hours"))


q1_df.show()
q2_df.show()
q3_df.show()
q4_df.show()
q5_df.show()
q6_df.show()
a_result.show()
dash_result.show()

# spark-submit --jars postgresql-42.7.8.jar app.py
# docker exec -it postgres psql -U postgres -d pagila