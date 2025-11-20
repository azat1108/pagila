from pyspark.sql import (
    SparkSession,
    functions as F,
    window as w
)

spark = (
    SparkSession.builder
    .appName("PagilaAnalysis")
    .config("spark.jars", "postgresql-42.7.8.jar")
    .getOrCreate()
)

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

q1_df = (film_category_df.join(category_df, on="category_id", how="inner")
                        .groupBy("name").count().orderBy("count", ascending=False)
        )                 
q2_df = (rental_df.join(inventory_df, on="inventory_id", how="inner")
                 .join(film_actor_df, on="film_id", how="inner")
                 .join(actor_df, on="actor_id", how="inner")
                 .groupBy("actor_id", "first_name", "last_name").count().orderBy("count", ascending=False).limit(10)
        )
q3_df = (payment_df.join(rental_df, on="rental_id", how="inner")
                  .join(inventory_df, on="inventory_id", how="inner")
                  .join(film_category_df, on="film_id", how="inner")
                  .join(category_df, on="category_id", how="inner")
                  .groupBy("category_id", "name").sum("amount").orderBy("sum(amount)", ascending=False).limit(1)
        )
q4_df = film_df.join(inventory_df, on="film_id", how="left_anti").select("title")
counts_df = (actor_df.join(film_actor_df, on="actor_id", how="inner")
                    .join(film_category_df, on="film_id", how="inner")
                    .join(category_df, on="category_id", how="inner")
                    .filter(F.col("name") == "Children").groupBy("actor_id", "first_name", "last_name")
                    .agg(F.count("*").alias("children_movie_count"))
            )
w = w.Window.orderBy(F.desc("children_movie_count"))
q5_df = (counts_df.withColumn("rank", F.dense_rank().over(w))
                 .filter(F.col("rank") <= 3)
                 .orderBy(F.desc("children_movie_count"), "last_name", "first_name").select("actor_id", "first_name", "last_name", "children_movie_count")
        )
q6_df = (customer_df.join(address_df, on="address_id", how="inner")
                   .join(city_df, on="city_id", how="inner")
                   .groupBy("city")
                   .agg(
                       F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_customers"),
                       F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
                   )
                   .orderBy(F.desc("inactive_customers"))
        )
rental_hours_df = rental_df.withColumn(
    "rental_hours",
    (F.unix_timestamp("return_date") - F.unix_timestamp("rental_date")) / 3600
)
joined_df = (rental_hours_df.join(inventory_df, on="inventory_id", how="inner")
                           .join(film_df, on="film_id", how="inner")
                           .join(film_category_df, on="film_id", how="inner")
                           .join(category_df, on="category_id", how="inner")
                           .join(customer_df, on="customer_id", how="inner")
                           .join(address_df, on="address_id", how="inner")
                           .join(city_df, on="city_id", how="inner")
            )
a_movies_df = joined_df.filter(F.col("title").startswith("A"))
dash_cities_df = joined_df.filter(F.col("city").contains("-"))
a_result = (a_movies_df.groupBy("city", "name")
                      .agg(F.sum("rental_hours").alias("total_hours"))
                      .orderBy(F.desc("total_hours"))
                      .groupBy("city")
                      .agg(F.first("name").alias("top_category"),
                           F.first("total_hours").alias("hours"))
            )
dash_result = (dash_cities_df.groupBy("city", "name")
                            .agg(F.sum("rental_hours").alias("total_hours"))
                            .orderBy(F.desc("total_hours"))
                            .groupBy("city")
                            .agg(F.first("name").alias("top_category"),
                                 F.first("total_hours").alias("hours"))
              )
q7_df = a_result.union(dash_result)

q1_df.show()
q2_df.show()
q3_df.show()
q4_df.show()
q5_df.show()
q6_df.show()
q7_df.show()

# spark-submit --jars postgresql-42.7.8.jar app.py
# docker exec -it postgres psql -U postgres -d pagila