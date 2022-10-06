from itertools import count
from tkinter.tix import COLUMN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/C:/Users\Dora Ritchik/Downloads/postgresql-42.5.0.jar") \
    .getOrCreate()

dfCategory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfAddress = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "address") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfCustomer = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "customer") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfPayment = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "payment") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfRental = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "rental") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfInventory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "inventory") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfFilmCategory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfCity = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "city") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfCity.join(dfAddress,dfCity['city_id'] == dfAddress['city_id'],"inner"
).join(dfCustomer,dfAddress['address_id'] == dfCustomer['address_id'],"inner"
).join(dfPayment,dfCustomer['customer_id'] == dfPayment['customer_id'],"inner"
).join(dfRental,dfPayment['rental_id'] == dfRental['rental_id'],"inner"
).join(dfInventory,dfRental['inventory_id'] == dfInventory['inventory_id'],"inner"
).join(dfFilmCategory,dfInventory['film_id'] == dfFilmCategory['film_id'],"inner"
).join(dfCategory,dfFilmCategory['category_id'] == dfCategory['category_id'],"inner"
).filter((col('name').like("%-%")) | (col('name').like("A%"))).groupBy(dfCategory["name"]).agg(F.max(dfRental['rental_id'])).sort(col('max(rental_id)').desc()).limit(1).show()
         