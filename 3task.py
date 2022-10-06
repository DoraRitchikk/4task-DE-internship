from itertools import count
from tkinter.tix import COLUMN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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


dfFilmCategory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()    

dfFilm = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "film") \
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

dfRental = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "rental") \
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

dfCategory.join(dfFilmCategory,dfCategory['category_id'] == dfFilmCategory['category_id'],"inner"
).join(dfFilm,dfFilmCategory['film_id'] == dfFilm['film_id'],"inner"
).join(dfInventory,dfFilm['film_id'] == dfInventory['film_id'],"inner"
).join(dfRental,dfInventory['inventory_id'] == dfRental['inventory_id'],"inner"
).join(dfPayment,dfRental['rental_id'] == dfPayment['rental_id'],"inner"
).groupBy(dfCategory["name"]
).agg({"amount": "max"}).sort(col('max(amount)').desc()).limit(1).show()
