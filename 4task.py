from itertools import count
from tkinter.tix import COLUMN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/C:/Users\Dora Ritchik/Downloads/postgresql-42.5.0.jar") \
    .getOrCreate()

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


dfFilm.join(dfInventory,how='left_anti',on=['film_id']).show()
