from itertools import count
from tkinter.tix import COLUMN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/C:/Users\Dora Ritchik/Downloads/postgresql-42.5.0.jar") \
    .getOrCreate()

dfActor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "actor") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()


dfFilmActor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "film_actor") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()    

dfActor.join(dfFilmActor,dfActor['actor_id'] == dfFilmActor['actor_id'],"inner").groupBy(dfActor['actor_id']).count().sort(col('count').desc()).limit(10).show()

