from tkinter.tix import COLUMN
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/C:/Users\Dora Ritchik/Downloads/postgresql-42.5.0.jar") \
    .getOrCreate()

dfFilmCategory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()


dfCategory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "8885Mi7890") \
    .option("driver", "org.postgresql.Driver") \
    .load()    

dfCategory.join(dfFilmCategory,dfCategory['category_id'] == dfFilmCategory['category_id'],"inner").groupBy(dfCategory['name']).count().show()


