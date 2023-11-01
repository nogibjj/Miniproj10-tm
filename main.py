"""
ETL-Query script
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query, query_best
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Extract
print("Extracting data...")
extract()

# Transform and load
print("Transforming data...")
load()

# Query
print("Querying data...")
query()

# Query best movie
print("Querying top 3 movies...")
query_best()


spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()
# Define the schema based on your column names and data types
schema = StructType([
    StructField("Rank", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Director", StringType(), True),
    StructField("Actors", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Runtime (Minutes)", IntegerType(), True),
    StructField("Rating", DoubleType(), True),
    StructField("Votes", IntegerType(), True),
    StructField("Revenue (Millions)", DoubleType(), True),
    StructField("Metascore", DoubleType(), True)
])

# Read the CSV file with the defined schema
df = spark.read.csv("/workspaces/Miniproj10-tm/master/IMDB-Movie-Data.csv", header=True, schema=schema)
# df = spark.read.csv("/workspaces/Miniproj10-tm/master/IMDB-Movie-Data.csv", header=True, inferSchema=True)
genre_avg_rating = df.groupBy("Genre").agg(
    col("Genre"),
    avg(col("Rating")).alias("AvgRating")
)
genre_avg_rating.show()
df.createOrReplaceTempView("movie_data")
df.show()
spark.stop()
