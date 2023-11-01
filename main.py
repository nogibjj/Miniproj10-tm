"""
ETL-Query script
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query, query_best
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

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
df = spark.read.csv("/workspaces/Miniproj10-tm/master/IMDB-Movie-Data.csv", header=True, inferSchema=True)
genre_avg_rating = df.groupBy("genre").agg(
    col("genre"),
    avg(col("rating")).alias("AvgRating")
)
df.createOrReplaceTempView("movie_data")
top_rated_action_movies = spark.sql("SELECT Title, Rating FROM movie_data WHERE genre LIKE %s ORDER BY Rating DESC LIMIT 10")
