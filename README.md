[![Build Status](https://github.com/nogibjj/Miniproj10-tm/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Miniproj10-tm/actions)

# Mini Project 10

## Data

- The dataset used for this project is "IMDB_Movie_Data.csv," which contains movie information.

### Overview:

* Extracts a dataset from 'https://raw.githubusercontent.com/laxmimerit/All-CSV-ML-Data-Files-Download/master/IMDB-Movie-Data.csv'
* Transforms the data by cleaning, filtering, enriching, etc to get it ready for analysis.
* Loads the transformed data into MySQL database.
* Preforms CRUD operations: Create, Read, Update, and Delete
* Writes and executes SQL queries on the database to analyze and retrieve insights from the data.
* Uses PySpark to perform data analysis on a dataset of movie data.

### Explanation of the PySpark section in `main.py`
* Initiates a Spark session using PySpark. It sets the application name to "IMDBAnalysis" for identification in the Spark cluster.
* Defines a schema for the data to be read from the CSV file. It specifies the data types for each column, which is essential for ensuring the data is interpreted correctly.
* Reads the CSV file located at the specified path using the defined schema.
* Calculates the average rating for each genre in the dataset. It groups the data by the "Genre" column, computes the average of the "Rating" column for each group, and assigns an alias "AvgRating" to the resulting column. Finally, it displays the results using show().
* Creates a temporary SQL view named "movie_data" for the DataFrame df, allowing you to perform Spark SQL queries. Then, it displays the content of the DataFrame using show().

### Spark SQL Dataframe

<img width="901" alt="Screenshot 2023-11-01 at 4 48 36 PM" src="https://github.com/nogibjj/Miniproj10-tm/assets/141086024/955ada0a-bda6-4a63-9606-02f6c3813b0c">

### Spark SQL Ratings By Genre

<img width="450" alt="Screenshot 2023-11-01 at 4 48 02 PM" src="https://github.com/nogibjj/Miniproj10-tm/assets/141086024/8fdfadaa-a2da-4720-b55b-588006428d8d">

#### Queries
* query() prints the top 5 rows of movies based on the given rank
* query_best() allows the user to input the genre of their choice and returns the best 3 movie's rating, metascore, title, genre, description, and actors  based on rating

### Example of usage of MySQL:

<img width="940" alt="Screen Shot 2023-09-30 at 3 07 35 PM" src="https://github.com/tommymmcguire/sqlite-lab-mcg/assets/141086024/80201295-0b4f-474f-b2aa-7320cfe48b9b">


