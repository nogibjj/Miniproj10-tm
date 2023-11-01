"""
Transforms and Loads data into the local SQLite3 database
"""
import sqlite3
import csv
import os

#load the csv file and insert into a new sqlite3 database
def load(dataset="/workspaces/Miniproj10-tm/master/IMDB-Movie-Data.csv"):
    """"Transforms and Loads data into the local SQLite3 database"""

    #prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline=''), delimiter=',')
    conn = sqlite3.connect('IMDB_Movie_Data.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS IMDB_Movie_Data")
    create_table_query = """
        CREATE TABLE IF NOT EXISTS IMDB_Movie_Data (
            rank INTEGER,
            title TEXT,
            genre TEXT,
            description TEXT,
            director TEXT,
            actors TEXT,
            year INTEGER,
            runtime_minutes INTEGER,
            rating REAL,
            votes INTEGER,
            revenue_millions REAL,
            metascore INTEGER
        )
    """
    c.execute(create_table_query)

    # Insert data
    insert_query = """
        INSERT INTO IMDB_Movie_Data 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    c.executemany(insert_query, payload)
    conn.commit()
    conn.close()
    return "IMDB_Movie_Data.db"