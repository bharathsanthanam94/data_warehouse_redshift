'''
This is a basic example of how to connect to a PostgreSQL database and perform some basic operations.
'''

import psycopg2
from psycopg2 import sql


try:
    conn = psycopg2.connect(host="127.0.0.1",database="udacity", user="student", password="student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the database")
    print(e)

conn.autocommit = True

try:
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar,artist_name varchar, year int , album_name varchar , single Boolean);")

except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) VALUES ('Bohemian Rhapsody', 'Queen', 1975, 'A Night at the Opera', False);")
except psycopg2.Error as e:
    print("Error: Issue inserting data")
    print(e)

try:
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) VALUES ('The Beatles', 'Think for yourself', 1965, 'Rubber Soul', False);")
except psycopg2.Error as e:
    print("Error: Issue inserting data")
    print(e)

try:
    cur.execute("SELECT * FROM songs;")
except psycopg2.Error as e:
    print("Error: Issue executing data")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

cur.close()
conn.close()



        

