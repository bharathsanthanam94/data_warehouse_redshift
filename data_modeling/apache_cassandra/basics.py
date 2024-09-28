import cassandra
from cassandra.cluster import Cluster
# Connect to the Cassandra cluster
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)

# create a keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    {'class' : 'SimpleStrategy', 'replication_factor': 1}
    """
    )
except Exception as e:
    print(e)

#connect to the keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# we need more information about the table to create a table
# I woud like to get every album that was released in a particular year

query = "CREATE TABLE IF NOT EXISTS songs"
query += "(year int, song_title text, artist_name text, album_name text, single boolean, PRIMARY KEY (year,artist_name))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into the table
query = "INSERT INTO songs (year, song_title, artist_name,album_name, single)"
query += "VALUES (%s, %s, %s, %s, %s)"
try:
    session.execute(query, (1970, 'Across the Universe', 'The Beatles', 'Let it Be', False))
except Exception as e:
    print(e)
try:
    session.execute(query, (1965, 'Think for yourself', 'The Beatles', 'Rubber Soul', False))
except Exception as e:
    print(e)

# validate the data
query = "SELECT * FROM songs"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.song_title, row.artist_name, row.album_name, row.single)

#close our connection and the cluster
cluster.shutdown()
session.shutdown()
