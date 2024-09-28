import cassandra
from cassandra.cluster import Cluster
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

# connect to the keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# create tables based on a query in cassandra
# my question is I want to extract every album in my music library that was released in a given year


query = "CREATE TABLE IF NOT EXISTS music_library"
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# my second query is I want every album in my music library that was created by a given artist

query = "CREATE TABLE IF NOT EXISTS album_library"
query = query + "(artist_name text, year int, album_name text, PRIMARY_KEY (artist_name, year))"

try:
    session.execute(query)
except Exception as e:
    print(e)


# my third query is given all the information from the music library about a given album

query = "CREATE TABLE IF NOT EXISTS album_library"
query =query + "(album_name text, artist_name text, year int, PRIMARY KEY (album_name, artist_name))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into the tables

query = "INSERT INTO music_library (year, artist_name, album_name) VALUES (%s, %s, %s)"
query1= "INSERT INTO album_library (artist_name, year, album_name) VALUES (%s, %s, %s)"
query2= "INSERT INTO album_library (album_name, artist_name, year) VALUES (%s, %s, %s)"

try:
    session.execute(query, (1965, 'The Beatles', 'Rubber Soul'))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, 'The Beatles', 'Let It Be'))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, 'The Who', 'My Generation'))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, 'The Monkees', 'The Monkees'))
except Exception as e:
    print(e)


try:
    session.execute(query, (1970, 'The Carpenters', 'Close To You'))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Beatles", 1970, "Let It Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)

    
try:
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)

#validate our model
query = "SELECT *FROM music_library WHERE year=1970"

try:
    rows = session.execute(query)
    for row in rows:
        print(row)
except Exception as e:
    print(e)
for row in rows:
    print(row.year, row.artist_name, row.album_name)

# validate our data model
query = "SELECT *FROM album_library WHERE artist_name='The Beatles'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist_name, row.year, row.album_name)

# validate our data model
query = "SELECT *FROM album_library WHERE album_name='Close To You'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row.album_name, row.artist_name, row.year)

for t in ["music_library", "album_library", "artist_library"]:
    query = f"DROP TABLE {t}"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

session.shutdown()
cluster.shutdown()