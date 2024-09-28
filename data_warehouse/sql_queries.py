import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              CHAR(1),
    itemInSession       INTEGER,
    lastName            VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            TEXT,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    sessionId           INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  BIGINT,
    userAgent           TEXT,
    userId              INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     TEXT,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time          TIMESTAMP,
    user_id             INTEGER,
    level               VARCHAR,
    song_id             VARCHAR,
    artist_id           VARCHAR,
    session_id          INTEGER,
    location            TEXT,
    user_agent          TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id             INTEGER NOT NULL PRIMARY KEY,
    first_name          VARCHAR,
    last_name           VARCHAR,
    gender              CHAR(1),
    level               VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id             VARCHAR NOT NULL PRIMARY KEY,
    title               VARCHAR,
    artist_id           VARCHAR,
    year                INTEGER,
    duration            FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id           VARCHAR NOT NULL PRIMARY KEY,
    name                VARCHAR,
    location            TEXT,
    latitude            FLOAT,
    longitude           FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time          TIMESTAMP NOT NULL PRIMARY KEY,
    hour                INTEGER,
    day                 INTEGER,
    week                INTEGER,
    month               INTEGER,
    year                INTEGER,
    weekday             VARCHAR);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from '{}'
credentials 'aws_iam_role={}'
format as json '{}'
region 'us-west-2'
""").format(
        config.get('S3', 'LOG_DATA'),
        config.get('IAM_ROLE', 'ARN'),
        config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs
from '{}'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2'
""").format(
        config.get('S3', 'SONG_DATA'),
        config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT (e.userId) AS user_id,
    e.firstName AS first_name,
    e.lastName AS last_name,
    e.gender AS gender,
    e.level AS level
FROM staging_events e
WHERE user_id IS NOT NULL AND PAGE = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT (s.song_id) AS song_id,
    s.title AS title,
    s.artist_id AS artist_id,
    s.year AS year,
    s.duration AS duration
FROM staging_songs s
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
    DISTINCT (s.artist_id) AS artist_id,
    s.artist_name AS name,
    s.artist_location AS location,
    s.artist_latitude AS latitude,
    s.artist_longitude AS longitude
FROM staging_songs s
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(start_time) AS start_time,
    EXTRACT (hour FROM start_time) AS hour,
    EXTRACT (day FROM start_time) AS day,
    EXTRACT (week FROM start_time) AS week,
    EXTRACT (month FROM start_time) AS month,
    EXTRACT (year FROM start_time) AS year,
    EXTRACT (weekday FROM start_time) AS weekday
FROM songplays
""")

# ANALYTICAL QUERIES
get_number_staging_events = "SELECT COUNT(*) FROM staging_events"
get_number_staging_songs = "SELECT COUNT(*) FROM staging_songs"
get_number_songplays = "SELECT COUNT(*) FROM songplays"
get_number_users = "SELECT COUNT(*) FROM users"
get_number_songs = "SELECT COUNT(*) FROM songs"
get_number_artists = "SELECT COUNT(*) FROM artists"
get_number_time = "SELECT COUNT(*) FROM time"

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
test_number_rows_queries = [
    get_number_staging_events,
    get_number_staging_songs,
    get_number_songplays,
    get_number_users,
    get_number_songs,
    get_number_artists,
    get_number_time,
]