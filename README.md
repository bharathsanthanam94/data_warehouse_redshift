# data_engineering

This project is aboutbuilding an ETL pipeline that:

1. Extracts their data from a S3 Bucket
2. Stages the data in AWS Redshift.
3. Transforms data into a set of dimensional tables using SQL statements.
This will help their analytics team to continue finding insights in what songs their users are listening to.

In order to simplify queries and enable fast aggregations, we are going to use the Star Schema using the song and event datasets. These tables will consist on:

1 Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

4 Dimension Tables

users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday
