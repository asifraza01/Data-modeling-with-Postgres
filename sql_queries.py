# DROP TABLES

#song_table_drop = "DROP TABLE IF EXISTS songs" 
song_table_drop = "DROP TABLE IF EXISTS songs" 
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
user_table_drop = "DROP TABLE IF EXISTS users"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"

# CREATE TABLES
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(artist_id varchar , duration float, song_id varchar PRIMARY KEY,
title varchar , year int);
""")

#artist_id, artist_latitude, artist_location, artist_longitude, artist_name

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR PRIMARY KEY, artist_latitude FLOAT, artist_location VARCHAR, artist_longitude FLOAT, artist_name VARCHAR );
""")

#ts	year	month	date	hour	weekofyear	weekdayname

#artist_id, duration,song_id, title ,year

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time BIGINT, 
year INT, 
month INT, 
day INT, 
hour INT, 
week INT, 
weekday varchar,
PRIMARY KEY (start_time))
""")

#	firstName	gender	lastName	level	userId
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
first_name VARCHAR, 
gender VARCHAR, 
last_name VARCHAR, 
level VARCHAR,
user_id INT, 
PRIMARY KEY (user_id))
""")


#timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to
#row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial,
start_time BIGINT NOT NULL, 
user_id INT NOT NULL, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id VARCHAR, 
location VARCHAR, 
user_agent VARCHAR,
PRIMARY KEY (songplay_id))
""")

# INSERT RECORDS

song_table_insert = ("""
INSERT INTO songs(artist_id, duration,song_id, title ,year) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO UPDATE SET title=songs.title, artist_id=songs.artist_id,
year=songs.year, duration=songs.duration
""")

 
artist_table_insert = ("""
INSERT INTO artists(artist_id,artist_latitude,artist_location,artist_longitude,artist_name) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO UPDATE SET artist_name = artists.artist_name, artist_location = artists.artist_location, artist_latitude = artists.artist_latitude, artist_longitude = artists.artist_longitude
""")

time_table_insert = ("""
INSERT INTO time(start_time,  year, month , day , hour, week, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) DO UPDATE SET hour=time.hour, day=time.day, week=time.week, month=time.month, 
year=time.year, weekday=time.weekday
""")

user_table_insert = ("""
INSERT INTO users(first_name,  gender, last_name , level , user_id) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE SET first_name=users.first_name, last_name=users.last_name, gender=users.gender, level=users.level
""")



songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s, artists a
WHERE s.artist_id = a.artist_id
AND s.title = %s AND a.artist_name = %s AND s.duration = %s
""")



# QUERY LISTS

create_table_queries = [song_table_create,artist_table_create,time_table_create, user_table_create, songplay_table_create]

#create_table_queries = [song_table_create]

#drop_table_queries = [songplay_table_drop, #user_table_drop,song_table_drop, artist_table_drop, time_table_drop]
#

drop_table_queries = [song_table_drop,artist_table_drop,time_table_drop, user_table_drop,songplay_table_drop]
