import os
import glob
import configparser
import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA                    = config.get('S3','LOG_DATA')
LOG_JSONPATH                = config.get('S3','LOG_JSONPATH')
SONG_DATA                   = config.get('S3','SONG_DATA')
ARN                         = config.get('IAM_ROLE','ARN')
KEY                         = config.get('AWS','KEY')
SECRET                      = config.get('AWS','SECRET')
S3                          = boto3.resource('s3',
                               region_name="us-west-2",
                                aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                )
Songs_Bucket                = S3.Bucket("udacity-dend")
IAM                         = boto3.client('iam',aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET,
                                 region_name='us-west-2')
client                      = boto3.client('s3',
                                    region_name="us-west-2",
                                    aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                )


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (artist varchar, auth varchar, firstName varchar, gender varchar, itemInSession integer, lastName varchar, length numeric, level varchar, location varchar, method varchar, page varchar, registration numeric, sessionId integer, song varchar, status integer, ts varchar, userAgent varchar, userId integer)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs integer, artist_id varchar, artist_latitude numeric, artist_longitude numeric, artist_location varchar, artist_name varchar,  song_id varchar, title varchar , duration numeric , year integer )
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint identity(0,1) PRIMARY KEY, start_time timestamp  not null , user_id integer not null, level varchar not null, song_id varchar , artist_id varchar , session_id integer not null, location varchar, user_agent varchar )
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS  users (user_id integer PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs ( song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year integer, duration numeric )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar not null, location varchar, latitude numeric, longitude numeric )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour integer not null, day integer not null, week integer not null, month integer not null, year integer not null, weekday integer not null)
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} 
iam_role {}
json {} region 'us-west-2'; 
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = [(""" copy staging_songs from {} 
                        iam_role {} 
                        json 'auto' region 'us-west-2' ; """).format(SONG_DATA, ARN)]


# FINAL TABLES   


user_table_insert = (""" INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE userId IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = (""" INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs ; 
""")

songplay_table_insert = (""" INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent ) SELECT * FROM (WITH song_artist AS
(SELECT DISTINCT songs.song_id, artists.artist_id, artists.name, songs.title, songs.duration FROM songs  JOIN artists ON songs.artist_id = artists.artist_id) 
SELECT  (timestamp 'epoch' + (ts/1000) * interval '1 second') as start_time, staging_events.userId, staging_events.level, song_artist.song_id, song_artist.artist_id ,staging_events.sessionId, staging_events.location, staging_events.userAgent FROM staging_events LEFT OUTER JOIN
song_artist ON   staging_events.song = song_artist.title AND staging_events.artist = song_artist.name AND staging_events.length = song_artist.duration  WHERE staging_events.page='NextSong');
 
""")

time_table_insert = (""" INSERT INTO time
SELECT DISTINCT (timestamp 'epoch' + (ts/1000) * interval '1 second') as ts1, EXTRACT(HOUR from ts1 ) as hour, EXTRACT(DAY from ts1 ) as day, EXTRACT(WEEK from ts1) as week, EXTRACT(MONTH from ts1) as month, EXTRACT(YEAR from ts1) as year, EXTRACT(WEEKDAY from ts1) as weekday FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [ user_table_insert,song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
copy_table_queries = [staging_events_copy, staging_songs_copy]




  
