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
CREATE TABLE staging_events
(
artist            varchar,
auth              varchar,
first_name        varchar,
gender            varchar,
item_in_session   integer,
last_name         varchar,
length            float,
level             varchar,
location          varchar, 
method            varchar,
page              varchar,
registration      float,
session_id        integer,
song              varchar,
status            integer,
ts                bigint,
user_agent        varchar,
user_id           integer
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            varchar,
num_songs          integer,
title              varchar,
artist_name        text,
artist_latitude    float,
year               integer,
duration           float,
artist_id          varchar,
artist_longitude   float,
artist_location    varchar
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id        integer       not null primary key,
start_time         timestamp     sortkey,
user_id            integer,
level              varchar(30),
song_id            varchar(50)   distkey,
artist_id          varchar(50),
session_id         integer,
location           varchar(256),
user_agent         varchar(50)        
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id       integer           not null sortkey primary key,
first_name    varchar(50),
last_name     varchar(50),
gender        varchar(20),
level         varchar(20)     
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id      varchar(50)    not null sortkey primary key,
title        varchar(256),
artist_id    varchar(50),
year         integer,
duration     decimal(10, 5)
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id   varchar(50)     not null sortkey primary key,
name        varchar(256),
location    varchar(256),
lattitude   decimal(20, 4),
longitude   decimal(20, 4)
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time      timestamp  not null sortkey primary key,
hour            integer,
day             integer,
week            integer,
month           integer,
year            integer,
weekday         integer
)
diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          region 'us-west-2'
                          compupdate off 
                          json {};
                       """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          region 'us-west-2'
                          compupdate off 
                          json 'auto';
                      """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                         intsert into songplays(
                             start_time,
                             user_id,
                             level,
                             song_id,
                             artist_id,
                             session_id,
                             location,
                             user_agent)
                         select distinct(
                             to_timestamp(events.ts::timestamp/1000) as start_time,
                             events.userId as user_id,
                             events.level as level,
                             songs.song_id as song_id,
                             songs.artist_id as artist_id,
                             events.sessionId as session_id,
                             events.location as location,
                             events.userAgent as user_agent
                         from 
                             staging_events as events
                         left join 
                             staging_songs as songs
                         on  events.song = songs.title
                         and events.artist = songs.artist_name;
                         """)

user_table_insert = ("""
                     insert into users(
                         user_id,
                         first_name,
                         last_name,
                         gender,
                         level)
                     select distinct
                         userId as user_id,
                         firstName as first_name,
                         lastName as last_name,
                         gender as gender,
                         level as level
                     FROM staging_events;
                     """)

song_table_insert = ("""
                     insert into songs(
                         song_id,
                         title,
                         artist_id,
                         year,
                         duration)
                     select distinct 
                         song_id as song_id,
                         title as title,
                         artist_id as artist_id,
                         year as year,
                         duration as duration
                     FROM staging_songs;
                     """)

artist_table_insert = ("""
                       insert into artists(
                           artist_id,
                           name,
                           location,
                           latitude,
                           longitude)
                       select distinct 
                           artist_id as artist_id,
                           artist_name as name,
                           location as location,
                           latitude as latitude,
                           longitude as longitude
                       FROM staging_songs;
                       """)

time_table_insert = ("""
                     insert into time(
                         start_time,
                         hour,
                         day,
                         week,
                         month,
                         year,
                         weekday)
                     select distinct(
                        to_timestamp(events.ts::timestamp/1000) as start_time,
                        extract(hour from start_time) as hour,
                        extract(day from start_time) as day,
                        extract(week from start_time) as week,
                        extract(month from start_time) as month,
                        extract(year from start_time) as year,
                        extract(weekday from start_time) as weekday
                     from 
                         staging_events;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
