import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

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
songplay_id        integer  identity(0,1) primary key,
start_time         timestamp not null sortkey,
user_id            integer not null,
level              text,
song_id            text    distkey not null,
artist_id          text   not null,
session_id         integer,
location           text,
user_agent         text     
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id       integer   sortkey primary key,
first_name    text,
last_name     text,
gender        text,
level         text     
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id      text     sortkey primary key not null,
title        text not null,
artist_id    text not null,
year         integer,
duration     decimal(10, 5)
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id   text    not null sortkey primary key,
name        text,
location    text,
lattitude   decimal(20, 4),
longitude   decimal(20, 4)
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time      timestamp  not null sortkey primary key,
hour            integer not null,
day             integer not null,
week            integer not null,
month           integer not null,
year            integer not null,
weekday         integer not null
)
diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          region 'us-west-2'
                          json {};
                       """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          region 'us-west-2'
                          json 'auto';
                      """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                         insert into songplays(
                             start_time,
                             user_id,
                             level,
                             song_id,
                             artist_id,
                             session_id,
                             location,
                             user_agent)
                         select distinct
                             timestamp 'epoch' + events.ts/1000 * Interval '1 second',
                             events.user_id as user_id,
                             events.level as level,
                             songs.song_id as song_id,
                             songs.artist_id as artist_id,
                             events.session_id as session_id,
                             events.location as location,
                             events.user_agent as user_agent
                         from 
                             staging_events as events
                         left join 
                             staging_songs as songs
                         on  events.song = songs.title
                         and events.artist = songs.artist_name
                         where song_id is not null;
                         """)

user_table_insert = ("""
                     insert into users(
                         user_id,
                         first_name,
                         last_name,
                         gender,
                         level)
                     select distinct
                         user_id as user_id,
                         first_name as first_name,
                         last_name as last_name,
                         gender as gender,
                         level as level
                     FROM staging_events
                     WHERE user_id is not null;
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
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
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
                           artist_location as location,
                           artist_latitude as latitude,
                           artist_longitude as longitude
                       FROM staging_songs
                       WHERE artist_id is not null;
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
                     select distinct
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
