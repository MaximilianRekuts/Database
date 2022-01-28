
## Introduction

A music streaming startup by the name Sparkify has expanded their user base and song database. They would like to move their processes and data into the cloud.
    
Hence, in this project we are tasked with setting up an ETL pipeline to extract their data from S3, stage it in Redshift, and transform the data into a set of dimensional tables for their analytics team.

---

## Dataset
    
The data is in log filed in Amazon S3 buckets at s3://udacity-dend/log_data and s3://udacity-dend/song_data. The first file contains the log data and the second file contains the song data. 

---

## Database Schema

Within the S3 buckets there are two tables with JSON files that contain the data.

###### Staging Table

    -staging_events: the actions done by a user
    -staging_songs: information around songs and artists
    
For this project the decision was made to create a star schema, hence, to denormalise the data for optimised query time.

For this project a fact table and several dimension tables were created:

##### Fact Table:

-songplay
    -columns: songplay_id, start_time, level, song_id, user_id, user_agent, location, session_id, artist_id

##### Dimension Table:

-artist
    -columns: artist_id, lattitude, longitude, name, location
    
-song
    -columns: song_id, title, duration, year, artist_id
    
-user
    -columns: level, gender, last_name, first_name, user_id, key
    
-time
    -columns: start_time, hour, day, week, month, year, weekday
    
---

## Run project

1. Create tables by running create_tables.py

2. Execute ETL process by running the etl.py script