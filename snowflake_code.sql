-- Create the database for the ETL process
CREATE DATABASE spotify_etl;

-- Use the newly created database
USE DATABASE spotify_etl;

-- Create a schema to organize data within the database
CREATE SCHEMA spotify_schema;

-- Create a table to store album data
CREATE OR REPLACE TABLE spotify_schema.album_data (
    album_id STRING,               -- Unique identifier for each album
    name STRING,                   -- Name of the album
    release_date DATE,             -- Release date of the album
    total_tracks INTEGER,          -- Total number of tracks in the album
    url STRING                     -- URL for the album
);

-- Create a table to store artist data
CREATE OR REPLACE TABLE spotify_schema.artist_data (
    artist_id STRING,              -- Unique identifier for each artist
    artist_name STRING,            -- Name of the artist
    external_url STRING            -- URL link to the artist’s external site
);

-- Create a table to store song data
CREATE OR REPLACE TABLE spotify_schema.songs_data (
    song_id STRING,                -- Unique identifier for each song
    song_name STRING,              -- Name of the song
    duration_ms INTEGER,           -- Duration of the song in milliseconds
    url STRING,                    -- URL for the song
    popularity INTEGER,            -- Popularity score of the song
    song_added TIMESTAMP,          -- Timestamp of when the song was added
    album_id STRING,               -- Foreign key linking to the album
    artist_id STRING               -- Foreign key linking to the artist
);

-- Define a storage integration for S3 to connect Snowflake with S3 data source
CREATE OR REPLACE STORAGE INTEGRATION spotify_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = ()  -- Role ARN for AWS S3 access
STORAGE_ALLOWED_LOCATIONS = ('');  -- S3 bucket for data storage

-- Show the details of the storage integration to verify its configuration
DESC STORAGE INTEGRATION spotify_s3_integration;

-- Create stages to reference the different S3 locations for songs, albums, and artists data
CREATE OR REPLACE STAGE songs_stage
URL = ''  -- Path to songs data in S3
STORAGE_INTEGRATION = spotify_s3_integration;  -- Use the S3 storage integration

CREATE OR REPLACE STAGE albums_stage
URL = ''  -- Path to albums data in S3
STORAGE_INTEGRATION = spotify_s3_integration;  -- Use the S3 storage integration

CREATE OR REPLACE STAGE artists_stage
URL = ''  -- Path to artists data in S3
STORAGE_INTEGRATION = spotify_s3_integration;  -- Use the S3 storage integration

-- Create pipes for automatic ingestion of data from S3 to Snowflake tables
CREATE OR REPLACE PIPE songs_pipe
AUTO_INGEST = TRUE  -- Automatically ingest new files from S3 to Snowflake
AS
COPY INTO spotify_schema.songs_data  -- Destination table in Snowflake
FROM @songs_stage  -- Source stage pointing to S3 location
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)  -- File format for CSV files
ON_ERROR = 'CONTINUE';  -- Skip records with errors and continue processing

CREATE OR REPLACE PIPE album_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_schema.album_data
FROM @albums_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Show the list of tables in the schema to ensure creation
SHOW TABLES IN spotify_schema;

-- Create the pipe for artist data ingestion
CREATE OR REPLACE PIPE artists_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_schema.artist_data
FROM @artists_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Manually load data into songs_data from the S3 stage (for testing/initial load)
COPY INTO spotify_schema.songs_data
FROM @songs_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Manually load data into album_data from the S3 stage (for testing/initial load)
COPY INTO spotify_schema.album_data
FROM @albums_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Manually load data into artist_data from the S3 stage (for testing/initial load)
COPY INTO spotify_schema.artist_data
FROM @artists_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Run queries to check the data in the tables
SELECT * FROM spotify_schema.songs_data;  -- Check song data
SELECT * FROM spotify_schema.album_data;  -- Check album data
SELECT * FROM spotify_schema.artist_data;  -- Check artist data

-- Display the descriptions of pipes to verify their configuration
desc pipe album_pipe;
desc pipe artists_pipe;
desc pipe songs_pipe;
