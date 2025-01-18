# Spotify-ETL-Pipeline-Project

## Introduction

This project implements an ETL pipeline utilizing the Spotify API, AWS services, and Snowflake. The pipeline extracts data from the Spotify API, processes it using AWS Lambda, and stores it in AWS S3. The data is subsequently ingested into Snowflake for advanced analysis and insights.

## Architecture
![image](https://raw.githubusercontent.com/Bhupesh0007/spotify_etl_pipeline/refs/heads/main/spotify_pipeline_architecture_diagram.webp)

## Business Problem

A client, with a deep interest in the music industry, seeks to gather and analyze music data to identify patterns and trends for creating new music. Initially, the client will focus on the top 50 Indian songs weekly, aiming to analyze popular genres, top albums, and leading artists dominating the music charts.

## Dataset/API Used

Spotify API: The Spotify Web API is used to fetch data related to songs, albums, and artists. It provides extensive metadata and analytics for music tracks, enabling detailed data exploration and analysis.

## Services Used

1. Amazon S3: S3 provides a scalable cloud storage solution that ensures high durability and security. It allows data storage and retrieval from anywhere, integrating seamlessly with AWS services for effective data handling.

2. AWS Lambda: Lambda offers serverless computing to execute code without managing servers. It scales automatically based on the workload, running code in response to events such as data changes or HTTP requests.

3. Amazon CloudWatch: CloudWatch enables real-time monitoring of AWS resources and applications. It offers metrics and logs to track performance, set up alarms, and automate responses to ensure resources function optimally.

4. Snowpipe: Snowpipe automates continuous data loading into Snowflake from external sources like AWS S3. It listens for new files in S3 and ingests them in near real-time, simplifying the data loading process for analytics.

5. Snowflake: A cloud-based data warehouse, Snowflake supports both structured and semi-structured data. It is designed for high-performance analytics with features like auto-scaling and seamless AWS integration, enabling effective data transformation and querying for insights.

## Installed Packages (Refer to requirements.txt)

1. Spotipy: For accessing the Spotify Web API.

2. Boto3: AWS SDK for Python, facilitating interactions with AWS services.

3. Pandas: For data manipulation and analysis.

## Project Execution Flow

1. Extract data from Spotify API.

2. Weekly trigger for AWS Lambda.

3. Execute data extraction code.

4. Store raw data in S3.

5. Trigger Lambda function for data transformation.

6. Transform the data and reload it to S3.

7. Snowpipe automatically ingests data into Snowflake tables for querying and analysis.
