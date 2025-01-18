# Import necessary libraries
import json  
import boto3  
from datetime import datetime  
from io import StringIO  
import pandas as pd  

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element) 
    return album_list 

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":  
                for artist in value['artists']:
                    
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict) 
    return artist_list  

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id'] 
        
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                        'popularity': song_popularity, 'song_added': song_added, 'album_id': album_id,
                        'artist_id': artist_id}
        song_list.append(song_element) 
    return song_list  

# Lambda handler function to orchestrate ETL process
def lambda_handler(event, context):
    # S3 client for uploading and downloading files from S3
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-bhupesh-snowflake"  # Target S3 bucket
    Key = "raw_data/to_processed/"  # Folder path where raw files are located

    spotify_data = []
    spotify_keys = []

    # Iterate through files in the 'to_processed' folder in S3
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":  # Only process JSON files
            # Get the file contents from S3 and load into a JSON object
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)  # Store the loaded JSON data
            spotify_keys.append(file_key)  # Store the S3 key of the file for later processing
            
    # Loop over the collected Spotify data
    for data in spotify_data:
        # Extract album, artist, and song information from the data
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)

        # Convert the extracted data to Pandas DataFrames
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)
        
        # Remove duplicate entries from album and artist data
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        # Convert string dates to datetime objects for proper handling
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        # Write transformed song data to CSV and upload to S3
        songs_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()  # Create a buffer to hold CSV data in memory
        song_df.to_csv(song_buffer, index=False)  # Convert DataFrame to CSV
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)

        # Write transformed album data to CSV and upload to S3
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        # Write transformed artist data to CSV and upload to S3
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    # Resource to manage file operations in S3
    s3_resource = boto3.resource('s3')

    # Move processed files from the 'to_processed' folder to 'processed'
    for key in spotify_keys:
        copy_source = {'Bucket': Bucket, 'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        # Delete the original file from 'to_processed' folder after moving
        s3_resource.Object(Bucket, key).delete()
