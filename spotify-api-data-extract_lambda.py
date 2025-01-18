# Importing necessary libraries
import json 
import os  
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

# Lambda handler function to execute the ETL process
def lambda_handler(event, context):
    # Retrieve Spotify API credentials from environment variables
    client_id = os.environ.get('client_id')  
    client_secret = os.environ.get('client_secret')  
    
    # Set up the authentication manager for Spotify using the client credentials
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Get the list of user playlists (replace with 'spotify' to get official playlists)
    playlists = sp.user_playlists('spotify')  
    
    # Spotify Playlist link for which data needs to be retrieved
    playlist_link = "https://open.spotify.com/playlist/2UZk7JjJnbTut1w8fqs3JL"
    # Extracting the Playlist URI from the link
    playlist_URI = playlist_link.split("/")[-1]
    
    # Fetch the tracks (songs) from the specified playlist using the Playlist URI
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    # Set up a connection to AWS S3 using boto3
    client = boto3.client("s3")
    
    # Generate a filename for the JSON data, appending the current timestamp
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    # Upload the retrieved Spotify data as a JSON file to the specified S3 bucket
    client.put_object(
        Bucket = "spotify-etl-project-bhupesh-snowflake",  # Target S3 bucket
        Key = "raw_data/to_processed/" + filename,  # Key/path inside the bucket
        Body = json.dumps(spotify_data)  # Body of the object - the playlist data in JSON format
    )
