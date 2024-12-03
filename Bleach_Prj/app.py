import os
import mysql.connector
from flask import Flask, jsonify
import json
import urllib.parse
from datetime import datetime, timedelta
import pytz

# Initialize Flask app
app = Flask(__name__)

# Function to create a connection to the Cloud SQL database
def create_connection():
    db_url = os.getenv("DATABASE_URL")
    
    if db_url:
        url = urllib.parse.urlparse(db_url)
        connection = mysql.connector.connect(
            user=url.username,
            password=url.password,
            host=url.hostname,
            database=url.path[1:]
        )
        return connection
    else:
        raise Exception("DATABASE_URL environment variable not set")

# Function to fetch the current character and its timestamp from the database
def get_current_character():
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT character_data, last_updated FROM current_character LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    
    return result

# Function to store or update the current character in the database
def set_current_character(character_data):
    connection = create_connection()
    cursor = connection.cursor()
    
    # Check if a row exists in the current_character table
    cursor.execute("SELECT id FROM current_character LIMIT 1")
    result = cursor.fetchone()
    
    if result:
        # If a row exists, update the existing character
        cursor.execute(
            "UPDATE current_character SET character_data = %s, last_updated = NOW() WHERE id = 1",
            (json.dumps(character_data),)
        )
    else:
        # If no row exists, insert the first character
        cursor.execute(
            "INSERT INTO current_character (id, character_data, last_updated) VALUES (1, %s, NOW())",
            (json.dumps(character_data),)
        )
    
    connection.commit()
    cursor.close()
    connection.close()

# Function to get a random character from the database
def get_random_character():
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM bleach_characters ORDER BY RAND() LIMIT 1")
    random_character = cursor.fetchone()
    cursor.close()
    connection.close()
    
    return random_character

# Function to check if the character needs to be refreshed (if it's midnight UTC)
def needs_refresh(last_updated):
    now = datetime.now(pytz.UTC)  # Ensure `now` is timezone-aware
    
    # If last_updated is None, it means no character exists, so refresh immediately
    if last_updated is None:
        return True
    
    # Make sure `last_updated` is also timezone-aware (if it's not, localize it)
    if last_updated.tzinfo is None:
        last_updated = pytz.UTC.localize(last_updated)  # Localize it to UTC if it's naive
    
    # Calculate the next midnight time and make it timezone-aware (only if it's naive)
    next_midnight = last_updated.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    # Only localize `next_midnight` if it doesn't already have timezone info
    if next_midnight.tzinfo is None:
        next_midnight = pytz.UTC.localize(next_midnight)  # Localize next_midnight to UTC if it's naive
    
    # If current time is past the next midnight, refresh the character
    if now >= next_midnight:
        return True
    
    return False

# Function to refresh or initialize the character
def refresh_character():
    # Get a new random character
    new_character = get_random_character()
    set_current_character(new_character)  # Store it in the DB
    return new_character

# Initialize the character only if it does not exist
def initialize_character():
    # Check if the current character exists in the database
    current_character_data = get_current_character()
    
    if not current_character_data:
        # If no character exists, initialize it
        new_character = refresh_character()

# Initialize the character before the app handles any requests
initialize_character()

# Endpoint to get the current character
@app.route('/', methods=['GET'])
def random_character():
    # Fetch the current character and its timestamp from the database
    current_character_data = get_current_character()
    
    if current_character_data:
        # Check if the character needs to be refreshed
        last_updated = current_character_data['last_updated']
        if needs_refresh(last_updated):
            # Refresh the character if needed
            current_character_data = refresh_character()
        
        # Deserialize character_data from JSON string to Python dictionary
        character_data = json.loads(current_character_data['character_data'])
        
        # Return the current character as a proper JSON object
        return jsonify(character_data)
    else:
        return jsonify({"error": "No character available"}), 500

# Ensuring the app runs on Google App Engine using the default host and port
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8080)
