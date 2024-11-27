import mysql.connector
import random
from flask import Flask, jsonify
from datetime import datetime
import os
import urllib.parse

# MySQL connection setup using remote database URL from an environment variable
def create_connection():
    db_url = os.getenv("DATABASE_URL")  # Get the remote DB URL from the environment variable
    
    if db_url:
        # Parse the database URL
        url = urllib.parse.urlparse(db_url)
        
        # Return the MySQL connection using the parsed components
        return mysql.connector.connect(
            host=url.hostname,           # Hostname from the URL
            user=url.username,           # Username from the URL
            password=url.password,       # Password from the URL
            database=url.path[1:]        # Database name from the URL (skip the leading '/')
        )
    else:
        raise Exception("DATABASE_URL environment variable not set")

# Create Flask app
app = Flask(__name__)

# Variable to store the current character
current_character = None

# Set the target time for the refresh (e.g., 12:00 PM)
TARGET_HOUR = 20
TARGET_MINUTE = 0
TARGET_SECOND = 0

# Function to get a random character from the database
def get_random_character():
    # Connect to the remote database
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Query the database for all characters
    cursor.execute("SELECT * FROM bleach_characters")  # Replace 'bleach_characters' with your table name
    characters = cursor.fetchall()
    
    # Close the connection
    cursor.close()
    connection.close()
    
    # Pick a random character from the list
    random_character = random.choice(characters) if characters else None
    
    return random_character

# Function to check if it's time to refresh (check if current time matches the target)
def is_refresh_time():
    current_time = datetime.now()  # Get the current time
    
    # Check if the current time matches the target time (e.g., 12:00 PM)
    if (current_time.hour == TARGET_HOUR and 
        current_time.minute == TARGET_MINUTE and 
        current_time.second == TARGET_SECOND):
        return True
    return False

# Initialize the current character
current_character = get_random_character()

# Endpoint to get a random character
@app.route('/', methods=['GET'])
def random_character():
    global current_character
    
    # If it's time to refresh (at the specific time), get a new character
    if is_refresh_time():
        current_character = get_random_character()
    
    # Return the current character as a JSON response
    return jsonify(current_character)

if __name__ == '__main__':
    app.run(debug=True)
