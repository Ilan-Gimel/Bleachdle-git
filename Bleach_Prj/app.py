import os
import mysql.connector
from flask import Flask, jsonify
import urllib.parse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
import pytz  

# Initialize Flask app
app = Flask(__name__)

# Function to create a connection to the Cloud SQL database
def create_connection():
    # Get the database URL from environment variables
    db_url = os.getenv("DATABASE_URL")
    
    if db_url:
        # Parse the database URL to get credentials and database name
        url = urllib.parse.urlparse(db_url)
        
        # Connect to MySQL using the public IP
        connection = mysql.connector.connect(
            user=url.username,
            password=url.password,
            host=url.hostname,  # Use the public IP directly from the parsed URL
            database=url.path[1:]  # Skip the leading '/' in the path
        )
        
        return connection
    else:
        raise Exception("DATABASE_URL environment variable not set")

# Initialize a global variable for current character
current_character = None

# Function to get a random character from the database
def get_random_character():
    # Connect to the Cloud SQL database
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Fetch a random character directly from the database
    cursor.execute("SELECT * FROM bleach_characters ORDER BY RAND() LIMIT 1")
    random_character = cursor.fetchone()  # Fetch one row (random character)
    
    cursor.close()
    connection.close()
    
    return random_character  # Will return None if no character is found

# Function to update the character
def update_character():
    global current_character
    current_character = get_random_character()  # Get new character from DB

# Function to get the next midnight (00:00)
def next_midnight():
    now = datetime.now(pytz.UTC)  # Ensure we are using UTC timezone
    # Set the next midnight
    next_midnight_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return next_midnight_time

# Function to refresh the character every day at midnight (task will run in the background)
def schedule_refresh():
    scheduler = BackgroundScheduler(timezone=pytz.UTC)  # Ensure timezone is set to pytz.UTC
    # Get the first refresh time at the next midnight
    first_run_time = next_midnight()
    # Schedule the refresh at midnight every day
    scheduler.add_job(update_character, trigger=IntervalTrigger(days=1, start_date=first_run_time))
    scheduler.start()

# Set the initial character when the app starts
update_character()

# Endpoint to get a random character
@app.route('/', methods=['GET'])
def random_character():
    global current_character

    # Ensure that current_character is not None before returning it
    if current_character:
        return jsonify(current_character)
    else:
        return jsonify({"error": "No character available"}), 500

# Start the background task for automatic refresh at midnight every day
schedule_refresh()

# Ensuring the app runs on Google App Engine using the default host and port
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8080)


