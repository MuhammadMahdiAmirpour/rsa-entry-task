"""
RSA Entry task:
1- initializing postgres database in docker and creating required database and tables and dummy data
2- Getting backup of the data which will be deleted immediately after.
"""

import os
import datetime
import time
import logging
import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()          # Log to the console
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
BACKUP_DIR = os.getenv("BACKUP_DIR", "/backups")
DAYS_TO_BACKUP = int(os.getenv("DAYS_TO_BACKUP", "7"))
# Default interval is 1 day (86400 seconds)
INTERVAL = int(os.getenv("INTERVAL", "86400"))


def initialize_database():
    """
    Initializes the database by creating required tables and inserting dummy data into them.
    """
    try:
        logger.info("Initializing database connection...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Create the 'records' table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            date TIMESTAMP NOT NULL
        );
        """
        cursor.execute(create_table_query)
        logger.info("Table 'records' created or already exists.")

        # Generate dummy data using Faker
        fake = Faker()
        num_records = 50  # Number of dummy records to create
        dummy_records = []
        for _ in range(num_records):
            name = fake.name()
            # Generate a random date within the last 30 days
            date = fake.date_time_between(start_date='-60d', end_date='now')
            dummy_records.append((name, date))

        # Insert dummy data
        insert_query = "INSERT INTO records (name, date) VALUES (%s, %s);"
        cursor.executemany(insert_query, dummy_records)

        # Commit changes
        conn.commit()
        logger.info(f"{num_records} dummy records inserted into 'records' table.")

        # Close connection
        cursor.close()
        conn.close()
        logger.info("Database initialization completed successfully.")

    except Exception as e:
        logger.error(f"Error occurred during database initialization: {e}")

def backup_and_clean():
    """
    Gets backup of the table records and deletes every record which's backup is taken.
    """
    try:
        logger.info("Starting backup and cleanup process...")

        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Fetch the oldest records (up to 10% of total records or at least 1)
        cursor.execute("SELECT COUNT(*) FROM records")
        total_records = cursor.fetchone()[0]
        records_to_backup = max(1, int(total_records * 0.1))  # At least 1 record, or 10% of total

        cursor.execute("SELECT * FROM records ORDER BY date ASC LIMIT %s", (records_to_backup,))
        records = cursor.fetchall()

        if not records:
            logger.info("No records found to back up.")
        else:
            # Save the backup to a file
            backup_filename = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            backup_filepath = os.path.join(BACKUP_DIR, backup_filename)
            with open(backup_filepath, "w") as backup_file:
                for record in records:
                    backup_file.write(f"{record}\n")
            logger.info(f"Backup saved to {backup_filepath}")

            # Delete backed-up records
            cursor.execute("DELETE FROM records WHERE id IN (SELECT id FROM records ORDER BY date ASC LIMIT %s)", (records_to_backup,))
            conn.commit()
            logger.info(f"{cursor.rowcount} records backed up and deleted from the database.")

        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error occurred during backup and cleanup: {e}")
        raise

if __name__ == "__main__":
    while True:
        try:
            # Initialize the database with dummy data
            logger.info("Initializing database with dummy data...")
            initialize_database()

            # Backup and clean until the database is empty
            while True:
                backup_and_clean()

                # Check if the table is empty after cleanup
                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM records;")
                row_count = cursor.fetchone()[0]
                cursor.close()
                conn.close()

                if row_count == 0:
                    logger.info(
                        "Database is now empty. Restarting the process.")
                    break
                else:
                    logger.info(f"Records remaining in the table: {row_count}")

                logger.info(
                    f"Sleeping for {INTERVAL} seconds before the next backup and clean...")
                time.sleep(INTERVAL)

        except Exception as e:
            logger.error(f"An error occurred: {e}. Restarting the script...")
            time.sleep(5)  # Wait 5 seconds before retrying
