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
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        crsr = connection.cursor()

        # Create the 'records' table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            date TIMESTAMP NOT NULL
        );
        """
        crsr.execute(create_table_query)
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
        crsr.executemany(insert_query, dummy_records)

        # Commit changes
        connection.commit()
        logger.info("%s dummy records inserted into 'records' table.", num_records)

        # Close connection
        crsr.close()
        connection.close()
        logger.info("Database initialization completed successfully.")

    except Exception as e:
        logger.error("Error occurred during database initialization: %s", e)

def backup_and_clean():
    """
    Gets backup of the table records and deletes every record which's backup is taken.
    """
    try:
        logger.info("Starting backup and cleanup process...")

        # Connect to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        crsr = connection.cursor()

        # Fetch the oldest records (up to 10% of total records or at least 1)
        crsr.execute("SELECT COUNT(*) FROM records")
        total_records = crsr.fetchone()[0]
        records_to_backup = max(1, int(total_records * 0.1))  # At least 1 record, or 10% of total

        crsr.execute("SELECT * FROM records ORDER BY date ASC LIMIT %s", (records_to_backup,))
        records = crsr.fetchall()

        if not records:
            logger.info("No records found to back up.")
        else:
            # Save the backup to a file
            backup_filename = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            backup_filepath = os.path.join(BACKUP_DIR, backup_filename)
            with open(backup_filepath, "w", encoding='utf-8') as backup_file:
                for record in records:
                    backup_file.write(f"{record}\n")
            logger.info("Backup saved to %s", backup_filepath)

            # Delete backed-up records
            crsr.execute("DELETE FROM records WHERE id IN (SELECT id FROM records ORDER BY date ASC LIMIT %s)", (records_to_backup,))
            connection.commit()
            logger.info("%d records backed up and deleted from the database.", crsr.rowcount)

        # Close the connection
        crsr.close()
        connection.close()

    except Exception as e:
        logger.error("Error occurred during backup and cleanup: %s", e)
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
                logger.info("Records remaining in the table: %d", row_count)

                logger.info(
                    "Sleeping for %d seconds before the next backup and clean...", INTERVAL)
                time.sleep(INTERVAL)

        except Exception as e:
            logger.error("An error occurred: %s. Restarting the script...", e)
            time.sleep(5)  # Wait 5 seconds before retrying
