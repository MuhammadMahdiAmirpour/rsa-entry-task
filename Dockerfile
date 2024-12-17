# Use Python 3.11 base image
FROM python:3.11

# Set work directory
WORKDIR /app

# Copy necessary files
COPY requirements.txt .
COPY backup_cleanup.py .
COPY .env .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the Python script
CMD ["python", "backup_cleanup.py"]
