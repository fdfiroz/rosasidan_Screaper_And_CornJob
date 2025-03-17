FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install cron and required packages
RUN apt-get update && apt-get install -y cron

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper code
COPY scraper.py .

# Create cron job
RUN echo "0 0 * * * /usr/local/bin/python /app/scraper.py >> /app/scraper.log 2>&1" > /etc/cron.d/scraper-cron
RUN chmod 0644 /etc/cron.d/scraper-cron
RUN crontab /etc/cron.d/scraper-cron

# Create log file
RUN touch /app/scraper.log

# Run cron in foreground
CMD ["cron", "-f"]