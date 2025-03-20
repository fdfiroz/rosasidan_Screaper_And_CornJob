#!/bin/bash

# Get the absolute path of the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create a temporary crontab file
TMP_CRON=$(mktemp)
crontab -l > "$TMP_CRON" 2>/dev/null

# Add the scraper job to run at 10 AM daily
echo "0 10 * * * cd $SCRIPT_DIR && python scraper.py >> scraper.log 2>&1" >> "$TMP_CRON"

# Install the new crontab
crontab "$TMP_CRON"

# Clean up
rm "$TMP_CRON"

echo "Cron job has been set up successfully."
echo "The scraper will run daily at 10:00 AM."