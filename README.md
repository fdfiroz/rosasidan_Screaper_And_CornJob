# Rosasidan Web Scraper

A Python-based web scraper for collecting and monitoring profile information from Rosasidan.ws. The scraper automatically detects and saves new profiles, storing data in both a main database and daily snapshot files.

## Features

- Automatic profile discovery and tracking
- Daily snapshots of new profiles
- Image URL collection
- Duplicate prevention
- Rate-limited scraping to be server-friendly
- Both Excel and CSV output formats

## Prerequisites

- Python 3.6 or higher
- Docker (optional, for containerized deployment)

## Installation

### Manual Setup

1. Clone the repository or download the source code

2. Create and activate a virtual environment:
   ```bash
   # Windows
   python -m venv .venv
   .venv\Scripts\activate

   # Linux/macOS
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Automated Scheduling Setup

#### Windows
1. Open PowerShell as Administrator
2. Navigate to the project directory
3. Run the scheduling script:
   ```powershell
   .\schedule_task.ps1
   ```
This will create a Windows Task Scheduler job that runs the scraper daily at 10:00 AM.

#### Linux/macOS
1. Open Terminal
2. Navigate to the project directory
3. Make the setup script executable:
   ```bash
   chmod +x setup_cron.sh
   ```
4. Run the setup script:
   ```bash
   ./setup_cron.sh
   ```
This will create a cron job that runs the scraper daily at 10:00 AM.

### Docker Installation

1. Build the Docker image:
   ```bash
   docker build -t rosasidan-scraper .
   ```

2. Run the container:
   ```bash
   docker run -v $(pwd):/app rosasidan-scraper
   ```

The Docker container includes a cron job scheduled to run the scraper daily at 10:00 AM. The scraper output and any errors will be logged to `/app/scraper.log` inside the container (mapped to your local directory).

## Usage

### Running the Scraper

1. Standard execution:
   ```bash
   python scraper.py
   ```

2. Docker execution:
   ```bash
   docker run -v $(pwd):/app rosasidan-scraper
   ```

### Output Files

The scraper generates several output files:

1. `Profile Detail.csv` - Main database containing all scraped profiles
2. `Profile Links.xlsx` - Excel file with all discovered profile URLs
3. `new_profiles_YYYY_MM_DD.csv` - Daily snapshot files of newly discovered profiles

### Output Format

The CSV files contain the following information for each profile:
- Profile URL
- Title
- Username
- Image URLs (pipe-separated)
- Image count
- Scrape date

## Configuration

The scraper's behavior can be modified by editing the following parameters in `scraper.py`:

```python
self.base_url = 'https://rosasidan.ws'  # Base website URL
self.ads_url = f'{self.base_url}/ads/3'  # Specific page to scrape
time.sleep(1)  # Delay between requests (seconds)
```

## Important Notes

1. **Rate Limiting**: The scraper includes a 1-second delay between requests to avoid overwhelming the server. Adjust this value responsibly.

2. **Data Storage**: All data is stored locally in CSV and Excel files. Ensure sufficient disk space is available.

3. **Error Handling**: The scraper includes basic error handling and will continue running even if individual profile scrapes fail.

4. **Responsible Scraping**: Please be mindful of the website's terms of service and implement appropriate delays between requests.

## Troubleshooting

1. If the scraper fails to connect, check your internet connection and verify the website is accessible.

2. For "Permission denied" errors when saving files, ensure you have write permissions in the directory.

3. If no new profiles are found, this might be normal - it means no new profiles have been added since the last scan.

## License

This project is for educational purposes only. Please ensure you have permission to scrape any website and comply with their terms of service.