import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import os
from typing import Dict, List, Set, Tuple
import pandas as pd
from urllib.parse import urljoin
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper.log'), logging.StreamHandler()]
)

class RosasidanScraper:
    def __init__(self):
        self.base_url = 'https://rosasidan.ws'
        # Include both main sections and their paginated URLs
        self.ads_urls = [f'{self.base_url}/ads/3', f'{self.base_url}/ads/1']
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.profile_csv = 'Profile Detail.csv'
        self.profile_links_file = 'Profile Links.xlsx'
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=5,  # number of retries
            backoff_factor=0.5,  # wait 0.5, 1, 2, 4... seconds between retries
            status_forcelist=[500, 502, 503, 504, 429]  # HTTP status codes to retry on
        )
        
        # Create session with retry strategy
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update(self.headers)
        
    def make_request(self, url: str, timeout: int = 30) -> requests.Response:
        """Make HTTP request with retry and timeout handling"""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logging.error(f"Timeout error accessing {url}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Error accessing {url}: {str(e)}")
            raise
        
    def get_profile_links(self) -> Tuple[Set[str], Dict[str, Tuple[str, int]]]:
        """Extract all profile links from the ads pages including pagination"""
        profile_links = set()
        base_urls = {}  # Maps profile URL to (base_url, page_number)
        for base_url in self.ads_urls:
            page = 1
            consecutive_empty_pages = 0
            while True:
                try:
                    # Construct URL with pagination
                    url = f"{base_url}/{page}" if page > 1 else base_url
                    logging.info(f"Fetching profile links from {url}")
                    
                    response = self.make_request(url)
                    if response.status_code != 200:
                        logging.error(f"Received status code {response.status_code} from {url}")
                        break
                        
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Check for 'No ads were found' message
                    no_ads_div = soup.find('div', id='info_message', class_='alert alert-info')
                    if no_ads_div and 'No ads were found' in no_ads_div.text:
                        logging.info(f"'No ads were found' message detected on {url}, moving to next base URL")
                        break

                    # Find all profile links and titles from h3 tags
                    found_links = False
                    for h3_tag in soup.find_all('h3'):
                        link = h3_tag.find('a', href=True)
                        if link and '/ads/details/' in link['href']:
                            full_url = urljoin(self.base_url, link['href'])
                            profile_links.add(full_url)
                            base_urls[full_url] = (base_url, page, link.get_text(strip=True))
                            found_links = True
                    
                    # If no profile links found on this page
                    if not found_links:
                        consecutive_empty_pages += 1
                        logging.info(f"No profile links found on {url}, consecutive empty pages: {consecutive_empty_pages}")
                        if consecutive_empty_pages >= 3:  # Stop after 3 consecutive empty pages
                            logging.info(f"Reached 3 consecutive empty pages, moving to next base URL")
                            break
                    else:
                        consecutive_empty_pages = 0
                        # Save profile links immediately after each successful page scrape
                        self.save_profile_links(profile_links, base_urls)
                    
                    time.sleep(2)  # Increased delay between requests
                    logging.info(f"Found {len(profile_links)} profile links so far")
                    page += 1
                        
                except requests.exceptions.RequestException as e:
                    logging.error(f"Network error accessing {url}: {str(e)}")
                    time.sleep(5)  # Wait longer on network errors
                    continue
                except Exception as e:
                    logging.error(f"Error getting profile links from {url}: {str(e)}")
                    break
        
        return profile_links, base_urls
    
    def get_profile_details(self, profile_url: str) -> Dict:
        """Scrape details from a profile page"""
        try:
            logging.info(f"Fetching details for profile: {profile_url}")
            response = self.make_request(profile_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the main content panel
            content_panel = soup.find('div', class_='webpanelcontent3')
            if not content_panel:
                raise ValueError("Could not find main content panel")
            
            # Load profile links data to get base_url and title
            profile_links_data = {}
            if os.path.exists(self.profile_links_file):
                try:
                    links_df = pd.read_excel(self.profile_links_file)
                    profile_links_data = links_df.set_index('profile_url').to_dict('index')
                except Exception as e:
                    logging.warning(f"Could not read profile links data: {str(e)}")
            
            # Get base_url and title from profile links data
            link_info = profile_links_data.get(profile_url, {})
                
            # Initialize details dictionary
            details = {
                'base_url': link_info.get('base_url', ''),
                'profile_url': profile_url,
                'title': link_info.get('title', ''),
                'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'images': []
            }
            
            # Extract main details from ad_detail_column
            detail_columns = content_panel.find_all('div', class_='ad_detail_column')
            for column in detail_columns:
                # Extract text content
                if column.get_text(strip=True):
                    details['details'] = column.get_text(strip=True)
                    break
            
            # Extract login country
            country_img = soup.find('img', id='myImage')
            if country_img and 'flags' in country_img.get('src', ''):
                details['login_country'] = country_img.get('src', '').split('/')[-1].split('.')[0].upper()
            
            # Extract price
            price_div = soup.find('div', class_='ad_detail_column', string=lambda text: text and any(char.isdigit() for char in text))
            if price_div:
                price_text = price_div.get_text(strip=True)
                # Extract first number found in the text
                import re
                price_match = re.search(r'\d+', price_text)
                if price_match:
                    details['price'] = int(price_match.group())
            
            # Extract phone number
            phone_div = soup.find('a', class_='btn btn-primary phone_value')
            if phone_div:
                details['phone'] = phone_div.get_text(strip=True).replace('\u202d', '').replace('\u202c', '')
            
            # Extract posted by
            posted_by = soup.find('div', class_='ad_detail_column').find('a')
            if posted_by:
                details['posted_by'] = posted_by.get_text(strip=True)
            
            # Extract posted date
            posted_date = soup.find('div', class_='ad_detail_column', string=lambda text: text and 'ago' in text)
            if posted_date:
                details['posted_date'] = posted_date.get_text(strip=True)
            
            # Extract images
            image_divs = soup.find_all('div', class_='ad-thumbnail-image')
            for div in image_divs:
                img = div.find('img')
                if img and img.get('src'):
                    details['images'].append(img['src'])
            
            logging.info(f"Successfully scraped details for {profile_url}")
            return details
        except Exception as e:
            logging.error(f"Error getting profile details for {profile_url}: {str(e)}")
            return {}
    
    def save_profile_links(self, links: Set[str], base_urls: Dict[str, Tuple[str, int]]):
        """Save profile links to Excel file with base URL and page information, avoiding duplicates"""
        try:
            # Load existing links if file exists
            existing_links = set()
            if os.path.exists(self.profile_links_file):
                try:
                    existing_df = pd.read_excel(self.profile_links_file)
                    existing_links = set(existing_df['profile_url'].tolist())
                    logging.info(f"Loaded {len(existing_links)} existing profile links")
                except Exception as e:
                    logging.warning(f"Could not read existing profile links: {str(e)}")

            # Filter out duplicate links
            new_links = links - existing_links
            if not new_links:
                logging.info("No new profile links to save")
                return

            # Prepare new data
            data = []
            for link in new_links:
                base_info = base_urls.get(link, ('Unknown', 0, ''))
                data.append({
                    'base_url': base_info[0],
                    'profile_url': link,
                    'page_number': base_info[1],
                    'title': base_info[2],
                    'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            # Create new DataFrame with only new links
            new_df = pd.DataFrame(data)

            # If file exists, append new data; otherwise create new file
            if os.path.exists(self.profile_links_file):
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                final_df = new_df

            final_df.to_excel(self.profile_links_file, index=False)
            logging.info(f"Saved {len(new_links)} new profile links to {self.profile_links_file}")
        except Exception as e:
            logging.error(f"Error saving profile links to Excel: {str(e)}")
            raise
    
    def load_existing_profiles(self) -> Set[str]:
        """Load existing profile URLs from CSV file"""
        try:
            if not os.path.exists(self.profile_csv):
                logging.info(f"No existing profiles file found at {self.profile_csv}")
                return set()
            
            # Try different encodings
            encodings = ['utf-8', 'latin1', 'cp1252']
            for encoding in encodings:
                try:
                    df = pd.read_csv(self.profile_csv, encoding=encoding)
                    profile_urls = set(df['profile_url'].tolist())
                    logging.info(f"Loaded {len(profile_urls)} existing profiles using {encoding} encoding")
                    return profile_urls
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logging.error(f"Error loading existing profiles with {encoding} encoding: {str(e)}")
                    continue
            
            logging.error("Failed to read CSV file with any supported encoding")
            return set()
        except Exception as e:
            logging.error(f"Error loading existing profiles: {str(e)}")
            return set()
    
    def save_profile_details(self, profiles: List[Dict]):
        """Save only new profile details in CSV file without updating existing ones"""
        try:
            df = pd.DataFrame(profiles)
            
            # Convert images list to string
            df['images'] = df['images'].apply(lambda x: '|'.join(x) if isinstance(x, list) else '')
            
            if os.path.exists(self.profile_csv):
                # Try different encodings for reading existing file
                encodings = ['utf-8', 'latin1', 'cp1252']
                existing_df = None
                for encoding in encodings:
                    try:
                        existing_df = pd.read_csv(self.profile_csv, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        logging.error(f"Error reading existing CSV with {encoding} encoding: {str(e)}")
                        continue
                
                if existing_df is not None:
                    # Only append new profiles without updating existing ones
                    existing_urls = set(existing_df['profile_url'])
                    df = pd.concat([existing_df, df[~df['profile_url'].isin(existing_urls)]], ignore_index=True)
                
            # Save with UTF-8 encoding
            df.to_csv(self.profile_csv, index=False, encoding='utf-8')
            logging.info(f"Saved {len(profiles)} new profiles to {self.profile_csv}")
        except Exception as e:
            logging.error(f"Error saving profile details: {str(e)}")
            raise
    
    def save_new_profiles(self, profiles: List[Dict]):
        """Save new profiles to a date-specific CSV file"""
        if not profiles:
            logging.info("No new profiles to save")
            return
            
        try:
            # Create filename with current date
            current_date = datetime.now().strftime('%Y_%m_%d')
            new_profiles_file = f'new_profiles_{current_date}.csv'
            
            # Create DataFrame and save to CSV
            df = pd.DataFrame(profiles)
            df['images'] = df['images'].apply(lambda x: '|'.join(x) if isinstance(x, list) else '')
            df.to_csv(new_profiles_file, index=False)
            logging.info(f'Saved {len(profiles)} new profiles to {new_profiles_file}')
        except Exception as e:
            logging.error(f"Error saving new profiles: {str(e)}")
            raise
    
    def run(self):
        """Main scraping process"""
        logging.info('Starting scraping process...')
        
        try:
            # Get all current profile links with their base URLs and page numbers
            current_links, base_urls = self.get_profile_links()
            if not current_links:
                logging.warning('No profile links found')
                return
            
            # Save all current links with their source information
            self.save_profile_links(current_links, base_urls)
            
            # Load existing profiles
            existing_profiles = self.load_existing_profiles()
            
            # Find new profiles
            new_profiles = current_links - existing_profiles
            if not new_profiles:
                logging.info('No new profiles found')
                return
            
            logging.info(f'Found {len(new_profiles)} new profiles')
            
            # Scrape and save details for new profiles immediately
            processed_count = 0
            for url in new_profiles:
                details = self.get_profile_details(url)
                if details:
                    # Save each profile immediately after scraping
                    self.save_profile_details([details])  # Save to main CSV
                    self.save_new_profiles([details])     # Save to daily CSV
                    processed_count += 1
                    logging.info(f'Saved profile {processed_count}/{len(new_profiles)} to database')
                time.sleep(1)  # Be nice to the server
            
            logging.info(f'Completed saving {processed_count} new profiles to main database')
        except Exception as e:
            logging.error(f"Error in main scraping process: {str(e)}")
            raise

if __name__ == '__main__':
    try:
        scraper = RosasidanScraper()
        scraper.run()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise