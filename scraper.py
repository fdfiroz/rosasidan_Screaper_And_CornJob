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
            total=3,  # number of retries
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

                    # Find all profile links (based on the HTML structure)
                    found_links = False
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/ads/details/' in href:
                            full_url = urljoin(self.base_url, href)
                            profile_links.add(full_url)
                            base_urls[full_url] = (base_url, page)
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
                
            # Extract profile information
            details = {
                'profile_url': profile_url,
                'title': '',
                'username': '',
                'description': '',
                'price': '',
                'phone': '',
                'skype': '',
                'kik': '',
                'posted_by': '',
                'posted_time': '',
                'images': [],
                'image_count': 0,
                'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Extract title
            title_link = content_panel.find('a', href='#')
            if title_link:
                details['title'] = title_link.text.strip()
            
            # Extract description
            desc_div = content_panel.find('div', class_='ad_detail_column')
            if desc_div:
                details['description'] = desc_div.text.strip()
            
            # Extract price
            price_row = content_panel.find('div', class_='row', string=lambda text: text and 'Price:' in text if text else False)
            if price_row:
                price_value = price_row.find('div', class_='ad_detail_column')
                if price_value:
                    details['price'] = price_value.text.strip()
            
            # Extract phone
            phone_value = content_panel.find('a', class_='phone_value')
            if phone_value:
                details['phone'] = phone_value.text.strip()
            
            # Extract Skype
            skype_value = content_panel.find('a', class_='skype_value')
            if skype_value:
                details['skype'] = skype_value.text.strip()
            
            # Extract KiK
            kik_row = content_panel.find('div', class_='row', string=lambda text: text and 'KiK:' in text if text else False)
            if kik_row:
                kik_value = kik_row.find('div', class_='ad_detail_column')
                if kik_value:
                    details['kik'] = kik_value.text.strip()
            
            # Extract posted by
            posted_by_row = content_panel.find('div', class_='row', string=lambda text: text and 'Posted by:' in text if text else False)
            if posted_by_row:
                posted_by_link = posted_by_row.find('a')
                if posted_by_link:
                    details['posted_by'] = posted_by_link.text.strip()
                    details['username'] = posted_by_link.text.strip()
            
            # Extract posted time
            posted_time_row = content_panel.find('div', class_='row', string=lambda text: text and 'Posted:' in text if text else False)
            if posted_time_row:
                posted_time = posted_time_row.find('div', class_='ad_detail_column')
                if posted_time:
                    details['posted_time'] = posted_time.text.strip()
            
            # Extract images
            image_divs = content_panel.find_all('div', class_='ad-thumbnail-image')
            for img_div in image_divs:
                img = img_div.find('img')
                if img and 'src' in img.attrs and 'uploads' in img['src']:
                    img_url = urljoin(self.base_url, img['src'])
                    details['images'].append(img_url)
            
            details['image_count'] = len(details['images'])
            
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
                base_info = base_urls.get(link, ('Unknown', 0))
                data.append({
                    'profile_url': link,
                    'base_url': base_info[0],
                    'page_number': base_info[1],
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
        """Save or update profile details in CSV file"""
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
                    # Update existing profiles and append new ones
                    df = pd.concat([existing_df, df]).drop_duplicates(subset=['profile_url'], keep='last')
            
            # Save with UTF-8 encoding
            df.to_csv(self.profile_csv, index=False, encoding='utf-8')
            logging.info(f"Saved {len(profiles)} profiles to {self.profile_csv}")
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