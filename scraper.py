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
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        # Set file paths for Database directory
        self.profile_csv = os.path.join('Database', 'profile_details.csv')
        self.profile_links_file = os.path.join('Database', 'Profile_Links.xlsx')
        self.image_download_pool = ThreadPoolExecutor(max_workers=5)
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=5,  # number of retries
            backoff_factor=0.5,  # wait 0.5, 1, 2, 4... seconds between retries
            status_forcelist=[500, 502, 503, 504, 429]  # HTTP status codes to retry on
        )
        
        # Create session with retry strategy and SSL configuration
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update(self.headers)
        self.session.verify = False  # Disable SSL verification
        
        # Suppress only the InsecureRequestWarning from urllib3
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
    def make_request(self, url: str, timeout: tuple = (10, 30)) -> requests.Response:
        """Make HTTP request with retry and timeout handling
        Args:
            url: The URL to request
            timeout: A tuple of (connect timeout, read timeout) in seconds
        """
        max_retries = 10
        base_wait = 2
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.SSLError as e:
                logging.warning(f"SSL Error encountered for {url} (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = base_wait ** attempt
                    logging.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                return self.session.get(url, timeout=timeout, verify=False)
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    wait_time = base_wait ** attempt
                    logging.warning(f"Timeout error accessing {url} (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                logging.error(f"Max retries reached for {url}")
                raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = base_wait ** attempt
                    logging.warning(f"Error accessing {url} (attempt {attempt + 1}/{max_retries}): {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                logging.error(f"Max retries reached for {url}: {str(e)}")
                raise
        
    def get_profile_links(self) -> Tuple[Set[str], Dict[str, Tuple[str, int, str]]]:
        """Extract all profile links from the ads pages including pagination"""
        profile_links = set()
        base_urls = {}  # Maps profile URL to (base_url, page_number, title)
        
        # Create images directory if it doesn't exist
        images_dir = os.path.join(os.getcwd(), 'images')
        os.makedirs(images_dir, exist_ok=True)
        
        # Load existing profile links if file exists
        if os.path.exists(self.profile_links_file):
            try:
                df = pd.read_excel(self.profile_links_file)
                profile_links.update(df['profile_url'].tolist())
            except Exception as e:
                logging.error(f"Error loading existing profile links: {str(e)}")
        
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
                        # Remove duplicates before saving
                        new_links = self.filter_existing_links(profile_links)
                        if new_links:
                            self.save_profile_links(new_links, base_urls)
                        else:
                            logging.info("No new links to save after filtering")
                    
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
    
    def download_image(self, image_url: str, profile_id: str, index: int) -> str:
        """Download an image and save it locally in profile-specific folder with retry mechanism"""
        if not image_url or not profile_id:
            logging.error(f"Invalid image URL or profile ID: {image_url}")
            return ''

        max_retries = 10
        base_timeout = 60  # Increased base timeout
        connect_timeout = 30  # Separate connect timeout
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # Create profile-specific directory inside images folder
                profile_dir = os.path.join(os.getcwd(), 'images', profile_id)
                os.makedirs(profile_dir, exist_ok=True)
                
                # Generate unique filename
                try:
                    file_extension = image_url.split('.')[-1].split('?')[0].lower()
                    if not file_extension or file_extension not in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                        file_extension = 'jpg'
                except Exception:
                    file_extension = 'jpg'
                    
                filename = f"{index}.{file_extension}"
                filepath = os.path.join(profile_dir, filename)
                
                # Download and save image with timeout
                read_timeout = base_timeout * (1.5 ** attempt)  # More gradual exponential backoff
                response = self.session.get(image_url, stream=True, timeout=(connect_timeout, read_timeout), verify=False)
                response.raise_for_status()
                
                # Verify content type is image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    last_error = f"Invalid content type for {image_url}: {content_type}"
                    if attempt == max_retries - 1:
                        logging.error(last_error)
                        return ''
                    continue
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                if os.path.getsize(filepath) == 0:
                    os.remove(filepath)
                    last_error = f"Downloaded empty file from {image_url}"
                    if attempt == max_retries - 1:
                        logging.error(last_error)
                        return ''
                    continue
                
                logging.info(f"Successfully downloaded image {index} for profile {profile_id}")
                return filepath
            except (requests.Timeout, requests.exceptions.SSLError, requests.exceptions.RequestException) as e:
                last_error = str(e)
                wait_time = 2 ** attempt  # Exponential backoff for wait time
                if attempt < max_retries - 1:
                    logging.warning(f"Error downloading image {image_url} (attempt {attempt + 1}/{max_retries}): {last_error}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to download image after {max_retries} attempts: {last_error}")
                    return ''
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logging.warning(f"Unexpected error downloading image {image_url} (attempt {attempt + 1}/{max_retries}): {last_error}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to download image after {max_retries} attempts: {last_error}")
                    return ''
        
        return ''

    def get_profile_details(self, profile_url: str) -> Dict:
        """Scrape details from a profile page"""
        try:
            # First check if profile exists in Profile Links
            if not os.path.exists(self.profile_links_file):
                logging.warning(f"Profile Links file not found: {self.profile_links_file}")
                return {}
                
            try:
                links_df = pd.read_excel(self.profile_links_file)
                if profile_url not in links_df['profile_url'].values:
                    logging.warning(f"Profile URL not found in Profile Links: {profile_url}")
                    return {}
                profile_links_data = links_df.set_index('profile_url').to_dict('index')
            except Exception as e:
                logging.error(f"Could not read Profile Links data: {str(e)}")
                return {}
                
            logging.info(f"Fetching details for profile: {profile_url}")
            response = self.make_request(profile_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the main content panel
            content_panel = soup.find('div', class_='webpanelcontent3')
            if not content_panel:
                logging.warning(f"Could not find main content panel for {profile_url}")
                return {}
            
            # Get base_url and title from profile links data
            link_info = profile_links_data.get(profile_url, {})
            
            # Generate unique profile ID from URL
            profile_id = hashlib.md5(profile_url.encode()).hexdigest()[:10]
                
            # Initialize details dictionary
            details = {
                'base_url': link_info.get('base_url', ''),
                'profile_url': profile_url,
                'title': link_info.get('title', ''),
                'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'images': []  # Ensure this field is initialized
            }
            
            try:
                # Extract main details from ad_detail_column
                detail_column = content_panel.find('div', class_='ad_detail_column')
                if detail_column and detail_column.get_text(strip=True):
                    details['description'] = detail_column.get_text(strip=True)
            except Exception as e:
                logging.warning(f"Error extracting detail columns for {profile_url}: {str(e)}")
                # Continue processing other fields even if detail extraction fails
            
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
                
            # Extract Skype ID
            skype_div = soup.find('a', class_='btn btn-primary skype_value')
            if skype_div:
                details['skype'] = skype_div.get_text(strip=True)
                
            # Extract KiK username
            kik_row = soup.find('strong', string='KiK:')
            if kik_row and kik_row.find_parent('div', class_='row'):
                kik_value = kik_row.find_parent('div', class_='row').find('div', class_='ad_detail_column')
                if kik_value:
                    details['kik'] = kik_value.get_text(strip=True)
            print(kik_row)
            # Extract posted by
            posted_by_row = soup.find('strong', string='Posted by:')
            if posted_by_row and posted_by_row.find_parent('div', class_='row'):
                posted_by_value = posted_by_row.find_parent('div', class_='row').find('div', class_='ad_detail_column')
                if posted_by_value and posted_by_value.find('a'):
                    details['posted_by'] = posted_by_value.find('a').get_text(strip=True)
            print(posted_by_row)
            # Extract posted date
            posted_date = soup.find('div', class_='ad_detail_column', string=lambda text: text and text is not None and 'ago' in text)
            if posted_date:
                details['posted_date'] = posted_date.get_text(strip=True)
            
            try:
                # Extract image URLs and prepare local paths first
                image_divs = soup.find_all('div', class_='ad-thumbnail-image') if soup else []
                for index, div in enumerate(image_divs):
                    if not div:
                        continue
                    img = div.find('img')
                    if img and img.get('src'):
                        image_url = img['src']
                        # Transform thumbnail URL to full-size URL by removing 't_' prefix
                        image_url = image_url.replace('/t_picture_', '/picture_')
                        details['images'].append(image_url)  # Add image URL to list

                # Download images immediately
                profile_id = profile_url.split('/')[-2]
                for index, image_url in enumerate(details['images']):
                    self.download_image(image_url, profile_id, index)
            except Exception as e:
                logging.warning(f"Error processing images for {profile_url}: {str(e)}")
                # Continue with the rest of the profile details even if image processing fails
            
            # Extract category, subcategory, and city
            try:
                # Find all webpanelhead elements
                webpanelheads = soup.find_all('div', class_='webpanelhead')
                logging.info(f"Found {len(webpanelheads)} webpanelhead elements")
                
                # Initialize fields
                details['category'] = ''
                details['subcategory'] = ''
                details['city'] = ''
                
                # Process each webpanelhead
                for idx, webpanelhead in enumerate(webpanelheads):
                    logging.info(f"Processing webpanelhead {idx + 1}:")
                    logging.info(f"Content: {webpanelhead.text.strip()}")
                    
                    # Get navigation links
                    links = webpanelhead.find_all('a')
                    link_texts = [link.get_text(strip=True) for link in links]
                    
                    # Skip if no useful navigation info
                    if len(links) <= 1:
                        continue
                        
                    # Check if this is the main navigation breadcrumb
                    if "Personal Ads" in link_texts:
                        logging.info("Found main navigation breadcrumb")
                        
                        # Extract category
                        if "Companionship" in link_texts:
                            details['category'] = "Companionship"
                            # Get city after Companionship
                            for i, text in enumerate(link_texts):
                                if text == "Companionship" and i + 1 < len(link_texts):
                                    city_parts = []
                                    city_parts.append(link_texts[i + 1])
                                    if i + 2 < len(link_texts) and link_texts[i + 2] in ['North', 'South', 'East', 'West']:
                                        city_parts.append(link_texts[i + 2])
                                    details['city'] = " ".join(city_parts)
                                    break
                                    
                        elif "Erotic Services" in link_texts:
                            details['category'] = "Erotic Services"
                            # Get subcategory after Erotic Services
                            for i, text in enumerate(link_texts):
                                if text == "Erotic Services" and i + 1 < len(link_texts):
                                    details['subcategory'] = link_texts[i + 1]
                                    break
                        
                        # Map location IDs
                        location_mapping = {
                            '4': 'Stockholm', '10': 'Stockholm North', '11': 'Stockholm City',
                            '12': 'Stockholm South', '45': 'Stockholm East', '16': 'Stockholm West',
                            '19': 'Västra Götaland', '20': 'Skåne',
                            '21': 'Sweden', '15': 'International'
                        }
                        
                        # Handle Stockholm special case
                        stockholm_found = False
                        stockholm_area = None
                        
                        for link in links:
                            href = link.get('href', '')
                            for loc_id, city_name in location_mapping.items():
                                if f'/ads/{loc_id}' in href:
                                    if 'Stockholm' in city_name:
                                        if loc_id == '4':
                                            # Base Stockholm
                                            stockholm_found = True
                                        else:
                                            # Stockholm area (North, South, etc)
                                            stockholm_area = city_name
                                        break
                                    elif not details['city']:
                                        details['city'] = city_name
                                        break
                        
                        # Set final Stockholm value
                        if stockholm_found or stockholm_area:
                            details['city'] = stockholm_area if stockholm_area else 'Stockholm City'
                        
                        # Break once we've found and processed the main navigation
                        if details['category']:
                            break
                            
                logging.info(f"Final extracted data - Category: {details.get('category')}, "
                          f"Subcategory: {details.get('subcategory')}, City: {details.get('city')}")
                        
            except Exception as e:
                logging.error(f"Error extracting category data: {str(e)}")
                if webpanelheads:
                    for idx, head in enumerate(webpanelheads):
                        logging.error(f"Webpanelhead {idx + 1} content: {head.text}")
                else:
                    logging.error("No webpanelhead elements found")

            logging.info(f"Successfully scraped details for {profile_url}")
            return details
        except Exception as e:
            logging.error(f"Error getting profile details for {profile_url}: {str(e)}")
            return {}
    
    def filter_existing_links(self, profile_links: Set[str]) -> Set[str]:
        """Filter out profile links that already exist in the Profile_Links.xlsx file"""
        if not os.path.exists(self.profile_links_file):
            # If file doesn't exist, all links are new
            return profile_links
            
        try:
            existing_df = pd.read_excel(self.profile_links_file)
            existing_links = set(existing_df['profile_url'].values)
            new_links = profile_links - existing_links
            logging.info(f"Found {len(new_links)} new profile links")
            return new_links
        except Exception as e:
            logging.error(f"Error reading existing profile links: {str(e)}")
            return set()
            
    def save_profile_links(self, new_links: Set[str], base_urls: Dict[str, Tuple[str, int, str]]):
        """Save new profile links to the Excel file"""
        try:
            # Prepare data for the new links
            new_data = [{
                'profile_url': url,
                'base_url': base_urls[url][0],
                'page_number': base_urls[url][1],
                'title': base_urls[url][2],
                'date_added': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            } for url in new_links]
            
            new_df = pd.DataFrame(new_data)
            
            if os.path.exists(self.profile_links_file):
                # Append to existing file
                existing_df = pd.read_excel(self.profile_links_file)
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                updated_df = new_df
                
            # Save to Excel file
            updated_df.to_excel(self.profile_links_file, index=False)
            logging.info(f"Saved {len(new_links)} new profile links to {self.profile_links_file}")
            
        except Exception as e:
            logging.error(f"Error saving profile links: {str(e)}")

    def save_profile_links(self, links: Set[str], base_urls: Dict[str, Tuple[str, int, str]]):
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
    
    def download_profile_images(self, profile: Dict) -> None:
        """Download images for a profile after its details have been saved using parallel processing"""
        try:
            if not profile.get('images'):
                logging.info(f"No images to download for profile {profile.get('profile_url', '')}")
                return
            
            profile_id = profile['profile_url'].split('/')[-2]
            if isinstance(profile['images'], str):
                image_urls = [url.strip() for url in profile['images'].split('|') if url.strip()]
            else:
                image_urls = [url for url in profile['images'] if url]
            
            if not image_urls:
                logging.info(f"No valid image URLs found for profile {profile.get('profile_url', '')}")
                return
            
            # Create profile directory
            profile_dir = os.path.join(os.getcwd(), 'images', profile_id)
            os.makedirs(profile_dir, exist_ok=True)
            
            # Check which images need to be downloaded
            futures = {}
            for index, image_url in enumerate(image_urls):
                # Check if image already exists and is valid
                local_path = os.path.join(profile_dir, f"{index}.jpg")
                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    logging.info(f"Image {index} already exists for profile {profile_id}, skipping...")
                    continue
                    
                # Submit only new or invalid images for download
                future = self.image_download_pool.submit(self.download_image, image_url, profile_id, index)
                futures[future] = {'url': image_url, 'index': index}
            
            if not futures:
                logging.info(f"All images already downloaded for profile {profile_id}")
                return
                
            # Track successful and failed downloads
            successful_downloads = 0
            failed_downloads = 0
            
            # Wait for all downloads to complete with timeout
            try:
                for future in as_completed(futures.keys(), timeout=300):
                    try:
                        result = future.result()
                        if result:
                            successful_downloads += 1
                        else:
                            failed_downloads += 1
                            image_info = futures[future]
                            logging.error(f"Failed to download image {image_info['index']} from {image_info['url']}")
                    except Exception as e:
                        failed_downloads += 1
                        image_info = futures[future]
                        logging.error(f"Error downloading image {image_info['index']} from {image_info['url']}: {str(e)}")
            except TimeoutError:
                logging.error(f"Timeout waiting for image downloads to complete for profile {profile.get('profile_url', '')}")
            
            logging.info(f"Completed image downloads for profile {profile_id}: {successful_downloads} successful, {failed_downloads} failed")
        except Exception as e:
            logging.error(f"Error in parallel image download process for profile {profile.get('profile_url', '')}: {str(e)}")

    def save_profile_details(self, profiles: List[Dict]):
        """Save profile details in main CSV file and create daily snapshot of new profiles"""
        try:
            if not profiles:
                return
            
            # Get current date for snapshot file
            current_date = datetime.now().strftime('%Y_%m_%d')
            # Set file path for Update directory
            snapshot_file = os.path.join('Update', f'new_profiles_{current_date}.csv')

            # Ensure all profiles have the same structure and order
            required_columns = [
                'base_url', 'profile_url', 'category', 'subcategory', 'title', 'scrape_date', 
                'images', 'description', 'city', 'login_country', 'price', 
                'skype', 'posted_date', 'phone', 'kik', 'posted_by',
                'folder_link'  # Added new columns
            ]

            # Initialize new fields for existing profiles
            for profile in profiles:
                for col in required_columns:
                    if col not in profile:
                        profile[col] = ''
                if isinstance(profile.get('images', []), list):
                    profile['images'] = '|'.join(profile['images'])
                # Add folder link using file:// protocol
                profile_id = profile['profile_url'].split('/')[-2]
                folder_path = os.path.abspath(os.path.join('images', profile_id))
                profile['folder_link'] = f'file://{folder_path}'
                
            df = pd.DataFrame(profiles)
            
            # Initialize new_profiles DataFrame
            new_profiles = df.copy()
            
            if os.path.exists(self.profile_csv):
                # Try different encodings for reading existing file
                encodings = ['utf-8', 'latin1', 'cp1252']
                existing_df = None
                
                for encoding in encodings:
                    try:
                        existing_df = pd.read_csv(self.profile_csv, encoding=encoding)
                        # Ensure existing DataFrame has all required columns
                        for col in required_columns:
                            if col not in existing_df.columns:
                                existing_df[col] = ''
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        logging.error(f"Error reading existing CSV with {encoding} encoding: {str(e)}")
                        continue
                
                if existing_df is not None:
                    # Create sets for efficient lookup
                    existing_urls = set(existing_df['profile_url'])
                    
                    # Filter out profiles that already exist
                    new_profiles = df[~df['profile_url'].isin(existing_urls)]
                    
                    # Combine with existing profiles for the main CSV
                    df = pd.concat([existing_df, new_profiles], ignore_index=True)
                    
                    # Log the number of new profiles found
                    if not new_profiles.empty:
                        logging.info(f"Found {len(new_profiles)} new profiles to add")
            
            # Ensure consistent column ordering for both files
            df = df[required_columns]
            new_profiles = new_profiles[required_columns]

            # Append new profiles to daily snapshot if any exist
            if not new_profiles.empty:
                # Check if snapshot file exists and append, otherwise create new
                if os.path.exists(snapshot_file):
                    new_profiles.to_csv(snapshot_file, mode='a', header=False, index=False, encoding='utf-8')
                else:
                    new_profiles.to_csv(snapshot_file, index=False, encoding='utf-8')
                logging.info(f"Appended {len(new_profiles)} new profiles to {snapshot_file}")
            
            # Save all profiles to main CSV
            df.to_csv(self.profile_csv, index=False, encoding='utf-8')
            logging.info(f"Updated main profile database with {len(profiles)} profiles")
        except Exception as e:
            logging.error(f"Error saving profile details: {str(e)}")
            raise
    

    
    def run(self):
        """Main scraping process"""
        logging.info('Starting scraping process...')
        
        try:
            try:
                # Get and save profile links
                current_links, base_urls = self.get_profile_links()
                if not current_links:
                    logging.warning('No profile links found')
                    return
                
                # Save all current links with their source information
                self.save_profile_links(current_links, base_urls)
                
                # Load existing profiles
                existing_profiles = self.load_existing_profiles()
            except Exception as e:
                logging.error(f"Error reading Profile_Links.xlsx: {str(e)}")
                return
            
            # For testing, use existing profiles from Profile_Links.xlsx
            try:
                links_df = pd.read_excel(self.profile_links_file)
                current_links = set(links_df['profile_url'].values)
            except Exception as e:
                logging.error(f"Error reading Profile_Links.xlsx: {str(e)}")
                return
            
            # Find new profiles
            new_profiles = current_links - existing_profiles
            if not new_profiles:
                logging.info('No new profiles found')
                return
            
            logging.info(f'Found {len(new_profiles)} new profiles')
            
            # Process both new and existing profiles
            processed_count = 0
            updated_count = 0
            
            # Process new profiles with immediate saving
            for url in new_profiles:
                details = self.get_profile_details(url)
                if details:
                    self.save_profile_details([details])  # Save to main CSV immediately
                    self.download_profile_images(details)  # Download images immediately after saving
                    processed_count += 1
                    logging.info(f'Processed and saved new profile {processed_count}/{len(new_profiles)}')
                time.sleep(1)  # Be nice to the server
            
            # Check existing profiles for updates with immediate saving
            existing_profiles = current_links - new_profiles
            for url in existing_profiles:
                details = self.get_profile_details(url)
                if details:
                    # Save profile details will handle the update check
                    if self.save_profile_details([details]):
                        self.download_profile_images(details)  # Download images for updated profiles
                        updated_count += 1
                        logging.info(f'Updated and saved existing profile {updated_count}/{len(existing_profiles)}')
                time.sleep(1)  # Be nice to the server
            
            logging.info(f'Completed processing: {processed_count} new profiles, {updated_count} updated profiles')
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


