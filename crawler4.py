import requests
import re
import urllib.request
from bs4 import BeautifulSoup
from collections import deque
from html.parser import HTMLParser
from urllib.parse import urlparse, urljoin
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import hashlib
from typing import Set, List, Optional, Dict
import json
import mimetypes
from pathlib import Path

class EnhancedCrawler:
    def __init__(self, base_url: str, max_pages: int = 1000, 
                 concurrent_requests: int = 5, request_delay: float = 0.1,
                 timeout: int = 30, max_retries: int = 3,
                 download_files: bool = True):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.max_pages = max_pages
        self.concurrent_requests = concurrent_requests
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.download_files = download_files
        
        # Downloadable file types
        self.downloadable_extensions = {
            'document': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'],
            'spreadsheet': ['.xls', '.xlsx', '.csv', '.ods'],
            'presentation': ['.ppt', '.pptx', '.odp'],
            'archive': ['.zip', '.rar', '.7z', '.tar', '.gz'],
            'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg'],
            'audio': ['.mp3', '.wav', '.ogg', '.m4a'],
            'video': ['.mp4', '.avi', '.mkv', '.mov'],
            'other': []  # For uncategorized files
        }
        
        # Track downloaded files
        self.downloaded_files: Dict[str, Set[str]] = {
            category: set() for category in self.downloadable_extensions.keys()
        }
        
        # Session for maintaining connections
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Custom Web Crawler (with permission to bypass robots.txt)',
            'Accept': '*/*',  # Accept all content types
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        
        # Setup logging
        logging.basicConfig(
            filename=f'crawler_{self.domain}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize storage
        self._setup_storage()
        
        # Crawling state
        self.seen_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        self.content_hashes: Set[str] = set()

    def _setup_storage(self):
        """Setup directory structure for storing crawled data and files"""
        self.base_dir = f"crawled_data/{self.domain}"
        self.text_dir = f"{self.base_dir}/text"
        self.meta_dir = f"{self.base_dir}/metadata"
        self.files_dir = f"{self.base_dir}/files"
        
        # Create main directories
        for directory in [self.text_dir, self.meta_dir]:
            os.makedirs(directory, exist_ok=True)
            
        # Create directories for each file type
        if self.download_files:
            for category in self.downloadable_extensions.keys():
                os.makedirs(f"{self.files_dir}/{category}", exist_ok=True)

    def _get_file_category(self, url: str) -> Optional[str]:
        """Determine the category of a file based on its extension"""
        ext = Path(url).suffix.lower()
        for category, extensions in self.downloadable_extensions.items():
            if ext in extensions:
                return category
        if ext:  # If extension exists but not in our lists
            return 'other'
        return None

    def _is_downloadable_file(self, url: str) -> bool:
        """Check if the URL points to a downloadable file"""
        return self._get_file_category(url) is not None

    def _generate_safe_filename(self, url: str, category: str) -> str:
        """Generate a safe filename for downloaded files"""
        # Extract original filename from URL
        original_filename = os.path.basename(urlparse(url).path)
        
        # If no filename in URL, use URL hash
        if not original_filename:
            filename_hash = hashlib.md5(url.encode()).hexdigest()
            ext = mimetypes.guess_extension(url) or '.unknown'
            original_filename = f"{filename_hash}{ext}"
            
        # Ensure filename is unique within category
        base_path = f"{self.files_dir}/{category}"
        filename = original_filename
        counter = 1
        
        while os.path.exists(os.path.join(base_path, filename)):
            name, ext = os.path.splitext(original_filename)
            filename = f"{name}_{counter}{ext}"
            counter += 1
            
        return filename

    async def _download_file(self, url: str, category: str) -> Optional[dict]:
        """Download a file and save it to the appropriate directory"""
        try:
            response = self.session.get(
                url,
                timeout=self.timeout,
                stream=True  # Stream the response for large files
            )
            response.raise_for_status()
            
            # Generate safe filename and full path
            filename = self._generate_safe_filename(url, category)
            filepath = f"{self.files_dir}/{category}/{filename}"
            
            # Download file in chunks
            total_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
            
            # Record metadata
            metadata = {
                'url': url,
                'filename': filename,
                'category': category,
                'size': total_size,
                'timestamp': time.time(),
                'headers': dict(response.headers),
                'status_code': response.status_code
            }
            
            self.downloaded_files[category].add(url)
            self.logger.info(f"Downloaded file: {url} -> {filepath}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error downloading file {url}: {str(e)}")
            self.failed_urls.add(url)
            return None

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and should be processed"""
        if not url:
            return False
        
        parsed = urlparse(url)
        
        # Check if URL is within the same domain
        if parsed.netloc and parsed.netloc != self.domain:
            return False
            
        # Allow downloadable files but exclude certain patterns
        excluded_patterns = [
            r'(login|logout|signin|signout|auth)',
            r'#.*$'
        ]
        
        return not any(re.search(pattern, url.lower()) for pattern in excluded_patterns)

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicate crawling"""
        url = urljoin(self.base_url, url)
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')

    async def _fetch_url(self, url: str) -> Optional[dict]:
        """Fetch URL content with retries and error handling"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.timeout,
                    allow_redirects=True
                )
                response.raise_for_status()
                
                # Check content type
                if 'text/html' not in response.headers.get('Content-Type', ''):
                    self.logger.warning(f"Skipping non-HTML content: {url}")
                    return None
                
                # Check for duplicate content using hash
                content_hash = hashlib.md5(response.content).hexdigest()
                if content_hash in self.content_hashes:
                    self.logger.info(f"Duplicate content found: {url}")
                    return None
                self.content_hashes.add(content_hash)
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract metadata
                metadata = {
                    'url': url,
                    'title': soup.title.string if soup.title else None,
                    'timestamp': time.time(),
                    'headers': dict(response.headers),
                    'status_code': response.status_code
                }
                
                # Extract text content
                text_content = ' '.join(soup.stripped_strings)
                
                return {
                    'metadata': metadata,
                    'content': text_content,
                    'html': response.text
                }
                
            except Exception as e:
                self.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {str(e)}")
                time.sleep(self.request_delay * (attempt + 1))
        
        self.failed_urls.add(url)
        return None

    def _extract_links(self, html: str, source_url: str) -> List[str]:
        """Extract and normalize links from HTML content"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for anchor in soup.find_all('a', href=True):
            url = self._normalize_url(anchor['href'])
            if self._is_valid_url(url):
                links.append(url)
                
        return list(set(links))

    def _save_data(self, url: str, data: dict):
        """Save crawled data and metadata to files"""
        if not data:
            return
            
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Save text content
        text_path = f"{self.text_dir}/{url_hash}.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(data['content'])
            
        # Save metadata
        meta_path = f"{self.meta_dir}/{url_hash}.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(data['metadata'], f, indent=2)

    async def _process_url(self, url: str):
        """Process a URL - either crawl it or download it if it's a file"""
        if self._is_downloadable_file(url):
            category = self._get_file_category(url)
            if category:
                metadata = await self._download_file(url, category)
                if metadata:
                    # Save file metadata
                    meta_path = f"{self.meta_dir}/{hashlib.md5(url.encode()).hexdigest()}_file.json"
                    with open(meta_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2)
                return None
        else:
            return await self._fetch_url(url)

    async def crawl(self):
        """Main crawling method"""
        queue = deque([self.base_url])
        self.seen_urls.add(self.base_url)
        
        with ThreadPoolExecutor(max_workers=self.concurrent_requests) as executor:
            while queue and len(self.seen_urls) < self.max_pages:
                url = queue.popleft()
                self.logger.info(f"Processing: {url}")
                
                # Process URL (either crawl or download)
                data = await self._process_url(url)
                
                if data:  # If it's a webpage with HTML content
                    # Save the webpage data
                    self._save_data(url, data)
                    
                    # Extract and queue new links
                    new_links = self._extract_links(data['html'], url)
                    for link in new_links:
                        if link not in self.seen_urls:
                            queue.append(link)
                            self.seen_urls.add(link)
                
                # Respect rate limiting
                time.sleep(self.request_delay)
        
        # Save crawl statistics
        stats = {
            'pages_crawled': len(self.seen_urls),
            'files_downloaded': {
                category: len(files) 
                for category, files in self.downloaded_files.items()
            },
            'failed_urls': list(self.failed_urls),
            'total_time': time.time() - time.time(),
        }
        
        with open(f"{self.base_dir}/crawl_stats.json", 'w') as f:
            json.dump(stats, f, indent=2)

# Usage example
if __name__ == "__main__":
    crawler = EnhancedCrawler(
        base_url="https://openai.com",
        max_pages=100,
        concurrent_requests=5,
        request_delay=0.2,
        download_files=True  # Enable file downloads
    )
    
    import asyncio
    asyncio.run(crawler.crawl())
