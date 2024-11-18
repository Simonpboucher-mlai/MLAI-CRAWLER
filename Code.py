import requests
import re
import os
import time
import logging
from collections import deque
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor
from typing import Set, List, Optional, Dict, Tuple
import hashlib
import json
from pathlib import Path
import PyPDF2
from pdf2image import convert_from_path
import pytesseract
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
import asyncio
import aiohttp

class PDFProcessor:
    """Handle advanced PDF processing and text extraction"""
    
    def __init__(self, logger):
        self.logger = logger
        self.tesseract_config = r'--oem 3 --psm 6'

    def _extract_with_pdfminer(self, pdf_path: str) -> str:
        """Extract text using PyMuPDF (MuPDF)"""
        try:
            text = []
            with fitz.open(pdf_path) as pdf:
                for page in pdf:
                    text.append(page.get_text())
            return "\n".join(text)
        except Exception as e:
            self.logger.error(f"PyMuPDF extraction failed: {str(e)}")
            return ""

    def _extract_with_pypdf2(self, pdf_path: str) -> str:
        """Extract text using PyPDF2"""
        try:
            text = []
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text.append(page.extract_text())
            return "\n".join(text)
        except Exception as e:
            self.logger.error(f"PyPDF2 extraction failed: {str(e)}")
            return ""

    def _extract_with_ocr(self, pdf_path: str) -> str:
        """Extract text using OCR (Tesseract)"""
        try:
            text = []
            images = convert_from_path(pdf_path)
            
            for image in images:
                text.append(pytesseract.image_to_string(image, config=self.tesseract_config))
                
            return "\n".join(text)
        except Exception as e:
            self.logger.error(f"OCR extraction failed: {str(e)}")
            return ""

    def extract_text_from_pdf(self, pdf_path: str) -> Tuple[str, str]:
        """Extract text using multiple methods and return the best result"""
        methods = {
            'pdfminer': self._extract_with_pdfminer,
            'pypdf2': self._extract_with_pypdf2,
            'ocr': self._extract_with_ocr
        }
        
        best_text = ""
        best_method = ""
        max_length = 0
        
        for method_name, extract_method in methods.items():
            try:
                extracted_text = extract_method(pdf_path)
                text_length = len(extracted_text.strip())
                
                if text_length > max_length:
                    max_length = text_length
                    best_text = extracted_text
                    best_method = method_name
                    
            except Exception as e:
                self.logger.error(f"Error with {method_name}: {str(e)}")
                continue
        
        return best_text, best_method

class FileHandler:
    """Handle file downloads and processing"""
    
    def __init__(self, logger):
        self.logger = logger
        self.downloadable_extensions = {
            'document': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'],
            'spreadsheet': ['.xls', '.xlsx', '.csv', '.ods'],
            'presentation': ['.ppt', '.pptx', '.odp'],
            'archive': ['.zip', '.rar', '.7z', '.tar', '.gz'],
            'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg'],
            'audio': ['.mp3', '.wav', '.ogg', '.m4a'],
            'video': ['.mp4', '.avi', '.mkv', '.mov'],
            'code': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.h'],
            'data': ['.json', '.xml', '.yaml', '.sql'],
            'ebook': ['.epub', '.mobi', '.azw'],
            'other': []
        }

    def get_file_category(self, url: str) -> Optional[str]:
        """Determine file category based on extension"""
        ext = Path(url).suffix.lower()
        for category, extensions in self.downloadable_extensions.items():
            if ext in extensions:
                return category
        if ext:
            return 'other'
        return None

    def is_downloadable_file(self, url: str) -> bool:
        """Check if URL points to a downloadable file"""
        return self.get_file_category(url) is not None

    def generate_safe_filename(self, url: str, base_path: str) -> str:
        """Generate safe and unique filename"""
        original_filename = os.path.basename(urlparse(url).path)
        
        if not original_filename:
            filename_hash = hashlib.md5(url.encode()).hexdigest()
            ext = mimetypes.guess_extension(url) or '.unknown'
            original_filename = f"{filename_hash}{ext}"
            
        filename = original_filename
        counter = 1
        
        while os.path.exists(os.path.join(base_path, filename)):
            name, ext = os.path.splitext(original_filename)
            filename = f"{name}_{counter}{ext}"
            counter += 1
            
        return filename

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
        
        # Initialize handlers
        self.file_handler = FileHandler(None)  # Will set logger after setup
        self.pdf_processor = PDFProcessor(None)  # Will set logger after setup
        
        # Setup logging
        self._setup_logging()
        
        # Update handlers with logger
        self.file_handler.logger = self.logger
        self.pdf_processor.logger = self.logger
        
        # Initialize storage
        self._setup_storage()
        
        # Initialize session
        self._setup_session()
        
        # Initialize tracking
        self._init_tracking()

    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            filename=f'logs/crawler_{self.domain}_{int(time.time())}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _setup_storage(self):
        """Setup storage directories"""
        self.base_dir = f"crawled_data/{self.domain}"
        self.text_dir = f"{self.base_dir}/text"
        self.meta_dir = f"{self.base_dir}/metadata"
        self.files_dir = f"{self.base_dir}/files"
        
        # Create main directories
        for directory in [self.text_dir, self.meta_dir]:
            os.makedirs(directory, exist_ok=True)
            
        # Create directories for file categories
        if self.download_files:
            for category in self.file_handler.downloadable_extensions.keys():
                os.makedirs(f"{self.files_dir}/{category}", exist_ok=True)

    def _setup_session(self):
        """Setup requests session with custom headers"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Custom Web Crawler (with permission to bypass robots.txt)',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })

    def _init_tracking(self):
        """Initialize tracking variables"""
        self.seen_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        self.content_hashes: Set[str] = set()
        self.downloaded_files: Dict[str, Set[str]] = {
            category: set() for category in self.file_handler.downloadable_extensions.keys()
        }
        self.pdf_stats = {
            'processed': 0,
            'failed': 0,
            'extraction_methods': {
                'pdfminer': 0,
                'pypdf2': 0,
                'ocr': 0
            }
        }

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicate crawling"""
        url = urljoin(self.base_url, url)
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')

    async def _process_pdf(self, pdf_path: str, url: str) -> Optional[dict]:
        """Process PDF file and extract text"""
        try:
            extracted_text, method_used = self.pdf_processor.extract_text_from_pdf(pdf_path)
            
            if not extracted_text.strip():
                self.logger.warning(f"No text extracted from PDF: {url}")
                self.pdf_stats['failed'] += 1
                return None
            
            self.pdf_stats['processed'] += 1
            self.pdf_stats['extraction_methods'][method_used] += 1
            
            url_hash = hashlib.md5(url.encode()).hexdigest()
            text_path = f"{self.text_dir}/{url_hash}_pdf.txt"
            
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            
            metadata = {
                'url': url,
                'source_file': pdf_path,
                'extraction_method': method_used,
                'timestamp': time.time(),
                'text_file': text_path,
                'text_length': len(extracted_text)
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error processing PDF {url}: {str(e)}")
            self.pdf_stats['failed'] += 1
            return None

    async def _download_file(self, url: str, category: str) -> Optional[dict]:
        """Download and process file"""
        try:
            response = await self._get_request(url)
            filepath = f"{self.files_dir}/{category}/{self.file_handler.generate_safe_filename(url, f'{self.files_dir}/{category}')}"
            
            total_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
            
            metadata = {
                'url': url,
                'filename': os.path.basename(filepath),
                'category': category,
                'size': total_size,
                'timestamp': time.time(),
                'headers': dict(response.headers),
                'status_code': response.status_code,
                'filepath': filepath
            }
            
            if category == 'document' and filepath.lower().endswith('.pdf'):
                pdf_metadata = await self._process_pdf(filepath, url)
                if pdf_metadata:
                    metadata['pdf_extraction'] = pdf_metadata
            
            self.downloaded_files[category].add(url)
            self.logger.info(f"Downloaded file: {url} -> {filepath}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error downloading file {url}: {str(e)}")
            self.failed_urls.add(url)
            return None

    async def _get_request(self, url: str) -> Optional[aiohttp.ClientResponse]:
        """Send asynchronous HTTP GET request"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=self.timeout) as response:
                    return response
        except Exception as e:
            self.logger.error(f"Error fetching URL {url}: {str(e)}")
            return None

    async def _process_url(self, url: str):
        """Process a URL - either crawl it or download it if it's a file"""
        if self.file_handler.is_downloadable_file(url):
            category = self.file_handler.get_file_category(url)
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

    async def _fetch_url(self, url: str) -> Optional[dict]:
        """Fetch URL content with retries"""
        for attempt in range(self.max_retries):
            try:
                response = await self._get_request(url)
                if not response or 'text/html' not in response.headers.get('Content-Type', ''):
                    self.logger.warning(f"Skipping non-HTML content: {url}")
                    return None
                
                content_hash = hashlib.md5(await response.read()).hexdigest()
                if content_hash in self.content_hashes:
                    self.logger.info(f"Duplicate content found: {url}")
                    return None
                self.content_hashes.add(content_hash)
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                metadata = {
                    'url': url,
                    'title': soup.title.string if soup.title else None,
                    'timestamp': time.time(),
                    'headers': dict(response.headers),
                    'status_code': response.status
                }
                
                text_content = ' '.join(soup.stripped_strings)
                
                return {
                    'metadata': metadata,
                    'content': text_content,
                    'html': html
                }
                
            except Exception as e:
                self.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {str(e)}")
                time.sleep(self.request_delay * (attempt + 1))
        
        self.failed_urls.add(url)
        return None

    async def crawl(self):
        """Main crawling method"""
        queue = deque([self.base_url])
        self.seen_urls.add(self.base_url)
        
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
            await asyncio.sleep(self.request_delay)

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
        """Save crawled data and metadata"""
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

def main():
    """Main function to run the crawler"""
    # Initialize and run crawler
    crawler = EnhancedCrawler(
        base_url="https://example.com",  # Remplacez par votre URL cible
        max_pages=100,
        concurrent_requests=5,
        request_delay=0.2,
        download_files=True
    )
    
    asyncio.run(crawler.crawl())

if __name__ == "__main__":
    main()
