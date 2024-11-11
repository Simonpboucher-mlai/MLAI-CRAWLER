import asyncio
import aiohttp
import hashlib
import json
import logging
import mimetypes
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import fitz  # PyMuPDF
import pytesseract
from bs4 import BeautifulSoup
from pdf2image import convert_from_path
from PIL import Image
from PyPDF2 import PdfReader


class PDFProcessor:
    """Handle advanced PDF processing and text extraction"""

    def __init__(self, logger):
        self.logger = logger
        self.tesseract_config = r'--oem 3 --psm 6'

    def _extract_with_pymupdf(self, pdf_path: str) -> str:
        """Extract text using PyMuPDF (MuPDF)"""
        try:
            text = []
            with fitz.open(pdf_path) as pdf:
                for page in pdf:
                    text.append(page.get_text())
            return "\n".join(text)
        except Exception as e:
            self.logger.error(f"PyMuPDF extraction failed: {e}")
            return ""

    def _extract_with_pypdf2(self, pdf_path: str) -> str:
        """Extract text using PyPDF2"""
        try:
            text = []
            with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text.append(page_text)
            return "\n".join(text)
        except Exception as e:
            self.logger.error(f"PyPDF2 extraction failed: {e}")
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
            self.logger.error(f"OCR extraction failed: {e}")
            return ""

    def extract_text_from_pdf(self, pdf_path: str) -> Tuple[str, str]:
        """Extract text using multiple methods and return the best result"""
        methods = {
            'pymupdf': self._extract_with_pymupdf,
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
                self.logger.error(f"Error with {method_name}: {e}")
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
        ext = Path(urlparse(url).path).suffix.lower()
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
            ext = mimetypes.guess_extension(urlparse(url).path) or '.unknown'
            original_filename = f"{filename_hash}{ext}"

        filename = original_filename
        counter = 1

        while os.path.exists(os.path.join(base_path, filename)):
            name, ext = os.path.splitext(original_filename)
            filename = f"{name}_{counter}{ext}"
            counter += 1

        return filename


class ProxyManager:
    """Manage and rotate proxies"""

    def __init__(self, logger, proxy_file: str = None):
        self.logger = logger
        self.proxies: List[Dict[str, Any]] = []
        self.current_index = 0
        self.proxy_file = proxy_file or "proxies.json"
        self.last_update = datetime.now()
        self.update_interval = timedelta(hours=1)  # Update proxy list every hour
        self.proxy_stats = {
            'total_used': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rotations': 0
        }

        # Load initial proxies
        self._load_proxies()

    def _load_proxies(self):
        """Load proxies from file or default list"""
        try:
            if os.path.exists(self.proxy_file):
                with open(self.proxy_file, 'r') as f:
                    proxy_lines = f.read().splitlines()
                    loaded_proxies = []
                    for line in proxy_lines:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(':')
                        if len(parts) == 4:
                            ip, port, username, password = parts
                            proxy_url = f"http://{username}:{password}@{ip}:{port}"
                            proxy = {'http': proxy_url, 'https': proxy_url}
                            loaded_proxies.append(proxy)
                        else:
                            self.logger.warning(f"Invalid proxy format: {line}")
                    self.proxies = loaded_proxies

            if not self.proxies:
                # Default to direct connection if no proxies are loaded
                self.proxies = [{"http": None, "https": None}]  # Direct connection as fallback
                self.logger.warning("No proxies loaded, using direct connection")
        except Exception as e:
            self.logger.error(f"Error loading proxies: {e}")
            self.proxies = [{"http": None, "https": None}]

    async def _test_proxy(self, proxy: Dict[str, str], timeout: int = 5) -> bool:
        """Test if proxy is working"""
        try:
            async with aiohttp.ClientSession() as session:
                start_time = time.time()
                async with session.get(
                    'https://httpbin.org/ip',
                    proxy=proxy.get('http') or proxy.get('https'),
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        response_time = time.time() - start_time
                        proxy['speed'] = response_time
                        return True
            return False
        except Exception:
            return False

    async def _test_all_proxies(self):
        """Test all proxies concurrently"""
        working_proxies = []
        tasks = []

        for proxy in self.proxies:
            task = asyncio.create_task(self._test_proxy(proxy))
            tasks.append((proxy, task))

        for proxy, task in tasks:
            try:
                is_working = await task
                if is_working:
                    working_proxies.append(proxy)
            except Exception as e:
                self.logger.error(f"Error testing proxy {proxy}: {e}")

        return working_proxies

    async def _update_proxies(self):
        """Update and verify proxy list"""
        self.logger.info("Updating proxy list...")

        # Test all proxies
        working_proxies = await self._test_all_proxies()

        # Update proxy list
        if working_proxies:
            self.proxies = sorted(working_proxies, key=lambda x: x.get('speed', float('inf')))
        else:
            self.proxies = [{"http": None, "https": None}]
            self.logger.warning("No working proxies found. Using direct connection.")

        self.last_update = datetime.now()
        self.logger.info(f"Updated proxy list. Working proxies: {len(self.proxies)}")

    async def get_proxy(self) -> Dict[str, str]:
        """Get next working proxy"""
        self.proxy_stats['total_used'] += 1

        # Update proxies if needed
        if datetime.now() - self.last_update > self.update_interval:
            await self._update_proxies()

        if not self.proxies:
            return {"http": None, "https": None}

        # Rotate through proxies
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        self.proxy_stats['rotations'] += 1

        return proxy

    def record_result(self, success: bool):
        """Record proxy request result"""
        if success:
            self.proxy_stats['successful_requests'] += 1
        else:
            self.proxy_stats['failed_requests'] += 1

    def get_stats(self) -> Dict[str, int]:
        """Get proxy usage statistics"""
        return {
            **self.proxy_stats,
            'current_proxies': len(self.proxies)
        }


class EnhancedCrawler:
    def __init__(self, base_url: str, max_pages: int = 1000,
                 concurrent_requests: int = 5, request_delay: float = 0.1,
                 timeout: int = 30, max_retries: int = 3,
                 download_files: bool = True,
                 proxy_file: str = None):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.max_pages = max_pages
        self.concurrent_requests = concurrent_requests
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.download_files = download_files

        # Setup logging first
        self._setup_logging()

        # Initialize handlers
        self.file_handler = FileHandler(self.logger)
        self.pdf_processor = PDFProcessor(self.logger)
        self.proxy_manager = ProxyManager(self.logger, proxy_file)

        # Initialize storage
        self._setup_storage()

        # Initialize session and tracking
        self._init_tracking()

        # Start time for crawl duration
        self.start_time = None

    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        self.logger = logging.getLogger(f"crawler_{self.domain}")
        self.logger.setLevel(logging.INFO)

        # Create file handler
        log_file = os.path.join(log_dir, f'crawler_{self.domain}_{int(time.time())}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger if not already added
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

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
                'pymupdf': 0,
                'pypdf2': 0,
                'ocr': 0
            }
        }
        self.crawl_stats = {
            'pages_processed': 0,
            'files_downloaded': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    async def _handle_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[aiohttp.ClientResponse]:
        """Handle HTTP request with proxy rotation and retries"""
        for attempt in range(self.max_retries):
            try:
                proxy = await self.proxy_manager.get_proxy()
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method,
                        url,
                        proxy=proxy.get('http') if url.startswith('http://') else proxy.get('https'),
                        timeout=self.timeout,
                        **kwargs
                    ) as response:
                        response.raise_for_status()
                        self.proxy_manager.record_result(True)
                        return response

            except Exception as e:
                self.logger.error(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                self.proxy_manager.record_result(False)
                if attempt == self.max_retries - 1:
                    self.failed_urls.add(url)
                    self.crawl_stats['errors'] += 1
                await asyncio.sleep(self.request_delay * (attempt + 1))

        return None

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicate crawling"""
        url = urljoin(self.base_url, url)
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and should be processed"""
        if not url:
            return False

        parsed = urlparse(url)

        if parsed.netloc and parsed.netloc != self.domain:
            return False

        excluded_patterns = [
            r'(login|logout|signin|signout|auth)',
            r'#.*$',
            r'\.(css|js|json|xml)$'
        ]

        return not any(re.search(pattern, url.lower()) for pattern in excluded_patterns)

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
            text_path = os.path.join(self.text_dir, f"{url_hash}_pdf.txt")

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
            self.logger.error(f"Error processing PDF {url}: {e}")
            self.pdf_stats['failed'] += 1
            return None

    async def _download_file(self, url: str, category: str) -> Optional[dict]:
        """Download and process file"""
        try:
            response = await self._handle_request(url)
            if not response:
                return None

            filepath = os.path.join(
                self.files_dir, category, self.file_handler.generate_safe_filename(url, os.path.join(self.files_dir, category))
            )

            # Download file in chunks
            total_size = 0
            with open(filepath, 'wb') as f:
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    total_size += len(chunk)

            metadata = {
                'url': url,
                'filename': os.path.basename(filepath),
                'category': category,
                'size': total_size,
                'timestamp': time.time(),
                'headers': dict(response.headers),
                'status_code': response.status,
                'filepath': filepath
            }

            if category == 'document' and filepath.lower().endswith('.pdf'):
                pdf_metadata = await self._process_pdf(filepath, url)
                if pdf_metadata:
                    metadata['pdf_extraction'] = pdf_metadata

            self.downloaded_files[category].add(url)
            self.crawl_stats['files_downloaded'] += 1
            self.logger.info(f"Downloaded file: {url} -> {filepath}")

            return metadata

        except Exception as e:
            self.logger.error(f"Error downloading file {url}: {e}")
            self.failed_urls.add(url)
            self.crawl_stats['errors'] += 1
            return None

    async def _fetch_url(self, url: str) -> Optional[dict]:
        """Fetch URL content with retries"""
        response = await self._handle_request(url)
        if not response:
            return None

        try:
            if 'text/html' not in response.headers.get('content-type', ''):
                self.logger.warning(f"Skipping non-HTML content: {url}")
                return None

            content = await response.text()
            content_hash = hashlib.md5(content.encode()).hexdigest()

            if content_hash in self.content_hashes:
                self.logger.info(f"Duplicate content found: {url}")
                return None

            self.content_hashes.add(content_hash)
            soup = BeautifulSoup(content, 'html.parser')

            metadata = {
                'url': url,
                'title': soup.title.string if soup.title else None,
                'timestamp': time.time(),
                'headers': dict(response.headers),
                'status_code': response.status
            }

            text_content = ' '.join(soup.stripped_strings)

            self.crawl_stats['pages_processed'] += 1

            return {
                'metadata': metadata,
                'content': text_content,
                'html': content
            }

        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {e}")
            self.crawl_stats['errors'] += 1
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
        """Save crawled data and metadata"""
        if not data:
            return

        url_hash = hashlib.md5(url.encode()).hexdigest()

        # Save text content
        text_path = os.path.join(self.text_dir, f"{url_hash}.txt")
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(data['content'])

        # Save metadata
        meta_path = os.path.join(self.meta_dir, f"{url_hash}.json")
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(data['metadata'], f, indent=2)

    async def _process_url(self, url: str):
        """Process a URL - either crawl it or download it if it's a file"""
        if self.file_handler.is_downloadable_file(url):
            category = self.file_handler.get_file_category(url)
            if category:
                metadata = await self._download_file(url, category)
                if metadata:
                    # Save file metadata
                    meta_path = os.path.join(self.meta_dir, f"{hashlib.md5(url.encode()).hexdigest()}_file.json")
                    with open(meta_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2)
                return None
        else:
            return await self._fetch_url(url)

    async def crawl(self):
        """Main crawling method"""
        self.start_time = time.time()
        self.crawl_stats['start_time'] = self.start_time

        queue = deque([self.base_url])
        self.seen_urls.add(self.base_url)

        async def process_batch(urls: List[str]):
            """Process a batch of URLs concurrently"""
            tasks = [self._process_url(url) for url in urls]
            return await asyncio.gather(*tasks)

        while queue and len(self.seen_urls) < self.max_pages:
            # Process URLs in batches
            batch_size = min(self.concurrent_requests, len(queue))
            batch_urls = [queue.popleft() for _ in range(batch_size)]

            self.logger.info(f"Processing batch of {len(batch_urls)} URLs")
            results = await process_batch(batch_urls)

            for url, data in zip(batch_urls, results):
                if data:  # If it's a webpage with HTML content
                    self._save_data(url, data)

                    # Extract and queue new links
                    new_links = self._extract_links(data['html'], url)
                    for link in new_links:
                        if link not in self.seen_urls:
                            queue.append(link)
                            self.seen_urls.add(link)

            # Respect rate limiting
            await asyncio.sleep(self.request_delay)

        # Record end time and save statistics
        self.crawl_stats['end_time'] = time.time()
        self.crawl_stats['duration'] = self.crawl_stats['end_time'] - self.start_time

        stats = {
            'crawl_stats': self.crawl_stats,
            'proxy_stats': self.proxy_manager.get_stats(),
            'pages_crawled': len(self.seen_urls),
            'files_downloaded': {
                category: len(files)
                for category, files in self.downloaded_files.items()
            },
            'failed_urls': list(self.failed_urls),
            'pdf_processing': self.pdf_stats
        }

        # Save statistics
        with open(os.path.join(self.base_dir, "crawl_stats.json"), 'w') as f:
            json.dump(stats, f, indent=2)

        self.logger.info("Crawl completed. Statistics saved.")


async def main():
    """Main function to run the crawler"""
    print("""Required packages for crawler with PDF processing and proxy rotation:
pip install aiohttp beautifulsoup4 PyPDF2 pdf2image pytesseract PyMuPDF Pillow

System requirements:
1. Tesseract OCR must be installed:
   - Ubuntu: sudo apt-get install tesseract-ocr
   - macOS: brew install tesseract
   - Windows: download installer from https://github.com/UB-Mannheim/tesseract/wiki

2. Poppler utilities (for pdf2image):
   - Ubuntu: sudo apt-get install poppler-utils
   - macOS: brew install poppler
   - Windows: download from http://blog.alivate.com.au/poppler-windows/
""")

    # Initialize and run crawler
    crawler = EnhancedCrawler(
        base_url="https://example.com",  # Remplacez par l'URL cible
        max_pages=100,
        concurrent_requests=5,
        request_delay=0.2,
        download_files=True,
        proxy_file="proxies.txt"  # Assurez-vous que le chemin est correct
    )

    await crawler.crawl()

if __name__ == "__main__":
    asyncio.run(main())
