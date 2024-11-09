import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, urljoin
import os
import string
import time
import io
import pdfplumber
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sqlite3
from queue import Queue

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_name='crawler.db'):
        self.db_path = db_name
        self.lock = threading.Lock()
        self.init_db()
    
    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS crawled_urls (
                    url TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status INTEGER
                )
            ''')
    
    def is_crawled(self, url):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                result = conn.execute('SELECT 1 FROM crawled_urls WHERE url = ?', (url,)).fetchone()
                return bool(result)

    def mark_crawled(self, url, status=1):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT OR REPLACE INTO crawled_urls (url, status) VALUES (?, ?)',
                           (url, status))
                conn.commit()

class WebCrawler:
    def __init__(self, start_url, output_dir='crawled_data', max_workers=10, max_depth=None):
        self.start_url = start_url
        self.domain = urlparse(start_url).netloc
        self.scheme = urlparse(start_url).scheme
        self.base_url = f"{self.scheme}://{self.domain}"
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.max_depth = max_depth
        self.session = self._init_session()
        self.db = DatabaseManager()
        self.url_queue = Queue()
        self.url_queue.put((start_url, 0))  # (url, depth)
        self.lock = threading.Lock()
        
        # Création des dossiers nécessaires
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(f"{output_dir}/html", exist_ok=True)
        os.makedirs(f"{output_dir}/pdf", exist_ok=True)
        
    def _init_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        return session

    def sanitize_filename(self, url):
        # Crée un nom de fichier valide à partir de l'URL
        name = re.sub(r'[^\w\-_.]', '_', url)
        if len(name) > 200:  # Limite la longueur du nom de fichier
            hash_part = hashlib.md5(url.encode()).hexdigest()[:10]
            name = f"{name[:150]}_{hash_part}"
        return name

    def extract_links(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Convertit les liens relatifs en absolus
            full_url = urljoin(base_url, href)
            # Vérifie si le lien est dans le même domaine
            if urlparse(full_url).netloc == self.domain:
                links.add(full_url)
        
        return links

    def process_pdf(self, content, url):
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                text = []
                for page in pdf.pages:
                    text.append(page.extract_text() or '')
                
                filename = f"{self.output_dir}/pdf/{self.sanitize_filename(url)}.txt"
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(text))
                logger.info(f"PDF saved: {url}")
        except Exception as e:
            logger.error(f"Error processing PDF {url}: {e}")

    def process_html(self, content, url):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Supprime les scripts et styles
            for element in soup(['script', 'style', 'meta', 'link']):
                element.decompose()
            
            text = soup.get_text(separator='\n', strip=True)
            
            filename = f"{self.output_dir}/html/{self.sanitize_filename(url)}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"URL: {url}\n\n{text}")
            
            logger.info(f"HTML saved: {url}")
            return soup.prettify()
        except Exception as e:
            logger.error(f"Error processing HTML {url}: {e}")
            return None

    def crawl_url(self, url, depth):
        if self.max_depth and depth > self.max_depth:
            return

        try:
            if self.db.is_crawled(url):
                return

            response = self.session.get(url, timeout=30, verify=False)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()

            if 'application/pdf' in content_type:
                self.process_pdf(response.content, url)
            else:
                html_content = self.process_html(response.text, url)
                if html_content:
                    for link in self.extract_links(html_content, url):
                        if not self.db.is_crawled(link):
                            self.url_queue.put((link, depth + 1))

            self.db.mark_crawled(url)
            time.sleep(0.1)  # Petit délai pour éviter la surcharge

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            self.db.mark_crawled(url, status=0)

    def crawl(self):
        logger.info(f"Starting crawl from {self.start_url}")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while True:
                try:
                    url, depth = self.url_queue.get(timeout=5)
                    executor.submit(self.crawl_url, url, depth)
                except queue.Empty:
                    if all(future.done() for future in executor._threads):
                        break
        
        logger.info("Crawling completed")

if __name__ == "__main__":
    START_URL = "https://www.example.com"  # Remplacez par votre URL
    crawler = WebCrawler(
        start_url=START_URL,
        output_dir="crawled_data",
        max_workers=10,
        max_depth=None
    )
    crawler.crawl()
