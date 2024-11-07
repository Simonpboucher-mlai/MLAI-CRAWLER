import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, urljoin
import os
import string
import time
import io
import pdfplumber  # Pour une meilleure extraction des tableaux
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sqlite3
from queue import Queue
import xml.etree.ElementTree as ET  # Pour parser le sitemap XML
from bs4.element import Comment

# Configuration du logging pour le crawler
crawler_logger = logging.getLogger('crawler_logger')
crawler_logger.setLevel(logging.INFO)
crawler_handler = logging.FileHandler("crawler_log.txt")
crawler_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
crawler_logger.addHandler(crawler_handler)
crawler_logger.addHandler(logging.StreamHandler())

# Regex pattern to match a URL
HTTP_URL_PATTERN = r'^http[s]*://.+'

class DatabaseHandler:
    """Gestionnaire de base de données pour stocker les URL visitées."""
    def __init__(self, db_path='crawler.db'):
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.setup_database()

    def setup_database(self):
        with self.lock:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url TEXT PRIMARY KEY
                )
            ''')
            self.conn.commit()

    def is_visited(self, url):
        with self.lock:
            self.cursor.execute("SELECT 1 FROM visited_urls WHERE url = ?", (url,))
            return self.cursor.fetchone() is not None

    def mark_as_visited(self, url):
        with self.lock:
            self.cursor.execute("INSERT OR IGNORE INTO visited_urls (url) VALUES (?)", (url,))
            self.conn.commit()

    def close(self):
        self.conn.close()

class Crawler:
    def __init__(self, start_url, max_workers=5):
        self.start_url = start_url
        self.local_domain = urlparse(start_url).netloc
        self.base_url = f"{urlparse(start_url).scheme}://{self.local_domain}"
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CustomCrawler/1.0 (+http://www.votredomaine.com/)'})
        self.max_workers = max_workers

        # Dossier de sortie
        self.output_dir = f"text/{self.local_domain}"
        os.makedirs(self.output_dir, exist_ok=True)

        # Gestionnaire de base de données pour les URL visitées
        self.db = DatabaseHandler()

        # Queue thread-safe pour gérer les URLs à crawler
        self.queue = Queue()
        self.queue.put(self.start_url)

        # Liste des sitemaps à traiter
        self.sitemaps = []

    def sanitize_filename(self, filename):
        short_filename = filename[:50]
        filename_hash = hashlib.md5(filename.encode()).hexdigest()
        valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
        filename = ''.join(c for c in short_filename if c in valid_chars)
        filename = f"{filename}_{filename_hash}"
        filename = filename.rstrip('.')
        return filename

    def extract_text_from_pdf(self, pdf_content, final_url):
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                text = f"[Source]({final_url})\n\n"  # Lien markdown au début
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                    # Extraction des tableaux
                    tables = page.extract_tables()
                    for table in tables:
                        if table:
                            # Convertir le tableau en texte structuré en gérant les None
                            table_text = "\n".join([
                                "\t".join([cell if cell is not None else "" for cell in row]) 
                                for row in table
                            ])
                            text += f"\n**Tableau:**\n{table_text}\n"
            return text
        except Exception as e:
            crawler_logger.error(f"Erreur lors de l'extraction du contenu du PDF: {e}", exc_info=True)
            return ""

    def clean_text(self, text):
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text

    def extract_text_from_html(self, html_content, final_url):
        soup = BeautifulSoup(html_content, "lxml")  # Utilisation de lxml pour une meilleure gestion
        for script in soup(["script", "style", "noscript"]):
            script.decompose()

        # Filtrer les textes visibles
        text_elements = soup.find_all(text=True)
        visible_texts = filter(self.tag_visible, text_elements)
        text = f"[Source]({final_url})\n\n"  # Lien markdown au début
        text += " ".join(t.strip() for t in visible_texts)
        return self.clean_text(text)

    def tag_visible(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]', 'noscript']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def extract_text_alternative(self, html_content, final_url):
        soup = BeautifulSoup(html_content, "lxml")
        text = f"[Source]({final_url})\n\n"  # Lien markdown au début
        text += self.clean_text(soup.get_text(separator=' ', strip=True))
        return text

    def normalize_url(self, url):
        parsed = urlparse(url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return normalized.rstrip('/')

    def get_hyperlinks(self, url):
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            if not response.headers.get('Content-Type', '').startswith("text/html"):
                return []
            html = response.text
        except requests.RequestException as e:
            crawler_logger.error(f"Erreur lors de la récupération de {url}: {e}", exc_info=True)
            return []

        soup = BeautifulSoup(html, "lxml")
        hyperlinks = [tag['href'] for tag in soup.find_all('a', href=True)]
        return hyperlinks

    def get_domain_hyperlinks(self, url):
        clean_links = []
        for link in set(self.get_hyperlinks(url)):
            clean_link = None
            if re.search(HTTP_URL_PATTERN, link):
                url_obj = urlparse(link)
                if url_obj.netloc == self.local_domain:
                    clean_link = link
            else:
                if link.startswith("/"):
                    clean_link = urljoin(self.base_url, link)
                elif link.startswith("#") or link.startswith("mailto:"):
                    continue
                else:
                    clean_link = urljoin(url, link)

            if clean_link:
                clean_link = clean_link.rstrip('/')

                if "postulez-en-ligne" not in clean_link:
                    clean_links.append(clean_link)

        return list(set(clean_links))

    def parse_sitemap(self, sitemap_url):
        try:
            response = self.session.get(sitemap_url, timeout=30)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'xml' not in content_type:
                crawler_logger.warning(f"Le sitemap n'est pas un fichier XML : {sitemap_url}")
                return []

            sitemap_urls = []
            tree = ET.fromstring(response.content)
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            for url in tree.findall('ns:url', namespaces=namespace):
                loc = url.find('ns:loc', namespaces=namespace)
                if loc is not None:
                    sitemap_url = loc.text.strip()
                    sitemap_urls.append(sitemap_url)
            return sitemap_urls
        except Exception as e:
            crawler_logger.error(f"Erreur lors du parsing du sitemap {sitemap_url}: {e}", exc_info=True)
            return []

    def find_sitemaps(self):
        robots_url = urljoin(self.base_url, '/robots.txt')
        try:
            response = self.session.get(robots_url, timeout=30)
            response.raise_for_status()
            sitemap_urls = []
            for line in response.text.splitlines():
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(sitemap_url)
            return sitemap_urls
        except requests.RequestException as e:
            crawler_logger.error(f"Erreur lors de la récupération de robots.txt : {e}", exc_info=True)
            return []

    def crawl_page(self, url):
        normalized_url = self.normalize_url(url)
        if self.db.is_visited(normalized_url):
            return
        self.db.mark_as_visited(normalized_url)

        crawler_logger.info(f"Crawling: {url}")

        try:
            response = self.session.get(url, timeout=30, allow_redirects=True)
            final_url = response.url

            if response.status_code == 404:
                crawler_logger.warning(f"Page non trouvée: {url}")
                return

            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type or final_url.lower().endswith('.pdf'):
                pdf_content = self.extract_text_from_pdf(response.content, final_url)
                if pdf_content.strip():
                    filename = self.sanitize_filename(unquote(final_url))
                    filepath = os.path.join(self.output_dir, f"{filename}.txt")
                    with open(filepath, "w", encoding='utf-8') as f:
                        f.write(pdf_content)
                    crawler_logger.info(f"PDF extrait: {final_url}")
                else:
                    crawler_logger.warning(f"Contenu PDF vide: {final_url}")
            elif 'text/html' in content_type:
                html_content = response.text
                text = self.extract_text_from_html(html_content, final_url)
                if not text.strip():
                    text = self.extract_text_alternative(html_content, final_url)
                if text.strip():
                    filename = self.sanitize_filename(unquote(final_url))
                    filepath = os.path.join(self.output_dir, f"{filename}.txt")
                    with open(filepath, "w", encoding='utf-8') as f:
                        f.write(text)
                    crawler_logger.info(f"HTML extrait: {final_url}")
                else:
                    crawler_logger.warning(f"Contenu HTML vide: {final_url}")

                new_links = self.get_domain_hyperlinks(final_url)
                for link in new_links:
                    normalized_link = self.normalize_url(link)
                    if not self.db.is_visited(normalized_link):
                        self.queue.put(link)
            else:
                crawler_logger.warning(f"Type de contenu non supporté: {content_type} pour l'URL {final_url}")

        except requests.RequestException as e:
            crawler_logger.error(f"Erreur lors du crawling {url}: {e}", exc_info=True)

    def crawl(self):
        # Ajouter les URLs du sitemap à la queue
        sitemap_urls = self.find_sitemaps()
        for sitemap_url in sitemap_urls:
            crawler_logger.info(f"Parsing sitemap: {sitemap_url}")
            urls = self.parse_sitemap(sitemap_url)
            for url in urls:
                normalized_url = self.normalize_url(url)
                if not self.db.is_visited(normalized_url):
                    self.queue.put(url)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            while not self.queue.empty() or futures:
                while not self.queue.empty() and len(futures) < self.max_workers:
                    url = self.queue.get()
                    futures.append(executor.submit(self.crawl_page, url))

                # Traiter les futures terminées
                for future in as_completed(futures):
                    futures.remove(future)
                    try:
                        future.result()  # Pour lever les exceptions si elles se produisent
                    except Exception as e:
                        crawler_logger.error(f"Exception dans le futur: {e}", exc_info=True)

                time.sleep(1)  # Pause pour éviter de surcharger le serveur

        self.db.close()

if __name__ == "__main__":
    # Définir l'URL de départ ici
    START_URL = "https://www.ouellet.com/fr-ca/"  # Remplacez par l'URL de départ souhaitée

    # Définir le nombre de threads (workers)
    MAX_WORKERS = 10  # Ajustez selon vos besoins

    # Lancer le crawler
    crawler = Crawler(start_url=START_URL, max_workers=MAX_WORKERS)
    crawler.crawl()
    crawler_logger.info("Crawling and text extraction completed.")
    print("Crawling and text extraction completed.")
