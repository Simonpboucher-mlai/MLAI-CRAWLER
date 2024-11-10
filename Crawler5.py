# Part 1: Imports and Helper Classes

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
from typing import Set, List, Optional, Dict, Tuple
import json
import mimetypes
from pathlib import Path
import PyPDF2
from pdf2image import convert_from_path
import pytesseract
import fitz  # PyMuPDF
import io
import shutil
from PIL import Image
import tempfile

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
