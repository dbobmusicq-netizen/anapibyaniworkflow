import os
import re
import sys
import time
import base64
import logging
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser as date_parser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from supabase import create_client, Client

# Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AnimeEngine")

# Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
PROXY_URL = os.environ.get("PROXY_URL")

# Auto-clean SUPABASE_URL to prevent PGRST125 malformed endpoint paths
if SUPABASE_URL:
    SUPABASE_URL = SUPABASE_URL.split("/rest/v1")[0].rstrip("/")

# Backfill controls mapped from GitHub Action inputs
HISTORICAL_MODE = os.environ.get("HISTORICAL_MODE", "false").lower() == "true"
MAX_PAGES = int(os.environ.get("MAX_PAGES", "10"))
MIN_SEEDERS = int(os.environ.get("MIN_SEEDERS", "10"))

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.critical("Database environment variables (SUPABASE_URL / SUPABASE_KEY) are missing.")
    sys.exit(1)

# Initialize Supabase Client
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    logger.critical(f"Supabase client initialization failed: {e}")
    sys.exit(1)


class ScraperException(Exception):
    """Base exception class for scraper operations."""
    pass


class NetworkException(ScraperException):
    """Raised when request pools encounter network failures."""
    pass


class ParsingException(ScraperException):
    """Raised when index parsing fails due to schema mutations."""
    pass


class Normalizer:
    """Helper class to parse and standardize complex metadata formats."""

    @staticmethod
    def info_hash(raw_hash):
        if not raw_hash:
            return None
        h_str = raw_hash.strip().lower()

        if (len(h_str) == 40 and re.match(r'^[0-9a-f]{40}$', h_str)) or \
           (len(h_str) == 64 and re.match(r'^[0-9a-f]{64}$', h_str)):
            return h_str

        if len(h_str) == 32 and re.match(r'^[a-z2-7]{32}$', h_str):
            try:
                missing_padding = len(h_str) % 8
                padded = h_str
                if missing_padding:
                    padded += '=' * (8 - missing_padding)
                decoded = base64.b32decode(padded.upper().encode('ascii'))
                return decoded.hex().lower()
            except Exception as e:
                logger.debug(f"Failed to normalize Base32 hash '{raw_hash}': {e}")
        
        return h_str

    @staticmethod
    def size_to_bytes(size_str):
        if not size_str:
            return None
        size_str = size_str.strip().lower()
        match = re.match(r'^([\d\.]+)\s*(gb|mb|kb|gib|mib|kib|b)?$', size_str)
        if not match:
            return None
        val = float(match.group(1))
        unit = match.group(2)
        if not unit:
            return int(val)
        if unit in ('gb', 'gib'):
            return int(val * 1024 * 1024 * 1024)
        if unit in ('mb', 'mib'):
            return int(val * 1024 * 1024)
        if unit in ('kb', 'kib'):
            return int(val * 1024)
        return int(val)

    @staticmethod
    def is_fresh_release(pub_date_str):
        """Checks if a torrent was published within the last 24 hours."""
        if not pub_date_str:
            return True  # Save by default if age cannot be verified
        try:
            pub_date = date_parser.parse(pub_date_str)
            now = datetime.now(timezone.utc)
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            delta = now - pub_date
            return delta.total_seconds() < 86400  # 24 hours
        except Exception:
            return True

    @staticmethod
    def title_metadata(title):
        group_match = re.match(r'^\[(.*?)\]', title)
        group = group_match.group(1).strip() if group_match else "Unknown"
        
        cleaned_title = title
        if group_match:
            cleaned_title = cleaned_title[group_match.end():].strip()
            
        res_match = re.search(r'\b(2160p|1080p|720p|480p|360p|4k)\b', title, re.IGNORECASE)
        resolution = res_match.group(1).lower() if res_match else "Unknown"
        
        audio_match = re.search(r'\b(FLAC|AAC|MP3|OPUS|DTS|DD\+?5\.1|AC3)\b', title, re.IGNORECASE)
        audio = audio_match.group(1).lower() if audio_match else "Unknown"

        audio_channels = "2.0"
        if re.search(r'\b(5\.1|6ch|surround|multichannel)\b', title, re.IGNORECASE):
            audio_channels = "5.1"

        codec_match = re.search(r'\b(x265|x264|h264|h265|hevc|av1)\b', title, re.IGNORECASE)
        codec = codec_match.group(1).lower() if codec_match else "Unknown"
        
        content_type = "episode"
        if re.search(r'\b(movie|film|theatrical|劇場版)\b', title, re.IGNORECASE):
            content_type = "movie"
        elif re.search(r'\b(batch|complete|01\s*-\s*\d+|pack|season\s*\d+|s\d+\s*-\s*s\d+)\b', title, re.IGNORECASE):
            content_type = "batch"

        subs = "English"
        if re.search(r'\b(multi-sub|multisubs|multi|esp|ger|fre)\b', title, re.IGNORECASE):
            subs = "Multi-Sub"
        elif re.search(r'\b(raw|unsubbed)\b', title, re.IGNORECASE):
            subs = "Raw"
        
        episode = "Unknown"
        ep_patterns = [
            r'\sS(\d+)E(\d+)\b',
            r'\s-\s(\d+(?:\.\d+)?)\b',
            r'\b(?:ep|episode)\s*(\d+)\b',
            r'\b(?<!S\d{2}E)(\d{2,3})\b'
        ]
        for pattern in ep_patterns:
            match = re.search(pattern, cleaned_title, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    episode = f"S{match.group(1)}E{match.group(2)}"
                else:
                    episode = match.group(1)
                break
                
        return {
            "release_group": group,
            "resolution": resolution,
            "codec": codec,
            "audio": audio,
            "audio_channels": audio_channels,
            "episode": episode,
            "content_type": content_type,
            "subtitles": subs
        }


class BaseEngine:
    def __init__(self, source_name, proxy_pool=None):
        self.source_name = source_name
        self.session = requests.Session()
        self.proxy_pool = proxy_pool or []
        self.current_proxy_idx = 0
        
        retries = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=12, pool_maxsize=12)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })

    def fetch(self, url, timeout=15):
        # Prioritize custom user proxy if defined
        if PROXY_URL:
            try:
                res = self.session.get(url, proxies={"http": PROXY_URL, "https": PROXY_URL}, timeout=timeout)
                res.raise_for_status()
                return res
            except Exception as e:
                raise NetworkException(f"Custom user proxy failed: {e}")

        # Fallback to rotating through parsed public proxies
        attempts = max(3, len(self.proxy_pool)) if self.proxy_pool else 1
        for attempt in range(attempts):
            current_proxies = None
            if self.proxy_pool:
                p = self.proxy_pool[self.current_proxy_idx % len(self.proxy_pool)]
                current_proxies = {"http": f"http://{p}", "https": f"http://{p}"}
                self.current_proxy_idx += 1
            
            try:
                res = self.session.get(url, proxies=current_proxies, timeout=timeout)
                res.raise_for_status()
                return res
            except Exception as e:
                if not self.proxy_pool:
                    raise NetworkException(f"Direct connection failed: {e}")
                logger.debug(f"[{self.source_name}] Proxy failed. Trying next node... Error: {e}")
        
        # Last-resort fallback to direct connection
        try:
            logger.debug(f"[{self.source_name}] Proxy pool exhausted. Attempting direct fallback connection...")
            res = self.session.get(url, timeout=timeout)
            res.raise_for_status()
            return res
        except Exception as e:
            raise NetworkException(f"Rotating proxy pool exhausted and direct connection failed: {e}")

    @staticmethod
    def extract_hash_from_magnet(magnet_uri):
        if not magnet_uri:
            return None
        match = re.search(r'urn:btih:([a-zA-Z0-9]+)', magnet_uri)
        if match:
            return Normalizer.info_hash(match.group(1))
        return None

    @staticmethod
    def find_magnet_in_xml_item(item_element):
        for child in item_element.iter():
            text = child.text or ""
            if "magnet:?" in text:
                return text.strip()
            for attr in child.attrib.values():
                if "magnet:?" in attr:
                    return attr.strip()
        return None


class NyaaEngine(BaseEngine):
    def __init__(self, proxy_pool=None):
        super().__init__("nyaa", proxy_pool)

    def scrape(self):
        res = self.fetch("https://nyaa.si/?page=rss&c=1_2")
        return self.parse_xml(res.content)

    def parse_xml(self, content):
        torrents = []
        try:
            root = ET.fromstring(content)
            for item in root.findall('.//item'):
                title = item.find('title').text
                
                info_hash = None
                hash_el = item.find('{https://nyaa.si/xmlns/nyaa}infoHash')
                if hash_el is not None:
                    info_hash = Normalizer.info_hash(hash_el.text)

                magnet = self.find_magnet_in_xml_item(item)
                if not magnet and info_hash:
                    magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(title)}"
                elif magnet and not info_hash:
                    info_hash = self.extract_hash_from_magnet(magnet)

                if not info_hash or not magnet:
                    continue

                size_str = None
                size_el = item.find('{https://nyaa.si/xmlns/nyaa}size')
                if size_el is not None:
                    size_str = size_el.text

                seeders = int(item.find('{https://nyaa.si/xmlns/nyaa}seeders').text or 0) if item.find('{https://nyaa.si/xmlns/nyaa}seeders') is not None else 0
                leechers = int(item.find('{https://nyaa.si/xmlns/nyaa}leechers').text or 0) if item.find('{https://nyaa.si/xmlns/nyaa}leechers') is not None else 0
                completed = int(item.find('{https://nyaa.si/xmlns/nyaa}downloads').text or 0) if item.find('{https://nyaa.si/xmlns/nyaa}downloads') is not None else 0
                
                pub_date_str = item.find('pubDate').text
                published_at = date_parser.parse(pub_date_str).isoformat()
                
                # Filter out old files with low seed counts
                if not Normalizer.is_fresh_release(pub_date_str) and seeders < MIN_SEEDERS:
                    continue

                meta = Normalizer.title_metadata(title)

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": Normalizer.size_to_bytes(size_str),
                    "size_text": size_str,
                    "resolution": meta["resolution"],
                    "release_group": meta["release_group"],
                    "codec": meta["codec"],
                    "audio": meta["audio"],
                    "episode": meta["episode"],
                    "source_site": "nyaa",
                    "torrent_url": item.find('link').text if item.find('link') is not None else None,
                    "seeders": seeders,
                    "leechers": leechers,
                    "completed": completed,
                    "published_at": published_at,
                    "metadata": {
                        "category": item.find('{https://nyaa.si/xmlns/nyaa}category').text if item.find('{https://nyaa.si/xmlns/nyaa}category') is not None else "Unknown",
                        "content_type": meta["content_type"],
                        "audio_channels": meta["audio_channels"],
                        "subtitles": meta["subtitles"]
                    }
                })
        except Exception as e:
            raise ParsingException(f"XML Parsing failed inside Nyaa: {e}")
        return torrents


class NyaaHistoricalEngine(NyaaEngine):
    def __init__(self, max_pages=10, min_seeders=10, proxy_pool=None):
        super().__init__(proxy_pool)
        self.source_name = "nyaa_historical"
        self.max_pages = max_pages
        self.min_seeders = min_seeders

    def scrape(self):
        logger.info(f"Starting historical backfill (Pages: 1 to {self.max_pages}, Min Seeders: {self.min_seeders})")
        historical_torrents = []

        for page in range(1, self.max_pages + 1):
            logger.info(f"Scraping Nyaa page {page} of {self.max_pages} (Sorted by Seeders DESC)...")
            url = f"https://nyaa.si/?page=rss&c=1_2&s=seeders&o=desc&p={page}"
            
            try:
                res = self.fetch(url)
                if not res or not res.content.strip():
                    break

                page_results = self.parse_xml(res.content)
                if not page_results:
                    break

                first_seeds = page_results[0].get("seeders", 0)
                last_seeds = page_results[-1].get("seeders", 0)
                logger.info(f"Page {page} complete. Seeder bounds: {first_seeds} -> {last_seeds}")

                # Save if seeders are above target limit
                filtered_results = [t for t in page_results if t.get("seeders", 0) >= self.min_seeders]
                historical_torrents.extend(filtered_results)

                if last_seeds < self.min_seeders:
                    logger.info(f"Seeders dropped below target limit ({self.min_seeders}). Halting.")
                    break

                time.sleep(2.0)

            except Exception as e:
                logger.error(f"Error occurred during historical backfill execution on page {page}: {e}")
                break

        return historical_torrents


class AnimeToshoEngine(BaseEngine):
    def __init__(self, proxy_pool=None):
        super().__init__("animetosho", proxy_pool)

    def scrape(self):
        res = self.fetch("https://feed.animetosho.org/json")
        torrents = []
        try:
            items = res.json()
            for item in items:
                title = item.get("title")
                magnet = item.get("magnet_uri")
                info_hash = Normalizer.info_hash(item.get("info_hash"))

                if not info_hash and magnet:
                    info_hash = self.extract_hash_from_magnet(magnet)
                if not info_hash or not magnet:
                    continue

                meta = Normalizer.title_metadata(title)
                published_at = datetime.fromtimestamp(item.get("timestamp")).isoformat() if item.get("timestamp") else None

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": item.get("total_size"),
                    "size_text": f"{round(item.get('total_size', 0) / (1024*1024), 2)} MiB" if item.get("total_size") else None,
                    "resolution": meta["resolution"],
                    "release_group": meta["release_group"],
                    "codec": meta["codec"],
                    "audio": meta["audio"],
                    "episode": meta["episode"],
                    "source_site": "animetosho",
                    "torrent_url": item.get("torrent_url"),
                    "published_at": published_at,
                    "metadata": {
                        "nyaa_id": item.get("nyaa_id"),
                        "tosho_id": item.get("tosho_id"),
                        "anidex_id": item.get("anidex_id"),
                        "anidb_aid": item.get("anidb_aid"),
                        "content_type": meta["content_type"],
                        "audio_channels": meta["audio_channels"],
                        "subtitles": meta["subtitles"]
                    }
                })
        except Exception as e:
            raise ParsingException(f"JSON parsing failed inside AnimeTosho: {e}")
        return torrents


class TokyoToshokanEngine(BaseEngine):
    def __init__(self, proxy_pool=None):
        super().__init__("tokyotosho", proxy_pool)

    def scrape(self):
        res = self.fetch("https://www.tokyotosho.info/rss.php")
        torrents = []
        try:
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                title = item.find('title').text
                magnet = self.find_magnet_in_xml_item(item)
                if not magnet:
                    continue

                info_hash = self.extract_hash_from_magnet(magnet)
                if not info_hash:
                    continue

                meta = Normalizer.title_metadata(title)
                pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else None
                published_at = date_parser.parse(pub_date_str).isoformat() if pub_date_str else None

                desc_text = item.find('description').text or ""
                size_match = re.search(r'Size:\s*([\d\.]+\s*[GKM]?B)', desc_text, re.IGNORECASE)
                size_str = size_match.group(1) if size_match else None

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": Normalizer.size_to_bytes(size_str),
                    "size_text": size_str,
                    "resolution": meta["resolution"],
                    "release_group": meta["release_group"],
                    "codec": meta["codec"],
                    "audio": meta["audio"],
                    "episode": meta["episode"],
                    "source_site": "tokyotosho",
                    "torrent_url": item.find('link').text if item.find('link') is not None else None,
                    "published_at": published_at,
                    "metadata": {
                        "content_type": meta["content_type"],
                        "audio_channels": meta["audio_channels"],
                        "subtitles": meta["subtitles"]
                    }
                })
        except Exception as e:
            raise ParsingException(f"XML parsing failed inside TokyoToshokan: {e}")
        return torrents


class SubsPleaseEngine(BaseEngine):
    def __init__(self, proxy_pool=None):
        super().__init__("subsplease", proxy_pool)

    def scrape(self):
        logger.info("Parsing SubsPlease RSS feed...")
        response = self.fetch("https://subsplease.org/rss/?r=1080p")
        
        if not response or not response.content.strip():
            logger.warning("SubsPlease 1080p feed returned empty. Falling back to main RSS feed...")
            response = self.fetch("https://subsplease.org/rss/")
            
        if not response or not response.content.strip():
            logger.error("SubsPlease main feed returned empty. Skipping this run.")
            return []

        torrents = []
        try:
            root = ET.fromstring(response.content)
            for item in root.findall('.//item'):
                title = item.find('title').text
                magnet = self.find_magnet_in_xml_item(item)
                if not magnet:
                    continue

                info_hash = self.extract_hash_from_magnet(magnet)
                if not info_hash:
                    continue

                meta = Normalizer.title_metadata(title)
                pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else None
                published_at = date_parser.parse(pub_date_str).isoformat() if pub_date_str else None

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": None,
                    "size_text": None,
                    "resolution": meta["resolution"] if meta["resolution"] != "Unknown" else "1080p",
                    "release_group": "SubsPlease",
                    "codec": meta["codec"],
                    "audio": meta["audio"],
                    "episode": meta["episode"],
                    "source_site": "subsplease",
                    "torrent_url": item.find('link').text if item.find('link') is not None else None,
                    "published_at": published_at,
                    "metadata": {
                        "content_type": meta["content_type"],
                        "audio_channels": meta["audio_channels"],
                        "subtitles": meta["subtitles"]
                    }
                })
        except Exception as e:
            raise ParsingException(f"XML parsing failed inside SubsPlease: {e}")
        return torrents


class EraiRawsEngine(BaseEngine):
    def __init__(self, proxy_pool=None):
        super().__init__("erai-raws", proxy_pool)

    def scrape(self):
        try:
            logger.info("Attempting direct RSS connection with Erai-Raws...")
            res = self.fetch("https://www.erai-raws.info/rss-page/")
            return self._parse_xml(res.content, source_tag="direct")
        except Exception as e:
            logger.warning(f"Erai-Raws direct scraper failed ({e}). Falling back to Nyaa filter...")
            return self.scrape_via_nyaa_fallback()

    def _parse_xml(self, xml_content, source_tag):
        torrents = []
        root = ET.fromstring(xml_content)
        for item in root.findall('.//item'):
            title = item.find('title').text
            magnet = self.find_magnet_in_xml_item(item)
            if not magnet:
                continue

            info_hash = self.extract_hash_from_magnet(magnet)
            if not info_hash:
                continue

            meta = Normalizer.title_metadata(title)
            pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else None
            published_at = date_parser.parse(pub_date_str).isoformat() if pub_date_str else None

            torrents.append({
                "title": title,
                "magnet": magnet,
                "info_hash": info_hash,
                "size_bytes": None,
                "size_text": None,
                "resolution": meta["resolution"],
                "release_group": "Erai-raws",
                "codec": meta["codec"],
                "audio": meta["audio"],
                "episode": meta["episode"],
                "source_site": "erai-raws",
                "torrent_url": item.find('link').text if item.find('link') is not None else None,
                "published_at": published_at,
                "metadata": {
                    "source_route": source_tag,
                    "content_type": meta["content_type"],
                    "audio_channels": meta["audio_channels"],
                    "subtitles": meta["subtitles"]
                }
            })
        return torrents

    def scrape_via_nyaa_fallback(self):
        res = self.fetch("https://nyaa.si/?page=rss&q=Erai-raws")
        torrents = []
        try:
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                title = item.find('title').text
                if "[Erai-raws]" not in title:
                    continue

                magnet = self.find_magnet_in_xml_item(item)
                info_hash = None
                hash_el = item.find('{https://nyaa.si/xmlns/nyaa}infoHash')
                if hash_el is not None:
                    info_hash = Normalizer.info_hash(hash_el.text)

                if not magnet and info_hash:
                    magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(title)}"
                elif magnet and not info_hash:
                    info_hash = self.extract_hash_from_magnet(magnet)

                if not info_hash or not magnet:
                    continue

                meta = Normalizer.title_metadata(title)
                pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else None
                published_at = date_parser.parse(pub_date_str).isoformat() if pub_date_str else None

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": None,
                    "size_text": None,
                    "resolution": meta["resolution"],
                    "release_group": "Erai-raws",
                    "codec": meta["codec"],
                    "audio": meta["audio"],
                    "episode": meta["episode"],
                    "source_site": "erai-raws",
                    "torrent_url": item.find('link').text if item.find('link') is not None else None,
                    "published_at": published_at,
                    "metadata": {
                        "source_route": "nyaa_fallback",
                        "content_type": meta["content_type"],
                        "audio_channels": meta["audio_channels"],
                        "subtitles": meta["subtitles"]
                    }
                })
        except Exception as e:
            raise ParsingException(f"Erai-Raws Nyaa fallback parse failed: {e}")
        return torrents


def fetch_public_proxies():
    """Fetches plain-text public HTTP proxies to bypass server limits."""
    logger.info("Loading public proxy list for scraper pool...")
    url = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            proxies = [p.strip() for p in res.text.split("\n") if p.strip()]
            logger.info(f"Loaded {len(proxies)} rotating proxies.")
            return proxies
    except Exception as e:
        logger.warning(f"Could not load public proxies: {e}. Executing with direct connections.")
    return []


def persist_to_supabase_resilient(data_payload):
    if not data_payload:
        logger.warning("No data retrieved to synchronize.")
        return

    dedup = {}
    for entry in data_payload:
        ih = entry["info_hash"]
        if ih not in dedup:
            dedup[ih] = entry
        else:
            if dedup[ih]["size_bytes"] is None and entry["size_bytes"] is not None:
                dedup[ih] = entry

    unique_list = list(dedup.values())
    logger.info(f"Writing {len(unique_list)} validated records to Supabase...")

    chunk_size = 100
    for idx in range(0, len(unique_list), chunk_size):
        chunk = unique_list[idx:idx + chunk_size]
        
        success = False
        attempts = 3
        delay = 2.0
        
        for attempt in range(attempts):
            try:
                supabase.table("anime_torrents").upsert(
                    chunk,
                    on_conflict="info_hash"
                ).execute()
                success = True
                logger.info(f"Database sync chunk completed: [{idx // chunk_size + 1}]")
                break
            except Exception as e:
                logger.warning(f"Database batch transaction attempt {attempt + 1} failed: {e}")
                if attempt < attempts - 1:
                    time.sleep(delay)
                    delay *= 2.0
                else:
                    logger.error("Starting transactional fallback handler...")

        if not success:
            for item in chunk:
                try:
                    supabase.table("anime_torrents").upsert(item, on_conflict="info_hash").execute()
                except Exception as ex:
                    logger.error(f"Failed transaction entry: '{item['title'][:45]}...' | Error details: {ex}")


def main():
    start_time = time.time()
    
    # Retrieve dynamic rolling proxies
    proxy_pool = fetch_public_proxies() if not PROXY_URL else []

    # Assemble engines
    if HISTORICAL_MODE:
        logger.info(f"DUAL-MODE TRIGGER: Starting historical backfill indexing...")
        engines = [NyaaHistoricalEngine(max_pages=MAX_PAGES, min_seeders=MIN_SEEDERS, proxy_pool=proxy_pool)]
    else:
        logger.info("DUAL-MODE TRIGGER: Starting automatic daily stream...")
        # Running both standard real-time feed extraction + 5-page historical update run on every execution!
        engines = [
            NyaaEngine(proxy_pool=proxy_pool),
            AnimeToshoEngine(proxy_pool=proxy_pool),
            TokyoToshokanEngine(proxy_pool=proxy_pool),
            SubsPleaseEngine(proxy_pool=proxy_pool),
            EraiRawsEngine(proxy_pool=proxy_pool),
            NyaaHistoricalEngine(max_pages=5, min_seeders=MIN_SEEDERS, proxy_pool=proxy_pool) # Sweep 5 most popular pages to refresh peers
        ]

    execution_results = []
    stats = {}

    with ThreadPoolExecutor(max_workers=len(engines), thread_name_prefix="ScraperPool") as executor:
        future_to_engine = {executor.submit(engine.scrape): engine for engine in engines}
        
        for future in as_completed(future_to_engine):
            engine = future_to_engine[future]
            try:
                data = future.result()
                execution_results.extend(data)
                stats[engine.source_name] = {"status": "SUCCESS", "records": len(data)}
                logger.info(f"Engine [{engine.source_name}] finished. Harvested {len(data)} items.")
            except Exception as e:
                stats[engine.source_name] = {"status": "FAILED", "error": str(e), "records": 0}
                logger.error(f"Engine [{engine.source_name}] crashed: {e}")

    try:
        persist_to_supabase_resilient(execution_results)
    except Exception as e:
        logger.critical(f"Critical Database pipeline failure: {e}")

    total_duration = round(time.time() - start_time, 2)
    logger.info("==============================================")
    logger.info("           PIPELINE EXECUTION SUMMARY         ")
    logger.info("==============================================")
    logger.info(f"Total processing time: {total_duration}s")
    for key, val in stats.items():
        if val["status"] == "SUCCESS":
            logger.info(f" -> Index: {key:<14} | Status: {val['status']:<8} | Collected: {val['records']}")
        else:
            logger.info(f" -> Index: {key:<14} | Status: {val['status']:<8} | Error: {val['error'][:40]}")
    logger.info("==============================================")


if __name__ == "__main__":
    main()
