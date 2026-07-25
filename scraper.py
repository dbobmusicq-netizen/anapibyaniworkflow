import os
import re
import sys
import time
import base64
import logging
import urllib.parse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
from dateutil import parser as date_parser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from supabase import create_client, Client

# Configure Structured Enterprise Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AnimeEngine")

# Credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
PROXY_URL = os.environ.get("PROXY_URL")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.critical("Database environment configurations (SUPABASE_URL/SUPABASE_KEY) are missing.")
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
    """Exception raised when an index provider request fails repeatedly."""
    pass


class ParsingException(ScraperException):
    """Exception raised when an index provider's payload structure changes."""
    pass


class DatabaseException(ScraperException):
    """Exception raised when database transactions fail to process."""
    pass


class Normalizer:
    """Utility class to parse and standardize complex metadata formats."""

    @staticmethod
    def info_hash(raw_hash):
        """
        Normalizes any valid torrent info-hash representation to lowercase Hex.
        Handles:
          - 40-character Hex (v1)
          - 32-character Base32 (converts to 40-character Hex)
          - 64-character Hex (v2 SHA-256)
        """
        if not raw_hash:
            return None
        h_str = raw_hash.strip().lower()

        # Already standard 40-char hex or 64-char hex v2
        if (len(h_str) == 40 and re.match(r'^[0-9a-f]{40}$', h_str)) or \
           (len(h_str) == 64 and re.match(r'^[0-9a-f]{64}$', h_str)):
            return h_str

        # Convert Base32 (32 chars) to standard hex representation
        if len(h_str) == 32 and re.match(r'^[a-z2-7]{32}$', h_str):
            try:
                # Pad to 8-byte boundary if needed
                missing_padding = len(h_str) % 8
                padded = h_str
                if missing_padding:
                    padded += '=' * (8 - missing_padding)
                decoded = base64.b32decode(padded.upper().encode('ascii'))
                return decoded.hex().lower()
            except Exception as e:
                logger.debug(f"Failed to decode Base32 info-hash '{raw_hash}': {e}")
        
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
    def title_metadata(title):
        """Analyzes and tokenizes title configurations."""
        # Release Group
        group_match = re.match(r'^\[(.*?)\]', title)
        group = group_match.group(1).strip() if group_match else "Unknown"
        
        cleaned_title = title
        if group_match:
            cleaned_title = cleaned_title[group_match.end():].strip()
            
        # Resolution/Quality
        res_match = re.search(r'\b(2160p|1080p|720p|480p|360p|4k)\b', title, re.IGNORECASE)
        resolution = res_match.group(1).lower() if res_match else "Unknown"
        
        # Audio Codec
        audio_match = re.search(r'\b(FLAC|AAC|MP3|OPUS|DTS|DD\+?5\.1|AC3)\b', title, re.IGNORECASE)
        audio = audio_match.group(1).lower() if audio_match else "Unknown"

        # Audio Channels
        audio_channels = "2.0"
        if re.search(r'\b(5\.1|6ch|surround|multichannel)\b', title, re.IGNORECASE):
            audio_channels = "5.1"

        # Video Codec
        codec_match = re.search(r'\b(x265|x264|h264|h265|hevc|av1)\b', title, re.IGNORECASE)
        codec = codec_match.group(1).lower() if codec_match else "Unknown"
        
        # Content Classification Type (Episode vs Movie vs Batch Season Pack)
        content_type = "episode"
        if re.search(r'\b(movie|film|theatrical|劇場版)\b', title, re.IGNORECASE):
            content_type = "movie"
        elif re.search(r'\b(batch|complete|01\s*-\s*\d+|pack|season\s*\d+|s\d+\s*-\s*s\d+)\b', title, re.IGNORECASE):
            content_type = "batch"

        # Subtitle properties
        subs = "English"
        if re.search(r'\b(multi-sub|multisubs|multi|esp|ger|fre)\b', title, re.IGNORECASE):
            subs = "Multi-Sub"
        elif re.search(r'\b(raw|unsubbed)\b', title, re.IGNORECASE):
            subs = "Raw"
        
        # Episode extraction
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
    """Base core for resilient fetching session initialization."""
    def __init__(self, source_name):
        self.source_name = source_name
        self.session = requests.Session()
        
        # Configure exponential backoff on HTTP layer
        retries = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })

        if PROXY_URL:
            self.session.proxies = {"http": PROXY_URL, "https": PROXY_URL}

    def fetch(self, url):
        try:
            res = self.session.get(url, timeout=25)
            res.raise_for_status()
            return res
        except Exception as e:
            raise NetworkException(f"Network processing failed for [{self.source_name}] at URL {url}: {e}")

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
    def __init__(self):
        super().__init__("nyaa")

    def scrape(self):
        # Category filter '1_2' narrows down items to English-translated Anime
        res = self.fetch("https://nyaa.si/?page=rss&c=1_2")
        torrents = []
        try:
            root = ET.fromstring(res.content)
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
            raise ParsingException(f"XML parsing failed inside Nyaa feed parser: {e}")
        return torrents


class AnimeToshoEngine(BaseEngine):
    def __init__(self):
        super().__init__("animetosho")

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
            raise ParsingException(f"JSON mapping failed inside AnimeTosho feed parser: {e}")
        return torrents


class TokyoToshokanEngine(BaseEngine):
    def __init__(self):
        super().__init__("tokyotosho")

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
            raise ParsingException(f"XML parsing failed inside TokyoToshokan feed parser: {e}")
        return torrents


class SubsPleaseEngine(BaseEngine):
    def __init__(self):
        super().__init__("subsplease")

    def scrape(self):
        res = self.fetch("https://subsplease.org/rss/?r=1080p")
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

                torrents.append({
                    "title": title,
                    "magnet": magnet,
                    "info_hash": info_hash,
                    "size_bytes": None,
                    "size_text": None,
                    "resolution": "1080p",
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
            raise ParsingException(f"XML parsing failed inside SubsPlease feed parser: {e}")
        return torrents


class EraiRawsEngine(BaseEngine):
    def __init__(self):
        super().__init__("erai-raws")

    def scrape(self):
        try:
            # Erai Raws official domain is often behind strict anti-bot mitigations.
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
        # Query Nyaa specifically for verified Erai-raws team releases
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
            raise ParsingException(f"XML execution on Erai-Raws Nyaa fallback failed: {e}")
        return torrents


def persist_to_supabase_resilient(data_payload):
    """
    Handles batch database operations with exponential backoff on exceptions.
    Fallbacks to itemized writes if the batch structure hits verification limits.
    """
    if not data_payload:
        logger.warning("No fresh data retrieved to commit to database.")
        return

    # Locally de-duplicate before writing to database to prevent unnecessary transaction size
    dedup_registry = {}
    for entry in data_payload:
        ih = entry["info_hash"]
        if ih not in dedup_registry:
            dedup_registry[ih] = entry
        else:
            # Prefer structural records containing defined size metrics
            if dedup_registry[ih]["size_bytes"] is None and entry["size_bytes"] is not None:
                dedup_registry[ih] = entry

    unique_list = list(dedup_registry.values())
    logger.info(f"Writing {len(unique_list)} validated records to Supabase...")

    chunk_size = 100
    for idx in range(0, len(unique_list), chunk_size):
        chunk = unique_list[idx:idx + chunk_size]
        
        # Exponential backoff write retry logic
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
                logger.info(f"Successfully processed database chunk [{idx // chunk_size + 1}]")
                break
            except Exception as e:
                logger.warning(f"Database batch transaction attempt {attempt + 1} failed: {e}")
                if attempt < attempts - 1:
                    time.sleep(delay)
                    delay *= 2.0  # double the delay time
                else:
                    logger.error("Database batch operations failed. Starting row-by-row fallback handler...")

        if not success:
            # Itemized fallback to isolate corrupt/malformed items
            for item in chunk:
                try:
                    supabase.table("anime_torrents").upsert(item, on_conflict="info_hash").execute()
                except Exception as ex:
                    logger.error(f"Failed transaction entry: '{item['title'][:45]}...' | Error details: {ex}")


def main():
    start_time = time.time()
    logger.info("Initializing multi-index scraping run...")

    engines = [
        NyaaEngine(),
        AnimeToshoEngine(),
        TokyoToshokanEngine(),
        SubsPleaseEngine(),
        EraiRawsEngine()
    ]

    execution_results = []
    stats = {}

    # Thread Pool Concurrency Pattern
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

    # Run Database Synchronization
    try:
        persist_to_supabase_resilient(execution_results)
    except Exception as e:
        logger.critical(f"Critical Database pipeline failure: {e}")

    # Build Pipeline Health Summary
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
