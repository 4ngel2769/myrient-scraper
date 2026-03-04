"""
scraper.py — Fetches and parses Apache/Caddy directory listings from myrient.erista.me
"""

from __future__ import annotations

import re
import asyncio
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, unquote, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://myrient.erista.me/files/"

# Size unit multipliers (IEC binary prefixes used by the site: KiB, MiB, GiB, TiB)
_SIZE_UNITS: dict[str, int] = {
    "b":   1,
    "kib": 1024,
    "mib": 1024 ** 2,
    "gib": 1024 ** 3,
    "tib": 1024 ** 4,
    "pib": 1024 ** 5,
    # SI aliases sometimes seen
    "kb":  1000,
    "mb":  1000 ** 2,
    "gb":  1000 ** 3,
    "tb":  1000 ** 4,
    "k":   1024,
    "m":   1024 ** 2,
    "g":   1024 ** 3,
    "t":   1024 ** 4,
}

_SIZE_RE = re.compile(
    r"^\s*([\d,]+(?:\.\d+)?)\s*([KMGTP]i?[Bb]?|[Bb])\s*$", re.IGNORECASE
)
_DATE_RE = re.compile(
    r"\d{2}-\w{3}-\d{4}\s+\d{2}:\d{2}"   # 04-Jan-2023 09:01
    r"|\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}"  # 2023-01-04 09:01
)


@dataclass
class Entry:
    name: str
    url: str
    size_bytes: Optional[int]   # None for directories
    size_str: str               # raw size string as shown on site
    date: str
    is_dir: bool

    @property
    def display_name(self) -> str:
        return self.name.rstrip("/")

    @property
    def display_size(self) -> str:
        if self.is_dir:
            return "[DIR]"
        if self.size_bytes is None:
            return self.size_str or "?"
        return format_size(self.size_bytes)


def parse_size_str(s: str) -> Optional[int]:
    """Convert a size string like '35.9 KiB' or '1.0 MiB' or '471 B' to bytes."""
    s = s.strip()
    if not s or s in ("-", "–", "—"):
        return None
    m = _SIZE_RE.match(s)
    if not m:
        # Try bare integer (bytes)
        try:
            return int(s.replace(",", ""))
        except ValueError:
            return None
    value_str = m.group(1).replace(",", "")
    unit = m.group(2).lower().rstrip("b").rstrip("i") if m.group(2) else ""
    # Normalise unit spelling
    full_unit = m.group(2).lower()
    multiplier = _SIZE_UNITS.get(full_unit) or _SIZE_UNITS.get(unit, 1)
    try:
        return int(float(value_str) * multiplier)
    except ValueError:
        return None


def format_size(size: Optional[int]) -> str:
    """Format a byte count into a human-readable string."""
    if size is None:
        return "?"
    if size == 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    v = float(size)
    for unit in units:
        if v < 1024.0:
            if unit == "B":
                return f"{int(v)} B"
            return f"{v:.1f} {unit}"
        v /= 1024.0
    return f"{v:.1f} PiB"


def _cells_from_row(tr: Tag) -> list[str]:
    """Return stripped text content of each <td> in a <tr>."""
    return [td.get_text(strip=True) for td in tr.find_all("td")]


def _classify_cells(cells: list[str]) -> tuple[str, str]:
    """Given a list of cell texts (excluding the name cell), return (size_str, date_str)."""
    size_str = "-"
    date_str = ""
    for c in cells:
        c = c.strip()
        if not c or c in ("-", "–", "—"):
            continue
        if _DATE_RE.match(c):
            date_str = c
        elif _SIZE_RE.match(c) or re.match(r"^\d+$", c):
            size_str = c
    return size_str, date_str


def _parse_html(html: str, base_url: str) -> list[Entry]:
    """
    Parse the HTML of a directory listing page and return a list of Entry objects.
    Handles Apache autoindex, Caddy, Nginx, custom templates.
    """
    soup = BeautifulSoup(html, "lxml")
    entries: list[Entry] = []
    seen_urls: set[str] = set()

    # Strategy 1: look for <tr> rows containing an <a> with href
    rows = soup.find_all("tr")
    if rows:
        for tr in rows:
            a_tag = tr.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag["href"]
            # Skip parent/self dirs (./, ../, ?sort=, etc.)
            if href.startswith("?") or href in ("../", "./", "/"):
                continue
            name = a_tag.get_text(strip=True)
            if not name or name in ("Parent Directory", ".", ".."):
                continue

            url = urljoin(base_url, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            is_dir = href.endswith("/")
            cells = _cells_from_row(tr)
            # Remove the cell that contains the name
            name_cell_text = name
            other_cells = [c for c in cells if name_cell_text not in c or not c.strip()]
            # Fallback: just use all cells that aren't the name
            all_non_name = [c for c in cells if c != name_cell_text]
            size_str, date_str = _classify_cells(all_non_name)

            size_bytes = None if is_dir else parse_size_str(size_str)

            entries.append(Entry(
                name=name,
                url=url,
                size_bytes=size_bytes,
                size_str=size_str,
                date=date_str,
                is_dir=is_dir,
            ))
        if entries:
            return entries

    # Strategy 2: <pre> formatted listing (some nginx versions)
    pre = soup.find("pre")
    if pre:
        for a_tag in pre.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("?") or href in ("../", "./", "/"):
                continue
            name = a_tag.get_text(strip=True)
            if not name or name in ("Parent Directory", ".", ".."):
                continue
            url = urljoin(base_url, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # The text after the link in a <pre> block typically has date and size
            next_sibling = a_tag.next_sibling
            tail = str(next_sibling) if next_sibling else ""
            is_dir = href.endswith("/")
            size_str, date_str = _classify_cells(tail.split())
            size_bytes = None if is_dir else parse_size_str(size_str)
            entries.append(Entry(
                name=name,
                url=url,
                size_bytes=size_bytes,
                size_str=size_str,
                date=date_str,
                is_dir=is_dir,
            ))
        if entries:
            return entries

    # Strategy 3: any <a> on the page with a relative href that isn't nav
    nav_hrefs = {a.get("href", "") for a in soup.select("nav a, header a, footer a")}
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href in nav_hrefs:
            continue
        if href.startswith(("http", "//", "?", "#", "mailto:")):
            continue
        if href in ("../", "./", "/"):
            continue
        name = a_tag.get_text(strip=True)
        if not name or name in ("Parent Directory", ".", ".."):
            continue
        url = urljoin(base_url, href)
        parsed = urlparse(url)
        # Must be on the same host and under /files/
        if "myrient" not in parsed.netloc:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        is_dir = href.endswith("/")
        entries.append(Entry(
            name=name,
            url=url,
            size_bytes=None,
            size_str="-",
            date="",
            is_dir=is_dir,
        ))

    return entries


# ── Async fetching with caching ─────────────────────────────────────────────

_cache: dict[str, list[Entry]] = {}
_cache_lock = asyncio.Lock()

_CLIENT: Optional[httpx.AsyncClient] = None

def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
            headers={"User-Agent": "MyrientArchiver/1.0 (preservation scraper)"},
        )
    return _CLIENT


async def fetch_directory(url: str, force_refresh: bool = False) -> list[Entry]:
    """
    Fetch and parse a directory listing.  Results are cached in memory.
    """
    async with _cache_lock:
        if not force_refresh and url in _cache:
            return _cache[url]

    client = _get_client()
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"HTTP {e.response.status_code} fetching {url}") from e
    except httpx.RequestError as e:
        raise RuntimeError(f"Network error fetching {url}: {e}") from e

    entries = _parse_html(response.text, url)
    # Sort: directories first, then files, both alphabetically
    entries.sort(key=lambda e: (not e.is_dir, e.name.lower()))

    async with _cache_lock:
        _cache[url] = entries

    return entries


async def calculate_dir_size(
    url: str,
    progress_callback=None,
    _depth: int = 0,
) -> int:
    """
    Recursively calculate the total byte size of a directory.
    progress_callback(url, bytes_found) is called for each directory fetched.
    """
    total = 0
    try:
        entries = await fetch_directory(url)
    except RuntimeError:
        return 0

    tasks = []
    for entry in entries:
        if entry.is_dir:
            tasks.append(calculate_dir_size(entry.url, progress_callback, _depth + 1))
        elif entry.size_bytes is not None:
            total += entry.size_bytes

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, int):
                total += r

    if progress_callback:
        await progress_callback(url, total)

    return total


async def collect_files(url: str) -> list["Entry"]:
    """
    Recursively walk a directory URL and return every *file* Entry found.
    Directories themselves are not included in the result.
    """
    result: list[Entry] = []
    try:
        entries = await fetch_directory(url)
    except RuntimeError:
        return result

    tasks = []
    for entry in entries:
        if entry.is_dir:
            tasks.append(collect_files(entry.url))
        else:
            result.append(entry)

    if tasks:
        sub_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in sub_results:
            if isinstance(r, list):
                result.extend(r)

    return result


async def download_entry(
    entry: "Entry",
    dest_root: str,
    progress_callback=None,
) -> bool:
    """
    Stream-download a single file entry to dest_root, mirroring the remote
    directory structure.  Skips the file if it already exists with the correct
    size.

    progress_callback(done_bytes: int, total_bytes: int) is called synchronously
    per 64 KiB chunk while downloading.

    Returns True if the file was skipped (already present), False if downloaded.
    Raises on HTTP / IO errors.
    """
    from pathlib import Path
    from urllib.parse import urlparse

    assert not entry.is_dir, "download_entry() only handles files"

    parsed = urlparse(entry.url)
    rel = parsed.path.lstrip("/")
    # Keep the full remote path but root it under "myrient/" so the layout is:
    #   downloads/myrient/files/<collection>/<subfolders>/<file>
    rel = "myrient/" + rel

    local_path = Path(dest_root) / rel
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip if already fully downloaded
    if (
        local_path.exists()
        and entry.size_bytes is not None
        and local_path.stat().st_size == entry.size_bytes
    ):
        return True  # skipped

    client = _get_client()
    async with client.stream("GET", entry.url, timeout=None) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", entry.size_bytes or 0))
        done = 0
        with open(local_path, "wb") as fh:
            async for chunk in resp.aiter_bytes(65_536):
                fh.write(chunk)
                done += len(chunk)
                if progress_callback:
                    progress_callback(done, total)

    return False  # downloaded


async def close():
    """Close the shared HTTP client."""
    global _CLIENT
    if _CLIENT and not _CLIENT.is_closed:
        await _CLIENT.aclose()
        _CLIENT = None
