thonfrom __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .utils_time import now_utc, to_iso

DEFAULT_BASE_URL = "https://www.quora.com"

@dataclass
class QuoraAnswerRecord:
    index: int
    qid: Optional[int]
    id: str
    url: str
    title: str
    creationTime: str
    answerCount: int
    answers: str
    numUpvotes: Optional[int]
    numViews: Optional[int]
    profileUrl: Optional[str]
    names: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class QuoraScraper:
    """
    High-level scraper that can handle both question URLs and search result URLs.

    It tries to be resilient to layout changes by using heuristic selectors and
    falling back to best-effort extraction rather than failing hard.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None) -> None:
        self.config = config or {}
        self.logger = logger or logging.getLogger("quora_scraper")
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        headers = {
            "User-Agent": self.config.get(
                "user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36",
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        session.headers.update(headers)

        cookies = self.config.get("cookies") or {}
        if isinstance(cookies, dict):
            session.cookies.update(cookies)

        proxies = self.config.get("proxies")
        if isinstance(proxies, dict) and proxies:
            session.proxies.update(proxies)

        timeout = self.config.get("timeout_seconds")
        self.timeout = float(timeout) if timeout is not None else 20.0

        return session

    # ---------------------------
    # Public interface
    # ---------------------------

    def scrape_url(self, url: str, limit_per_search: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Decide if the URL is a search URL or a question URL and route accordingly.
        """
        if self._is_search_url(url):
            return [r.to_dict() for r in self._scrape_search(url, limit_per_search)]
        return [r.to_dict() for r in self._scrape_question(url)]

    # ---------------------------
    # URL helpers
    # ---------------------------

    def _is_search_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if "search" in parsed.path:
            return True
        if "q=" in (parsed.query or ""):
            return True
        return False

    def _normalize_url(self, url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme:
            return urljoin(DEFAULT_BASE_URL, url)
        return url

    # ---------------------------
    # HTTP helpers
    # ---------------------------

    def _get(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            self.logger.error("Request failed for %s: %s", url, exc)
            return None

    # ---------------------------
    # Scraping logic
    # ---------------------------

    def _scrape_search(self, url: str, limit_per_search: Optional[int]) -> List[QuoraAnswerRecord]:
        """
        Scrape a Quora search page:
        - collect question URLs
        - then scrape each question page.
        """
        self.logger.debug("Scraping search page: %s", url)
        html = self._get(url)
        if html is None:
            return []

        question_urls = self._parse_search_results_page(html)
        if limit_per_search is not None:
            question_urls = question_urls[: max(limit_per_search, 0)]

        records: List[QuoraAnswerRecord] = []
        for q_index, q_url in enumerate(question_urls, start=1):
            self.logger.debug("Scraping question %d from search: %s", q_index, q_url)
            records.extend(self._scrape_question(q_url))

        return records

    def _scrape_question(self, url: str) -> List[QuoraAnswerRecord]:
        """
        Scrape a single Quora question URL.
        """
        normalized_url = self._normalize_url(url)
        self.logger.debug("Scraping question page: %s", normalized_url)
        html = self._get(normalized_url)
        if html is None:
            return []

        return self._parse_question_page(html, normalized_url)

    # ---------------------------
    # HTML parsing
    # ---------------------------

    def _parse_search_results_page(self, html: str) -> List[str]:
        """
        Extract question URLs from a Quora search results page.

        This function uses simple heuristics and may not be perfect but
        aims to be robust to moderate layout changes.
        """
        soup = BeautifulSoup(html, "lxml")
        links = soup.find_all("a", href=True)

        question_urls: List[str] = []
        for a in links:
            href = a["href"]

            # Ignore external links
            if href.startswith("http") and "quora.com" not in href:
                continue

            # Heuristic: question URLs often look like /What-is-... or contain "/question/"
            if "/profile/" in href:
                continue
            if "answer/" in href:
                continue

            if "/What-" in href or "/How-" in href or "/Why-" in href or "/Is-" in href or "/Can-" in href:
                full_url = self._normalize_url(href)
                if full_url not in question_urls:
                    question_urls.append(full_url)
            elif "/question/" in href:
                full_url = self._normalize_url(href)
                if full_url not in question_urls:
                    question_urls.append(full_url)

        self.logger.info("Found %d candidate question URLs in search results.", len(question_urls))
        return question_urls

    def _parse_question_page(self, html: str, url: str) -> List[QuoraAnswerRecord]:
        """
        Parse a Quora question page into one or more QuoraAnswerRecord objects.

        Because Quora is highly dynamic, this uses multiple strategies and
        falls back to conservative defaults when necessary.
        """
        soup = BeautifulSoup(html, "lxml")

        title = self._extract_title(soup)
        qid = self._extract_qid(soup, url)
        answers_blocks = self._extract_answer_blocks(soup)

        records: List[QuoraAnswerRecord] = []
        creation_time_iso = to_iso(now_utc())  # Best effort if we can't find a real one

        for index, answer in enumerate(answers_blocks, start=1):
            answer_text = answer.get("text", "").strip()
            upvotes = answer.get("upvotes")
            views = answer.get("views")
            profile_url = answer.get("profile_url")
            author_name = answer.get("author_name")

            record = QuoraAnswerRecord(
                index=index,
                qid=qid,
                id=self._build_encoded_id(qid, index),
                url=url,
                title=title,
                creationTime=creation_time_iso,
                answerCount=len(answers_blocks),
                answers=answer_text,
                numUpvotes=upvotes,
                numViews=views,
                profileUrl=profile_url,
                names=[{"givenName": author_name or "", "familyName": ""}],
            )
            records.append(record)

        # If we somehow didn't find any answer blocks, create a single record with the page text
        if not records:
            self.logger.debug("No answers found, falling back to whole-page text.")
            page_text = soup.get_text(" ", strip=True)
            record = QuoraAnswerRecord(
                index=1,
                qid=qid,
                id=self._build_encoded_id(qid, 1),
                url=url,
                title=title,
                creationTime=creation_time_iso,
                answerCount=0,
                answers=page_text[:5000],
                numUpvotes=None,
                numViews=None,
                profileUrl=None,
                names=[{"givenName": "", "familyName": ""}],
            )
            records.append(record)

        return records

    # ---------------------------
    # Field extraction helpers
    # ---------------------------

    def _extract_title(self, soup: BeautifulSoup) -> str:
        # Try Open Graph title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"].strip()

        # Fallback to <title>
        if soup.title and soup.title.string:
            return soup.title.string.strip()

        # Last resort
        heading = soup.find("h1")
        if heading and heading.get_text(strip=True):
            return heading.get_text(strip=True)

        return "Quora Question"

    def _extract_qid(self, soup: BeautifulSoup, url: str) -> Optional[int]:
        # Some pages may expose a question ID in meta tags or data attributes
        meta_qid = soup.find("meta", {"name": "qid"}) or soup.find("meta", {"property": "qid"})
        if meta_qid and meta_qid.get("content"):
            try:
                return int(meta_qid["content"])
            except ValueError:
                pass

        # Try data-qid on some container
        qid_holder = soup.find(attrs={"data-qid": True})
        if qid_holder:
            try:
                return int(qid_holder["data-qid"])
            except (KeyError, ValueError, TypeError):
                pass

        # Fallback: derive a pseudo-ID from URL hash
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        # Take first 12 hex chars as integer
        try:
            return int(digest[:12], 16)
        except ValueError:
            return None

    def _extract_answer_blocks(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Try to identify answer blocks and basic metrics.

        Returns list of dicts with keys: text, upvotes, views, profile_url, author_name.
        """
        answers: List[Dict[str, Any]] = []

        # Heuristic 1: look for components with 'data-testid' that contains 'answer'
        for container in soup.find_all(attrs={"data-testid": True}):
            testid = container.get("data-testid", "")
            if "answer" in testid.lower():
                text = container.get_text(" ", strip=True)
                if text:
                    answers.append(self._build_answer_dict(container, text))

        # Heuristic 2: look for typical answer article/card containers
        if not answers:
            for container in soup.find_all("div"):
                class_attr = " ".join(container.get("class", [])).lower()
                if "answer" in class_attr and len(container.get_text(strip=True)) > 50:
                    text = container.get_text(" ", strip=True)
                    answers.append(self._build_answer_dict(container, text))

        # Deduplicate by text
        deduped: List[Dict[str, Any]] = []
        seen_texts = set()
        for a in answers:
            key = a.get("text", "")
            if key in seen_texts:
                continue
            seen_texts.add(key)
            deduped.append(a)

        return deduped

    def _build_answer_dict(self, container: Any, text: str) -> Dict[str, Any]:
        upvotes = self._extract_upvotes(container)
        views = self._extract_views(container)
        profile_url, author_name = self._extract_author(container)

        return {
            "text": text,
            "upvotes": upvotes,
            "views": views,
            "profile_url": profile_url,
            "author_name": author_name,
        }

    def _extract_upvotes(self, container: Any) -> Optional[int]:
        # Look for text like "123 upvotes" or "1.2k upvotes"
        text = container.get_text(" ", strip=True)
        lowered = text.lower()
        if "upvote" not in lowered:
            return None

        # Very simple numeric scanning
        tokens = lowered.split()
        for i, tok in enumerate(tokens):
            if "upvote" in tok:
                # Try token immediately before, e.g. "123 upvotes"
                if i > 0:
                    maybe = tokens[i - 1]
                    parsed = self._parse_compact_number(maybe)
                    if parsed is not None:
                        return parsed
        return None

    def _extract_views(self, container: Any) -> Optional[int]:
        text = container.get_text(" ", strip=True)
        lowered = text.lower()
        if "view" not in lowered:
            return None

        tokens = lowered.split()
        for i, tok in enumerate(tokens):
            if "view" in tok:
                if i > 0:
                    maybe = tokens[i - 1]
                    parsed = self._parse_compact_number(maybe)
                    if parsed is not None:
                        return parsed
        return None

    def _extract_author(self, container: Any) -> (Optional[str], Optional[str]):
        # Look for a profile link
        link = container.find("a", href=True)
        best_link = None
        if link and "/profile/" in link["href"]:
            best_link = link
        else:
            for a in container.find_all("a", href=True):
                if "/profile/" in a["href"]:
                    best_link = a
                    break

        if not best_link:
            return None, None

        href = best_link["href"]
        full_url = self._normalize_url(href)
        name = best_link.get_text(strip=True)
        return full_url, name or None

    def _parse_compact_number(self, token: str) -> Optional[int]:
        """
        Parse numbers like '1.2k', '3m', '452'.
        """
        token = token.strip().replace(",", "")
        if not token:
            return None

        multiplier = 1
        last = token[-1].lower()
        if last == "k":
            multiplier = 1_000
            token = token[:-1]
        elif last == "m":
            multiplier = 1_000_000
            token = token[:-1]

        try:
            value = float(token)
            return int(value * multiplier)
        except ValueError:
            return None

    def _build_encoded_id(self, qid: Optional[int], answer_index: int) -> str:
        """
        Build a stable-ish encoded ID similar in shape to Quora internal IDs.
        """
        base = f"Question@{qid or 0}:{answer_index}"
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
        # Roughly emulate a base64-ish ID without depending on external libs
        return digest[:32]