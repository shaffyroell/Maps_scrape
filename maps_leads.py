import argparse
import csv
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
API_HOST = "local-business-data.p.rapidapi.com"

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# Keywords used to score/prioritise internal links for crawling
PAGE_PRIORITY_KEYWORDS = [
    "about", "team", "staff", "leadership", "contact", "services",
    "who-we-are", "our-team", "meet", "people", "management",
    "what-we-do", "solutions", "offerings", "expertise",
]

# Title keywords for leadership extraction
LEADERSHIP_TITLES = [
    "ceo", "cfo", "coo", "cto", "cmo", "president", "founder", "co-founder",
    "owner", "director", "vp ", "vice president", "partner", "principal",
    "managing director", "head of", "chief ", "general manager",
]


# ---------------------------------------------------------------------------
# Stage 1 — Google Maps scraper
# ---------------------------------------------------------------------------

def scrape_google_maps(query: str, city: str, max_results: int) -> list[dict]:
    if not RAPIDAPI_KEY:
        raise ValueError("RAPIDAPI_KEY not found in .env file")

    full_query = f"{query} in {city}"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": API_HOST,
    }

    results = []
    seen_ids = set()
    offset = 0
    limit = 20

    print(f"\n[Stage 1] Scraping Google Maps: '{full_query}'")

    while len(results) < max_results:
        params = {
            "query": full_query,
            "limit": limit,
            "offset": offset,
            "extract_emails_and_contacts": "true",
            "language": "en",
            "region": "us",
        }

        resp = None
        for attempt in range(6):
            try:
                r = requests.get(
                    f"https://{API_HOST}/search",
                    headers=headers,
                    params=params,
                    timeout=15,
                )
                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"  Rate limited — waiting {wait}s...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                resp = r
                break
            except requests.exceptions.RequestException as e:
                if attempt == 5:
                    print(f"  API error after 6 attempts: {e}")
                    return results
                time.sleep(2 ** attempt)

        # If all attempts were rate-limited, stop gracefully
        if resp is None:
            print(f"  Stopped after rate limiting — saving {len(results)} results collected so far.")
            break

        data = resp.json().get("data", [])
        if not data:
            print(f"  No more results from API at offset {offset}.")
            break

        for biz in data:
            if len(results) >= max_results:
                break
            place_id = biz.get("place_id", "")
            if place_id in seen_ids:
                continue
            seen_ids.add(place_id)

            email = ""
            contacts = biz.get("emails_and_contacts", {}) or {}
            emails = contacts.get("emails", [])
            if emails:
                email = emails[0]

            results.append({
                "place_id": place_id,
                "name": biz.get("name", ""),
                "phone_number": biz.get("phone_number", ""),
                "full_address": biz.get("full_address", ""),
                "city": biz.get("city", ""),
                "state": biz.get("state", ""),
                "zipcode": biz.get("zipcode", ""),
                "rating": biz.get("rating", ""),
                "review_count": biz.get("review_count", 0),
                "type": biz.get("type", ""),
                "website": biz.get("website", ""),
                "latitude": biz.get("latitude", ""),
                "longitude": biz.get("longitude", ""),
                "email": email,
            })

        count = len(results)
        if count % 20 == 0 or count == max_results:
            print(f"  {count}/{max_results} businesses scraped...")

        offset += limit
        time.sleep(1)

    print(f"[Stage 1] Done — {len(results)} businesses found\n")
    return results


# ---------------------------------------------------------------------------
# Stage 2 — Deep website scraper
# ---------------------------------------------------------------------------

def _fetch_page(url: str, timeout: int = 8) -> tuple[str, str]:
    """Fetch a URL. Returns (html, final_url). Empty string on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text, resp.url
    except Exception:
        pass
    return "", url


def _clean_text(soup: BeautifulSoup) -> str:
    """Strip nav/header/footer/scripts and return clean body text."""
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "iframe", "form"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    # Collapse whitespace
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _score_link(href: str) -> int:
    """Score an internal link URL by relevance to business intelligence."""
    href_lower = href.lower()
    score = 0
    for kw in PAGE_PRIORITY_KEYWORDS:
        if kw in href_lower:
            score += 2
    # Penalise clearly irrelevant pages
    skip = ["blog", "news", "press", "faq", "privacy", "terms",
            "cookie", "cart", "shop", "login", "register", "wp-"]
    for s in skip:
        if s in href_lower:
            score -= 3
    return score


def _discover_pages(homepage_html: str, base_url: str, max_pages: int = 4) -> list[str]:
    """Return up to max_pages internal URLs scored by relevance."""
    soup = BeautifulSoup(homepage_html, "html.parser")
    base_domain = urlparse(base_url).netloc

    seen = set()
    scored = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(base_url, href)
        parsed = urlparse(full)

        # Must be same domain, http/https, not an anchor-only or file link
        if (parsed.netloc != base_domain
                or parsed.scheme not in ("http", "https")
                or full in seen
                or full == base_url
                or any(full.endswith(ext) for ext in
                       [".pdf", ".jpg", ".png", ".gif", ".zip", ".docx"])):
            continue

        seen.add(full)
        score = _score_link(parsed.path + parsed.query)
        if score > 0:
            scored.append((score, full))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [url for _, url in scored[:max_pages]]


def _extract_emails(text: str) -> list[str]:
    found = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    filtered = [e for e in found if not any(
        x in e.lower() for x in ["example.com", "sentry", "w3.org", "schema",
                                  "wixpress", "@2x", "domain.com"]
    )]
    return list(dict.fromkeys(filtered))  # deduplicate, preserve order


def _extract_phones(text: str) -> list[str]:
    found = re.findall(
        r"(?:\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}", text
    )
    return list(dict.fromkeys(f.strip() for f in found if len(re.sub(r"\D", "", f)) == 10))


def _extract_leadership(text: str, soup: BeautifulSoup) -> str:
    """
    Two-pass extraction:
    1. Regex: Name followed closely by a title keyword in plain text
    2. Structured HTML: elements whose class/id suggest team/bio cards
    """
    people = []
    seen_names = set()

    # Pass 1 — regex on plain text (most reliable)
    # Matches: "John Smith, CEO" / "Jane Doe — Founder" / "Mike Jones | Owner"
    title_pattern = (
        r"(CEO|CFO|CTO|COO|CMO|President|Founder|Co-Founder|Owner|"
        r"Director|Vice President|VP|Partner|Principal|Managing Director|"
        r"General Manager|Head of [A-Za-z]+|Chief [A-Za-z]+ Officer)"
    )
    pattern = re.compile(
        r"([A-Z][a-z]{1,15} (?:[A-Z][a-z]{1,15} )?[A-Z][a-z]{1,20})"
        r"[\s,\-–|:]+(" + title_pattern[1:-1] + r")",
        re.MULTILINE,
    )
    # Common non-name words that start with capitals — filter these out
    NON_NAME_WORDS = {
        "book", "call", "contact", "click", "read", "learn", "find", "get",
        "view", "see", "sign", "visit", "request", "word", "note", "meet",
        "about", "home", "more", "our", "your", "the", "this", "that",
        "schedule", "follow", "watch", "listen", "join", "check",
    }

    def _is_real_name(candidate: str) -> bool:
        parts = candidate.split()
        if len(parts) < 2:
            return False
        # Each word must start with capital and contain only letters/hyphens
        for part in parts:
            if not re.match(r"^[A-Z][a-z\-]{1,20}$", part):
                return False
        # First word must not be a common verb/nav word
        if parts[0].lower() in NON_NAME_WORDS:
            return False
        return True

    for m in pattern.finditer(text):
        name = m.group(1).strip()
        title = m.group(2).strip()
        if _is_real_name(name) and name not in seen_names:
            people.append(f"{name} — {title}")
            seen_names.add(name)
        if len(people) >= 6:
            break

    # Also try reversed order: "Owner: John Smith" / "CEO John Smith"
    if len(people) < 3:
        rev_pattern = re.compile(
            r"(" + title_pattern[1:-1] + r")"
            r"[\s,\-–|:]+([A-Z][a-z]{1,15} (?:[A-Z][a-z]{1,15} )?[A-Z][a-z]{1,20})",
            re.MULTILINE,
        )
        for m in rev_pattern.finditer(text):
            title = m.group(1).strip()
            name = m.group(2).strip()
            if _is_real_name(name) and name not in seen_names:
                people.append(f"{name} — {title}")
                seen_names.add(name)
            if len(people) >= 6:
                break

    # Pass 2 — structured HTML: look for team card containers
    if len(people) < 2:
        for tag in soup.find_all(True, {"class": re.compile(
                r"team|staff|person|bio|member|leadership|people", re.I)}):
            tag_text = tag.get_text(" ", strip=True)
            # Should contain a title keyword and look like a person card
            if (any(t.lower() in tag_text.lower() for t in
                    ["CEO", "Founder", "Owner", "Director", "President", "Manager"])
                    and 10 < len(tag_text) < 200):
                # Clean and de-duplicate
                clean = re.sub(r"\s{2,}", " ", tag_text).strip()
                if clean not in people:
                    people.append(clean)
            if len(people) >= 6:
                break

    return " | ".join(people[:6])


def _extract_description(soup: BeautifulSoup, text: str) -> str:
    """Get meta description first, then fall back to first meaty paragraph."""
    meta = soup.find("meta", attrs={"name": re.compile("description", re.I)})
    if meta and meta.get("content"):
        desc = meta["content"].strip()
        if len(desc) > 40:
            return desc[:400]

    # Fallback: find first paragraph with >60 chars that isn't nav/cookie text
    for p in soup.find_all("p"):
        p_text = p.get_text(strip=True)
        if (len(p_text) > 60
                and not any(skip in p_text.lower()
                            for skip in ["cookie", "javascript", "browser",
                                         "©", "all rights reserved"])):
            return p_text[:400]
    return ""


def _extract_services(pages_html: dict[str, str]) -> str:
    """
    Two-pass service extraction:
    1. Find lists (<ul>/<ol>) adjacent to service/solution headings on any page
    2. On pages whose URL contains service keywords, extract all short <li> items
    """
    services = []
    seen = set()

    SERVICE_URL_KEYS = ["service", "solution", "offer", "what-we-do",
                        "speciali", "expertise", "capabilities"]
    HEADING_KEYS = ["service", "solution", "offer", "speciali",
                    "what we do", "expertise", "capabilities", "we provide"]

    for page_url, html in pages_html.items():
        soup = BeautifulSoup(html, "html.parser")
        url_is_service_page = any(k in page_url.lower() for k in SERVICE_URL_KEYS)

        # Pass 1 — lists next to matching headings
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            heading_text = heading.get_text(strip=True).lower()
            if any(kw in heading_text for kw in HEADING_KEYS):
                for sibling in heading.find_next_siblings(["ul", "ol", "div"], limit=3):
                    for li in sibling.find_all("li"):
                        item = re.sub(r"\s{2,}", " ", li.get_text(" ", strip=True))
                        if 3 < len(item) < 80 and item not in seen:
                            services.append(item)
                            seen.add(item)
                    if len(services) >= 20:
                        break

        # Pass 2 — if URL looks like a services page, grab all short <li> items
        if url_is_service_page:
            for li in soup.find_all("li"):
                item = re.sub(r"\s{2,}", " ", li.get_text(" ", strip=True))
                if 5 < len(item) < 60 and item not in seen:
                    # Skip nav-style items (single words or "Home", "About" etc.)
                    word_count = len(item.split())
                    if word_count >= 2:
                        services.append(item)
                        seen.add(item)
                if len(services) >= 20:
                    break

        if len(services) >= 20:
            break

    return ", ".join(services[:20])


def scrape_website(lead: dict) -> dict:
    """Crawl up to 5 pages of a business website and extract structured intel."""
    url = lead.get("website", "")

    empty = {
        "pages_scraped": 0,
        "site_description": "",
        "services": "",
        "site_emails": "",
        "site_phones": "",
        "leadership": "",
        "about_text": "",
        "scrape_status": "no_website",
    }

    if not url:
        return empty

    # --- Homepage ---
    homepage_html, final_url = _fetch_page(url)
    if not homepage_html:
        empty["scrape_status"] = "unreachable"
        return empty

    homepage_soup = BeautifulSoup(homepage_html, "html.parser")
    pages_html = {final_url: homepage_html}

    # --- Discover + fetch up to 4 more relevant pages ---
    additional_urls = _discover_pages(homepage_html, final_url, max_pages=4)
    for page_url in additional_urls:
        html, _ = _fetch_page(page_url)
        if html:
            pages_html[page_url] = html
        if len(pages_html) >= 5:
            break

    # --- Combine all text ---
    all_text = ""
    about_text = ""
    for page_url, html in pages_html.items():
        soup = BeautifulSoup(html, "html.parser")
        page_text = _clean_text(soup)
        all_text += f"\n\n--- {page_url} ---\n{page_text}"

        # Keep the richest "about" page text
        if any(kw in page_url.lower() for kw in ["about", "who-we-are", "team", "company"]):
            if len(page_text) > len(about_text):
                about_text = page_text[:1500]

    # --- Extract structured fields ---
    all_emails = _extract_emails(all_text)
    all_phones = _extract_phones(all_text)

    # Build a combined soup for leadership extraction
    combined_soup = BeautifulSoup(
        "".join(pages_html.values()), "html.parser"
    )

    return {
        "pages_scraped": len(pages_html),
        "site_description": _extract_description(homepage_soup, all_text),
        "services": _extract_services(pages_html),
        "site_emails": "; ".join(all_emails[:5]),
        "site_phones": "; ".join(all_phones[:5]),
        "leadership": _extract_leadership(all_text, combined_soup),
        "about_text": about_text or _clean_text(homepage_soup)[:1500],
        "scrape_status": "ok",
    }


def enrich_all_websites(leads: list[dict]) -> list[dict]:
    print(f"[Stage 2] Deep-scraping websites for {len(leads)} leads...")
    enriched_leads = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_lead = {executor.submit(scrape_website, lead): lead for lead in leads}
        completed = 0
        for future in as_completed(future_to_lead):
            lead = future_to_lead[future]
            enrichment = future.result()
            enriched_leads.append({**lead, **enrichment})
            completed += 1
            if completed % 10 == 0 or completed == len(leads):
                print(f"  {completed}/{len(leads)} sites scraped...")

    print(f"[Stage 2] Done\n")
    return enriched_leads


# ---------------------------------------------------------------------------
# Stage 3 — Merge site emails/phones into lead (deduplicate with Maps data)
# ---------------------------------------------------------------------------

def merge_contacts(lead: dict) -> dict:
    """Merge site-scraped emails/phones with those from Google Maps."""
    # Emails
    maps_email = lead.get("email", "")
    site_emails_raw = lead.get("site_emails", "")
    all_emails = []
    if maps_email:
        all_emails.append(maps_email)
    for e in site_emails_raw.split(";"):
        e = e.strip()
        if e and e not in all_emails:
            all_emails.append(e)
    lead["all_emails"] = "; ".join(all_emails[:5])

    # Phones
    maps_phone = lead.get("phone_number", "")
    site_phones_raw = lead.get("site_phones", "")
    all_phones = []
    if maps_phone:
        all_phones.append(maps_phone)
    for p in site_phones_raw.split(";"):
        p = p.strip()
        if p and p not in all_phones:
            all_phones.append(p)
    lead["all_phones"] = "; ".join(all_phones[:5])

    return lead


# ---------------------------------------------------------------------------
# CSV + output
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    # Google Maps data
    "place_id", "name", "full_address", "city", "state", "zipcode",
    "rating", "review_count", "type", "website", "latitude", "longitude",
    # Merged contact info
    "all_emails", "all_phones",
    # Website intelligence
    "pages_scraped", "scrape_status",
    "site_description", "services", "leadership", "about_text",
]


def save_csv(leads: list[dict], query: str, city: str) -> str:
    slug = lambda s: re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{slug(query)}_{slug(city)}_{date_str}.csv"
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)

    return filepath


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if not RAPIDAPI_KEY:
        print("\nERROR: No RAPIDAPI_KEY found.")
        print("Create a .env file in this folder with:")
        print("  RAPIDAPI_KEY=your_key_here\n")
        return

    parser = argparse.ArgumentParser(description="Scrape local business leads from Google Maps")
    parser.add_argument("--query", required=True, help='Business type, e.g. "dentist"')
    parser.add_argument("--city",  required=True, help='City and state, e.g. "Austin TX"')
    parser.add_argument("--max",   type=int, default=100, help="Max results (default: 100)")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"  Local Business Lead Scraper")
    print(f"  Query: {args.query} | City: {args.city} | Max: {args.max}")
    print(f"{'='*50}\n")

    # Stage 1 — Google Maps
    leads = scrape_google_maps(args.query, args.city, args.max)
    if not leads:
        print("No leads found. Check your query and API key.")
        return

    # Stage 2 — Deep website scrape
    enriched = enrich_all_websites(leads)

    # Stage 3 — Merge contacts
    print("[Stage 3] Merging contact data...")
    for lead in enriched:
        merge_contacts(lead)
    print("[Stage 3] Done\n")

    # Save
    filepath = save_csv(enriched, args.query, args.city)
    print(f"{'='*50}")
    print(f"  Saved: {os.path.basename(filepath)}")
    print(f"  Total leads: {len(enriched)}")
    print(f"  Path: {filepath}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
