"""
Chainalysis Daily Media Monitor — Daily & Weekly Media Intelligence

Three-prong media monitoring:
  1. Direct Mentions — articles referencing Chainalysis (Google News + RSS keyword)
  2. Narrative Relevance — articles covering 19 policy domains
  3. Competitor Watch — Elliptic, TRM Labs, CipherTrace, Arkham, Nansen, Crystal

Outputs:
  - Structured JSON with article links, AI narratives, trend tracking
  - Email digest (daily operational or weekly strategic briefing)
  - Slack digest posted to configured webhook

Environment variables:
  - SLACK_WEBHOOK_URL_DAILY: Slack webhook for the daily operational channel
  - SLACK_WEBHOOK_URL_WEEKLY: Slack webhook for the weekly leadership channel
  - SLACK_WEBHOOK_URL: Fallback Slack webhook (used if mode-specific var is not set)
  - SLACK_WEBHOOK_URL_APAC: Slack webhook for APAC DM delivery
  - WORKFLOW_SLUG: This workflow's slug (for cross-run trend tracking)
  - GITHUB_PAGES_REPO: GitHub repo for weekly briefing archive (e.g. jawane1/weekly-briefing-mock)
"""

from skills_core.workflow import chainalysis_workflow
from skills_core.proxy import create_session
from skills_core import context
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_step,
)
import json
import os

# ── RSS Feed Registry ────────────────────────────────────────────────
RSS_FEEDS = {
    "coindesk": {"name": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "format": "rss", "tier": "crypto_native"},
    "cointelegraph": {"name": "CoinTelegraph", "url": "https://cointelegraph.com/rss", "format": "rss", "tier": "crypto_native"},
    "the_block": {"name": "The Block", "url": "https://www.theblock.co/rss.xml", "format": "rss", "tier": "crypto_native"},
    "decrypt": {"name": "Decrypt", "url": "https://decrypt.co/feed", "format": "rss", "tier": "crypto_native"},
    "dl_news": {"name": "DL News", "url": "https://www.dlnews.com/arc/outboundfeeds/rss/", "format": "rss", "tier": "crypto_native"},
    "protos": {"name": "Protos", "url": "https://protos.com/feed/", "format": "rss", "tier": "crypto_native"},
    "unchained": {"name": "Unchained", "url": "https://unchainedcrypto.com/feed/", "format": "rss", "tier": "crypto_native"},
    "beincrypto": {"name": "BeInCrypto", "url": "https://beincrypto.com/feed/", "format": "rss", "tier": "crypto_native"},
    "bitcoin_magazine": {"name": "Bitcoin Magazine", "url": "https://bitcoinmagazine.com/feed", "format": "rss", "tier": "crypto_native"},
    "crypto_briefing": {"name": "Crypto Briefing", "url": "https://cryptobriefing.com/feed/", "format": "rss", "tier": "crypto_native"},
    "the_defiant": {"name": "The Defiant", "url": "https://thedefiant.io/feed", "format": "rss", "tier": "crypto_native"},
    "cryptoslate": {"name": "CryptoSlate", "url": "https://cryptoslate.com/feed/", "format": "rss", "tier": "crypto_native"},
    "cryptonews": {"name": "CryptoNews", "url": "https://cryptonews.com/news/feed/", "format": "rss", "tier": "crypto_native"},
    "techcrunch_fintech": {"name": "TechCrunch Fintech", "url": "https://techcrunch.com/category/fintech/feed/", "format": "rss", "tier": "mainstream"},
    "finextra": {"name": "Finextra", "url": "https://www.finextra.com/rss/headlines.aspx", "format": "rss", "tier": "mainstream"},
    "crowdfund_insider": {"name": "Crowdfund Insider", "url": "https://www.crowdfundinsider.com/feed/", "format": "rss", "tier": "mainstream"},
    "compliance_week": {"name": "Compliance Week", "url": "https://www.complianceweek.com/rss", "format": "rss", "tier": "mainstream"},
    # ── Mainstream / traditional news ────────────────────────────
    "bbc_business": {"name": "BBC Business", "url": "https://feeds.bbci.co.uk/news/business/rss.xml", "format": "rss", "tier": "mainstream"},
    "bbc_technology": {"name": "BBC Technology", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml", "format": "rss", "tier": "mainstream"},
    "cnbc_finance": {"name": "CNBC Finance", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "format": "rss", "tier": "mainstream"},
    "cnbc_world": {"name": "CNBC World", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362", "format": "rss", "tier": "mainstream"},
    "cnbc_tech": {"name": "CNBC Technology", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910", "format": "rss", "tier": "mainstream"},
    "ft": {"name": "Financial Times", "url": "https://www.ft.com/rss/home", "format": "rss", "tier": "mainstream"},
    "guardian_business": {"name": "The Guardian", "url": "https://www.theguardian.com/uk/business/rss", "format": "rss", "tier": "mainstream"},
    "yahoo_finance": {"name": "Yahoo Finance", "url": "https://finance.yahoo.com/news/rssindex", "format": "rss", "tier": "mainstream"},
    "pymnts": {"name": "PYMNTS", "url": "https://www.pymnts.com/feed/", "format": "rss", "tier": "mainstream"},
    "nyt_tech": {"name": "New York Times Tech", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "format": "rss", "tier": "mainstream"},
    "wired_security": {"name": "Wired Security", "url": "https://www.wired.com/feed/category/security/latest/rss", "format": "rss", "tier": "mainstream"},
    "axios": {"name": "Axios", "url": "https://api.axios.com/feed/", "format": "rss", "tier": "mainstream"},
    "chainalysis_blog": {"name": "Chainalysis Blog", "url": "https://www.chainalysis.com/blog/feed/", "format": "rss", "tier": "chainalysis"},
}

# ── Google News Proxy Feeds ────────────────────────────────────────
# Publications whose native RSS feeds are dead/stale — pulled via Google News.
GOOGLE_NEWS_PROXY_FEEDS = {
    "blockworks_gn": {"name": "Blockworks", "url": "https://news.google.com/rss/search?q=%22blockworks%22+crypto+blockchain&hl=en-US&gl=US&ceid=US:en", "tier": "crypto_native"},
    "wsj_gn": {"name": "Wall Street Journal", "url": "https://news.google.com/rss/search?q=%22wall+street+journal%22+cryptocurrency+OR+blockchain+OR+%22digital+assets%22&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "ledger_insights_gn": {"name": "Ledger Insights", "url": "https://news.google.com/rss/search?q=site:ledgerinsights.com&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "forbes_crypto_gn": {"name": "Forbes Crypto", "url": "https://news.google.com/rss/search?q=%22cryptocurrency%22+OR+%22blockchain%22+site:forbes.com&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "fortune_crypto_gn": {"name": "Fortune Crypto", "url": "https://news.google.com/rss/search?q=%22crypto%22+OR+%22blockchain%22+site:fortune.com&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "reuters_gn": {"name": "Reuters", "url": "https://news.google.com/rss/search?q=site:reuters.com+%22crypto%22+OR+%22blockchain%22+OR+%22digital+assets%22+OR+%22stablecoin%22+OR+%22sanctions%22&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "wapo_gn": {"name": "Washington Post", "url": "https://news.google.com/rss/search?q=site:washingtonpost.com+%22cryptocurrency%22+OR+%22blockchain%22+OR+%22digital+assets%22+OR+%22crypto%22&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
    "bloomberg_gn": {"name": "Bloomberg", "url": "https://news.google.com/rss/search?q=site:bloomberg.com+%22crypto%22+OR+%22blockchain%22+OR+%22digital+assets%22+OR+%22stablecoin%22+OR+%22sanctions%22&hl=en-US&gl=US&ceid=US:en", "tier": "mainstream"},
}

GOOGLE_NEWS_SEARCHES = {
    "chainalysis": {
        "name": "Chainalysis (All Media)",
        "url": "https://news.google.com/rss/search?q=chainalysis&hl=en-US&gl=US&ceid=US:en",
        "category": "chainalysis_mention",
    },
    "chainalysis_broad": {
        "name": "Chainalysis (Broad)",
        "url": "https://news.google.com/rss/search?q=%22chainalysis%22+blockchain&hl=en-US&gl=US&ceid=US:en",
        "category": "chainalysis_mention",
    },
    "bloomberg_crypto": {
        "name": "Bloomberg Crypto",
        "url": "https://news.google.com/rss/search?q=%22blockchain%22+OR+%22cryptocurrency%22+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
        "category": "narrative",
    },
    "reuters_crypto": {
        "name": "Reuters Crypto",
        "url": "https://news.google.com/rss/search?q=%22blockchain%22+OR+%22cryptocurrency%22+site:reuters.com&hl=en-US&gl=US&ceid=US:en",
        "category": "narrative",
    },
    "doj_crypto": {
        "name": "DOJ Crypto Enforcement",
        "url": "https://news.google.com/rss/search?q=%22department+of+justice%22+cryptocurrency+OR+%22crypto%22+enforcement&hl=en-US&gl=US&ceid=US:en",
        "category": "narrative",
    },
}

COMPETITOR_SEARCHES = {
    "elliptic": {"name": "Elliptic", "url": "https://news.google.com/rss/search?q=%22elliptic%22+crypto&hl=en-US&gl=US&ceid=US:en"},
    "trm_labs": {"name": "TRM Labs", "url": "https://news.google.com/rss/search?q=%22TRM+Labs%22&hl=en-US&gl=US&ceid=US:en"},
    "ciphertrace": {"name": "CipherTrace", "url": "https://news.google.com/rss/search?q=%22CipherTrace%22+crypto&hl=en-US&gl=US&ceid=US:en"},
    "arkham": {"name": "Arkham Intelligence", "url": "https://news.google.com/rss/search?q=%22Arkham+Intelligence%22&hl=en-US&gl=US&ceid=US:en"},
    "nansen": {"name": "Nansen", "url": "https://news.google.com/rss/search?q=%22nansen%22+blockchain+analytics&hl=en-US&gl=US&ceid=US:en"},
    "crystal_blockchain": {"name": "Crystal Intelligence", "url": "https://news.google.com/rss/search?q=%22Crystal+Intelligence%22+OR+%22Crystal+Blockchain%22&hl=en-US&gl=US&ceid=US:en"},
    "solidus_labs": {"name": "Solidus Labs", "url": "https://news.google.com/rss/search?q=%22Solidus+Labs%22&hl=en-US&gl=US&ceid=US:en"},
    "merkle_science": {"name": "Merkle Science", "url": "https://news.google.com/rss/search?q=%22Merkle+Science%22&hl=en-US&gl=US&ceid=US:en"},
}

CHAINALYSIS_KEYWORDS = ["chainalysis", "chain analysis"]

# ── Publication Category Scoring ──────────────────────────────────
# Categories: mainstream, crypto, fintech, blog, niche
PUBLICATION_CATEGORIES = {
    # Mainstream financial / general news
    "bloomberg": "mainstream", "reuters": "mainstream", "financial times": "mainstream",
    "bbc": "mainstream", "cnbc": "mainstream",
    "yahoo finance": "mainstream", "the guardian": "mainstream", "forbes": "mainstream",
    "new york times": "mainstream", "washington post": "mainstream", "ft": "mainstream",
    "associated press": "mainstream", "abc news": "mainstream", "sky news": "mainstream",
    "tech in asia": "mainstream", "axios": "mainstream", "wired": "mainstream",
    # Major crypto-native publications
    "coindesk": "crypto", "cointelegraph": "crypto", "the block": "crypto",
    "decrypt": "crypto", "beincrypto": "crypto",
    "bitcoin magazine": "crypto", "unchained": "crypto",
    "protos": "crypto", "crypto briefing": "crypto", "the defiant": "crypto",
    "cryptonews": "crypto", "cryptoslate": "crypto", "bitcoinist": "crypto",
    "dl news": "crypto", "dlnews": "crypto", "blockworks": "crypto",
    # Fintech / payments / trade press
    "pymnts": "fintech", "techcrunch": "fintech", "finextra": "fintech",
    "ledger insights": "fintech", "crowdfund insider": "fintech", "compliance week": "fintech",
    "fortune": "mainstream", "wall street journal": "mainstream",
    # Company blogs / self-published (competitor + own)
    "chainalysis": "blog", "trm labs": "blog", "elliptic": "blog",
    "arkham": "blog", "nansen": "blog", "crystal": "blog",
    "solidus labs": "blog", "merkle science": "blog",
}



# ── Hardcoded post-migration (2026-06-02) ────────────────────────────
# Workflows no longer carry env vars; the destination repo for weekly
# briefing drafts is now baked into the handler. To move the briefing
# destination, change this constant and redeploy.
GITHUB_PAGES_REPO = "chainmarketing/weekly-briefing"


def _get_pub_category(source_name):
    """Return publication category. Falls back to 'niche' for unrecognised sources."""
    sl = source_name.lower()
    for key, cat in PUBLICATION_CATEGORIES.items():
        if key in sl:
            return cat
    return "niche"

# ── Google Sheets Data Sources (weekly enrichment) ───────────────────
POLICY_CALENDAR_SHEET_ID = "1lMeoK00KfXdCRo59n-0Y21jfbhZIcb-3s46uwvBFT5g"
SEIZURE_TRACKER_SHEET_ID = "16kQzBE5KBsa2McWnsxrZuMNe7jAWXiYYZqYj_vLixqo"


# ── Shared helpers ───────────────────────────────────────────────────

def _parse_feed(content, fmt, source_name, source_key, cutoff, extra_fields=None):
    """Parse RSS/Atom feed content into article dicts."""
    import xml.etree.ElementTree as ET
    import re
    from datetime import datetime, timezone
    from email.utils import parsedate_to_datetime

    root = ET.fromstring(content)
    articles = []
    if fmt == "atom":
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title = entry.findtext("atom:title", "", ns).strip()
            link_el = entry.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""
            summary = entry.findtext("atom:summary", "", ns).strip()
            updated = entry.findtext("atom:updated", "", ns)
            try:
                pub_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pub_dt = datetime.now(timezone.utc)
            if pub_dt >= cutoff:
                a = {"source": source_name, "source_key": source_key, "title": title[:250], "url": link,
                     "description": re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", summary)).strip()[:300], "published": pub_dt.isoformat()}
                if extra_fields: a.update(extra_fields)
                articles.append(a)
    else:
        for item in root.findall(".//item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", item.findtext("description", "").strip())).strip()
            pub_date_str = item.findtext("pubDate", "")
            gn_source = item.findtext("source", "")
            try:
                pub_dt = parsedate_to_datetime(pub_date_str)
                if pub_dt.tzinfo is None: pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pub_dt = datetime.now(timezone.utc)
            if pub_dt >= cutoff:
                a = {"source": gn_source or source_name, "source_key": source_key, "title": title[:250], "url": link,
                     "description": desc[:300], "published": pub_dt.isoformat()}
                if gn_source: a["original_source"] = gn_source
                if extra_fields: a.update(extra_fields)
                articles.append(a)
    return articles


def _slim(article):
    """Strip article to essential fields for checkpoint storage."""
    return {
        "title": article.get("title", "")[:200], "url": article.get("url", ""),
        "source": article.get("source", ""), "published": article.get("published", ""),
        "matched_domains": [{"name": d["name"], "key": d["key"]} for d in article.get("matched_domains", [])[:3]],
    }


def _format_usd(value_str):
    """Convert '$35,704,005,093.91' to '$35.7B'."""
    try:
        num = float(str(value_str).replace('$', '').replace(',', ''))
        if num >= 1_000_000_000:
            return f"${num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"${num/1_000:.0f}K"
        else:
            return f"${num:,.0f}"
    except (ValueError, AttributeError):
        return str(value_str)


def _norm_title(title):
    """Normalize title for dedup: lowercase, strip source suffixes, remove punctuation."""
    import re
    t = title.lower().strip()
    # Strip common trailing source attributions: " - Source", " | Source", " : Source"
    t = re.split(r'\s*[\-\|—–:]\s*(?=[A-Z][a-z])', t)[0]
    # Remove punctuation and extra whitespace
    t = re.sub(r'[^a-z0-9 ]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ── Step: Fetch + Classify (merged) ─────────────────────────────────

@durable_step
def fetch_and_classify(step_ctx: StepContext, days_back: int) -> dict:
    """Fetch RSS + Google News, deduplicate, classify. Returns slimmed lists."""
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    rss_articles, feed_stats = [], {}

    for fk, fi in RSS_FEEDS.items():
        try:
            resp = create_session().get(fi["url"], timeout=15, headers={"User-Agent": "Chainalysis-MediaMonitor/3.0"})
            resp.raise_for_status()
            arts = _parse_feed(resp.content, fi["format"], fi["name"], fk, cutoff)
            rss_articles.extend(arts)
            feed_stats[fk] = {"name": fi["name"], "in_window": len(arts), "status": "ok"}
        except Exception as e:
            step_ctx.logger.error(f"Feed failed: {fi['name']}", extra={"error": str(e)[:120]})
            feed_stats[fk] = {"name": fi["name"], "in_window": 0, "status": f"error: {str(e)[:100]}"}

    step_ctx.logger.info(f"RSS: {len(rss_articles)} articles from {len(RSS_FEEDS)} feeds")

    # ── Google News proxy feeds (for pubs with dead/stale RSS) ──
    for fk, fi in GOOGLE_NEWS_PROXY_FEEDS.items():
        try:
            resp = create_session().get(fi["url"], timeout=15, headers={"User-Agent": "Chainalysis-MediaMonitor/3.0"})
            resp.raise_for_status()
            arts = _parse_feed(resp.content, "rss", fi["name"], fk, cutoff)
            rss_articles.extend(arts)
            feed_stats[fk] = {"name": fi["name"], "in_window": len(arts), "status": "ok (gnews proxy)"}
        except Exception as e:
            step_ctx.logger.error(f"Proxy feed failed: {fi['name']}", extra={"error": str(e)[:120]})
            feed_stats[fk] = {"name": fi["name"], "in_window": 0, "status": f"error: {str(e)[:100]}"}

    step_ctx.logger.info(f"RSS + proxy: {len(rss_articles)} articles from {len(RSS_FEEDS) + len(GOOGLE_NEWS_PROXY_FEEDS)} feeds")

    gnews_articles, publications = [], set()
    for key, search in GOOGLE_NEWS_SEARCHES.items():
        try:
            resp = create_session().get(search["url"], timeout=15, headers={"User-Agent": "Chainalysis-MediaMonitor/3.0"})
            resp.raise_for_status()
            arts = _parse_feed(resp.content, "rss", search["name"], f"gnews_{key}", cutoff)
            # Tag each article with its search category
            for a in arts:
                a["_gnews_category"] = search.get("category", "narrative")
            gnews_articles.extend(arts)
            if search.get("category") == "chainalysis_mention":
                for a in arts: publications.add(a.get("original_source", a["source"]))
        except Exception as e:
            step_ctx.logger.error(f"Google News failed: {key}", extra={"error": str(e)[:120]})

    step_ctx.logger.info(f"Google News: {len(gnews_articles)} articles from {len(GOOGLE_NEWS_SEARCHES)} searches, {len(publications)} Chainalysis publications")

    config_path = os.path.join(os.path.dirname(__file__), "domain_config.json")
    with open(config_path, "r") as f:
        domains = json.load(f)

    seen_urls, seen_titles, all_articles = set(), set(), []
    for a in gnews_articles:
        uk = a["url"].split("?")[0].rstrip("/").lower()
        nt = _norm_title(a["title"])
        if uk not in seen_urls and nt not in seen_titles:
            seen_urls.add(uk)
            if nt:
                seen_titles.add(nt)
            a["is_mention"] = a.get("_gnews_category") == "chainalysis_mention"
            all_articles.append(a)
    for a in rss_articles:
        uk = a["url"].split("?")[0].rstrip("/").lower()
        nt = _norm_title(a["title"])
        if uk not in seen_urls and nt not in seen_titles:
            seen_urls.add(uk)
            if nt:
                seen_titles.add(nt)
            text = f"{a['title']} {a.get('description', '')}".lower()
            a["is_mention"] = any(kw in text for kw in CHAINALYSIS_KEYWORDS)
            all_articles.append(a)

    for a in all_articles:
        text = f"{a['title']} {a.get('description', '')}".lower()
        matched = []
        for dk, di in domains.items():
            for sig in di.get("signals", []):
                if sig.lower() in text:
                    matched.append({"key": dk, "name": di["name"], "category": di.get("category", "")})
                    break
        a["matched_domains"] = matched

    mentions = [a for a in all_articles if a.get("is_mention")]
    narrative = [a for a in all_articles if a.get("matched_domains") and not a.get("is_mention")]

    domain_cov = {}
    for a in all_articles:
        for d in a.get("matched_domains", []):
            k = d["key"]
            if k not in domain_cov: domain_cov[k] = {"name": d["name"], "category": d["category"], "count": 0}
            domain_cov[k]["count"] += 1

    # ── Category breakdown for mentions ──────────────────────────
    cat_counts = {"mainstream": 0, "crypto": 0, "fintech": 0, "blog": 0, "niche": 0}
    for a in mentions:
        cat = _get_pub_category(a.get("source", ""))
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    stats = {"total_rss": len(rss_articles), "total_gnews": len(gnews_articles), "total_unique": len(all_articles),
             "chainalysis_mentions": len(mentions), "narrative_relevant": len(narrative),
             "duplicates_removed": (len(rss_articles) + len(gnews_articles)) - len(all_articles), "domain_coverage": domain_cov,
             "mention_categories": cat_counts}

    step_ctx.logger.info(f"Classified: {len(mentions)} mentions, {len(narrative)} narrative")

    # ── Feed health: flag feeds with errors or zero articles ────
    # Low-frequency feeds that legitimately have 0 articles on most days
    _LOW_FREQ_FEEDS = {"chainalysis_blog", "techcrunch_fintech", "compliance_week"}
    unhealthy_feeds = [fs["name"] for fk, fs in feed_stats.items()
                       if fk not in _LOW_FREQ_FEEDS and (
                           fs.get("status", "").startswith("error") or fs.get("in_window", 0) == 0)]

    return {"mentions": [_slim(a) for a in mentions[:30]], "narrative": [_slim(a) for a in narrative[:40]],
            "stats": stats, "feed_stats": feed_stats, "publications": sorted(publications), "publication_count": len(publications),
            "unhealthy_feeds": unhealthy_feeds}


# ── Step: Fetch Competitors ──────────────────────────────────────────

@durable_step
def fetch_competitors(step_ctx: StepContext, days_back: int) -> dict:
    """Fetch competitor mentions from Google News RSS."""
    from datetime import datetime, timezone, timedelta

    step_ctx.logger.info("Fetching competitor mentions")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    comp_articles, comp_stats = {}, {}

    for ck, ci in COMPETITOR_SEARCHES.items():
        try:
            resp = create_session().get(ci["url"], timeout=15, headers={"User-Agent": "Chainalysis-MediaMonitor/3.0"})
            resp.raise_for_status()
            arts = _parse_feed(resp.content, "rss", ci["name"], f"comp_{ck}", cutoff, extra_fields={"competitor": ci["name"]})
            comp_stats[ck] = {"name": ci["name"], "count": len(arts), "status": "ok"}
            comp_articles[ck] = [{"title": a["title"][:200], "url": a["url"], "source": a["source"],
                                  "published": a.get("published", ""), "competitor": ci["name"]} for a in arts[:8]]
            step_ctx.logger.info(f"  {ci['name']}: {len(arts)} articles")
        except Exception as e:
            step_ctx.logger.error(f"Competitor failed: {ci['name']}", extra={"error": str(e)[:120]})
            comp_articles[ck] = []; comp_stats[ck] = {"name": ci["name"], "count": 0, "status": f"error: {str(e)[:100]}"}

    total = sum(s["count"] for s in comp_stats.values())
    step_ctx.logger.info(f"Competitors: {total} articles across {len(COMPETITOR_SEARCHES)} competitors")
    return {"articles": comp_articles, "stats": comp_stats}


# ── Step: Fetch Key Events (weekly only) ──────────────────────────────

@durable_step
def fetch_key_events(step_ctx: StepContext) -> list:
    """Pull upcoming policy dates from Google Sheets + macro/crypto events via web search (next 14 days)."""
    import csv, io
    from datetime import datetime, timezone, timedelta
    from chainalysis_skill_google_drive import GoogleDriveClient

    step_ctx.logger.info("Fetching key events from Google Sheets + web search")
    try:
        client = GoogleDriveClient()
        resp = client.oauth.request_with_refresh(
            "GET", f"https://docs.google.com/spreadsheets/d/{POLICY_CALENDAR_SHEET_ID}/export",
            params={"format": "csv", "gid": 0})
        resp.raise_for_status()
        rows = list(csv.reader(io.StringIO(resp.text)))
    except Exception as e:
        step_ctx.logger.error(f"Policy calendar fetch failed: {e}")
        return []

    if not rows:
        return []

    headers = rows[0]
    status_idx = headers.index("Status") if "Status" in headers else -1
    if status_idx < 0:
        step_ctx.logger.warning("No Status column found in Key Dates sheet")
        return []

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=14)
    upcoming = []

    for row in rows[1:]:
        status = row[status_idx] if len(row) > status_idx else ""
        if status != "Forthcoming":
            continue
        date_start_str = row[1] if len(row) > 1 else ""
        country = row[5] if len(row) > 5 else ""
        description = row[6] if len(row) > 6 else ""
        category = row[8] if len(row) > 8 else ""
        priority = row[21] if len(row) > 21 else ""

        if not date_start_str:
            continue
        try:
            date_start = datetime.strptime(date_start_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if date_start < now or date_start > horizon:
            continue

        date_end_str = row[2] if len(row) > 2 else ""
        date_end = None
        if date_end_str:
            try:
                de = datetime.strptime(date_end_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
                if de != date_start:
                    date_end = de
            except ValueError:
                pass

        try:
            pri = int(priority)
        except (ValueError, TypeError):
            pri = 0

        upcoming.append({
            "date_start": date_start.strftime("%b %d"),
            "date_end": date_end.strftime("%b %d") if date_end else None,
            "country": country, "description": description[:120],
            "category": category[:60], "priority": pri,
        })

    # ── Supplement with macro/crypto events via web search ──────
    try:
        from datetime import datetime as _dt
        today_str = now.strftime("%Y-%m-%d")
        horizon_str = horizon.strftime("%Y-%m-%d")
        search_url = f"https://news.google.com/rss/search?q=%22crypto+conference%22+OR+%22blockchain+summit%22+OR+%22FOMC%22+OR+%22Fed+rate%22+OR+%22G20%22+OR+%22IMF%22+OR+%22ECB%22+after:{today_str}+before:{horizon_str}&hl=en-US&gl=US&ceid=US:en"
        resp2 = create_session().get(search_url, timeout=10, headers={"User-Agent": "Chainalysis-MediaMonitor/3.0"})
        if resp2.ok:
            import xml.etree.ElementTree as ET
            import re
            from email.utils import parsedate_to_datetime as _pdt
            root = ET.fromstring(resp2.content)
            seen_titles = set(e["description"].lower()[:40] for e in upcoming)
            for item in root.findall(".//item")[:10]:
                title = item.findtext("title", "").strip()
                if title.lower()[:40] in seen_titles:
                    continue
                pub_str = item.findtext("pubDate", "")
                try:
                    pub_dt = _pdt(pub_str)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    pub_dt = now
                is_macro = any(kw in title.lower() for kw in ["fomc", "fed rate", "ecb", "g20", "imf", "treasury"])
                is_conf = any(kw in title.lower() for kw in ["conference", "summit", "forum", "expo", "consensus", "token2049"])
                if is_macro or is_conf:
                    cat = "Macro/Economic" if is_macro else "Event/Conference"
                    upcoming.append({
                        "date_start": pub_dt.strftime("%b %d"),
                        "date_end": None, "country": "",
                        "description": re.sub(r"<[^>]+>", "", title)[:120],
                        "category": cat, "priority": 2 if is_macro else 1,
                    })
                    seen_titles.add(title.lower()[:40])
        step_ctx.logger.info(f"Web search added events, total now {len(upcoming)}")
    except Exception as e:
        step_ctx.logger.warning(f"Web search for events failed (non-fatal): {e}")

    upcoming.sort(key=lambda e: e.get("priority", 0), reverse=True)
    step_ctx.logger.info(f"Key events: {len(upcoming)} events in next 14 days")
    return upcoming[:8]


# ── Step: Fetch Seizure Stats (weekly only) ──────────────────────────

@durable_step
def fetch_seizure_stats(step_ctx: StepContext) -> dict:
    """Pull Chainalysis seizure involvement stats from Google Sheets."""
    import csv, io
    from chainalysis_skill_google_drive import GoogleDriveClient

    step_ctx.logger.info("Fetching seizure stats from Google Sheets")
    try:
        client = GoogleDriveClient()
        resp = client.oauth.request_with_refresh(
            "GET", f"https://docs.google.com/spreadsheets/d/{SEIZURE_TRACKER_SHEET_ID}/export",
            params={"format": "csv", "gid": 21778312})
        resp.raise_for_status()
        rows = list(csv.reader(io.StringIO(resp.text)))
    except Exception as e:
        step_ctx.logger.error(f"Seizure stats fetch failed: {e}")
        return {}

    if len(rows) < 13:
        step_ctx.logger.warning("Seizure stats sheet has insufficient data")
        return {}

    # ── Totals row (index 16) — Chainalysis involvement only ─────
    totals = rows[16] if len(rows) > 16 else []
    chain_involved_value = totals[7] if len(totals) > 7 else "$0"
    chain_involved_cases = totals[10] if len(totals) > 10 else "0"

    # ── Current year (last populated yearly row) ─────────────────
    current_year_row = None
    for row in rows[2:13]:
        if row and row[0] and row[0].strip():
            current_year_row = row

    ytd = {}
    if current_year_row:
        ytd = {
            "year": current_year_row[0],
            "seizure_value_chain": _format_usd(current_year_row[2] if len(current_year_row) > 2 else "$0"),
            "seizure_count_chain": current_year_row[4] if len(current_year_row) > 4 else "0",
            "freeze_value_chain": _format_usd(current_year_row[6] if len(current_year_row) > 6 else "$0"),
            "freeze_count_chain": current_year_row[8] if len(current_year_row) > 8 else "0",
        }

    result = {
        "chain_involved_value": _format_usd(chain_involved_value),
        "chain_involved_cases": chain_involved_cases,
        "ytd": ytd,
    }
    step_ctx.logger.info(f"Seizure stats: {result['chain_involved_value']} across {result['chain_involved_cases']} cases")
    return result


# ── Step: Load Narrative History ─────────────────────────────────────

@durable_step
def load_narrative_history(step_ctx: StepContext, lookback_days: int) -> list:
    """Pull key_narratives from recent successful runs for trend detection."""
    slug = (context.get("WORKFLOW_SLUG") or "")
    if not slug:
        step_ctx.logger.info("WORKFLOW_SLUG not set — trend tracking disabled"); return []

    from chainalysis_skill_workflows import WorkflowsClient
    from datetime import datetime, timezone, timedelta

    step_ctx.logger.info("Loading narrative history", extra={"lookback_days": lookback_days})
    client = WorkflowsClient()
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        executions = client.list_executions(slug, status="SUCCEEDED", limit=30)
    except Exception as e:
        step_ctx.logger.warning(f"Could not list executions: {e}"); return []

    history = []
    for ex in executions.get("executions", []):
        finished = ex.get("completedAt") or ex.get("startedAt", "")
        try:
            ex_dt = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        if ex_dt < cutoff: continue
        try:
            full = client.get_execution(slug, ex["id"])
        except Exception:
            continue
        output_str = full.get("output", "")
        if not output_str: continue
        try:
            output = json.loads(output_str) if isinstance(output_str, str) else output_str
        except (json.JSONDecodeError, TypeError):
            continue
        narratives = output.get("key_narratives", [])
        prev_stats = output.get("classification_stats", {})
        if narratives:
            history.append({"date": ex_dt.strftime("%Y-%m-%d"),
                            "narratives": [{"title": n.get("title", "")} for n in narratives],
                            "stats": {"mentions": prev_stats.get("chainalysis_mentions", 0),
                                      "narrative": prev_stats.get("narrative_relevant", 0)}})

    history.sort(key=lambda h: h["date"], reverse=True)
    step_ctx.logger.info(f"Loaded history from {len(history)} prior runs")
    return history


# ── Step: AI Analysis + Trend Detection ─────────────────────────────

@durable_step
def analyze_and_detect_trends(step_ctx: StepContext, mentions: list, narrative: list,
                               competitor_data: dict, narrative_history: list, days_back: int, mode: str = "daily") -> dict:
    """Single LLM call: executive summary, narratives+trends, action items/competitor spotlight, coverage/landscape."""
    from chainalysis_skill_ai import get_client

    step_ctx.logger.info("AI analysis + trend detection", extra={
        "mentions": len(mentions), "narrative": len(narrative), "history_runs": len(narrative_history)})

    mention_lines = "\n".join(f"- [{a['source']}] {a['title']}" for a in mentions[:25]) or "No direct mentions found."
    narrative_lines = "\n".join(
        f"- [{a['source']}] {a['title']} (Domains: {', '.join(d['name'] for d in a.get('matched_domains', []))})"
        for a in narrative[:25]) or "No narrative articles found."

    comp_lines = []
    for ck, articles in competitor_data.get("articles", {}).items():
        if articles:
            comp_lines.append(f"\n{articles[0].get('competitor', ck)} ({len(articles)} articles):")
            for a in articles[:5]: comp_lines.append(f"  - {a['title']}")
    comp_text = "\n".join(comp_lines) or "No competitor activity found."

    if narrative_history:
        history_text = "\n\nHISTORICAL NARRATIVES from prior runs (most recent first):\n"
        for h in narrative_history[:20]:
            history_text += f"  {h['date']}: {', '.join(n['title'] for n in h['narratives'])}\n"
        history_text += ('\nFor each narrative, compare against this history. Themes may use different words '
                         'but cover the same topic. Include a "trend" object with:\n'
                         '- "matching_dates": list of YYYY-MM-DD dates where this theme appeared before\n'
                         '- "momentum": "rising" | "stable" | "fading" | "new"\n')
    else:
        history_text = '\nNo historical data. For each narrative, set "trend": {"matching_dates": [], "momentum": "new"}\n'

    # ── Mode-conditional prompt sections ────────────────────────
    if mode == "weekly":
        item_3 = "3. COMPETITOR WATCH: For each competitor that had media coverage this period (from the COMPETITOR ACTIVITY data), provide a 1-sentence strategic read on what they are positioning on or what the coverage signals. Focus on positioning, wins, setbacks, or notable deals. If no coverage found for a competitor, skip them."
        item_5 = "5. MARKET LANDSCAPE: Return an array of 3 themed sub-sections covering the key dynamics shaping the crypto/blockchain landscape this week. Each sub-section has a short punchy title (3-5 words) and a body of 1-2 sentences MAX. Be concise and direct — no filler. Write for employees and investors."
        json_extra = '''  "competitor_reads": {{
    "competitor_name": "One sentence strategic read on their media positioning this period"
  }},
  "market_landscape": [
    {{"title": "Short theme title", "body": "1-2 sentence paragraph"}},
    {{"title": "Short theme title", "body": "1-2 sentence paragraph"}},
    {{"title": "Short theme title", "body": "1-2 sentence paragraph"}}
  ],'''
    else:
        item_3 = "3. ACTION ITEMS: Only include high or medium urgency items that require immediate attention from our team (regulatory changes, critical mentions requiring response). Do NOT include routine competitor mentions. Return empty list if nothing urgent."
        if days_back <= 1:
            item_5 = "5. COVERAGE ANALYSIS: A 2-3 sentence punchy summary comparing us vs competitors. One sentence on our coverage (article count, key topic, earned vs self-published). One sentence on competitors. One sentence verdict on who owned the cycle. Be direct, opinionated, no fluff."
        else:
            item_5 = "5. COVERAGE ANALYSIS: 3 short paragraphs (2-3 sentences each, max 100 words per paragraph). Separate with newlines. Paragraph 1: Our coverage — article count, top topic, earned vs self-published, standout placements. Paragraph 2: Competitors — one line per competitor with count and whether earned or blog. Paragraph 3: One-sentence verdict on who owned the narrative. Be punchy and direct — write for busy executives."
        json_extra = '''  "action_items": [
    {{"title": "...", "reason": "...", "urgency": "high|medium"}}
  ],
  "coverage_analysis": "...",'''

    prompt = f"""You are an internal media intelligence analyst at Chainalysis. Write as part of the team — use "we", "our", "us" when referring to Chainalysis. Never say "Chainalysis" when "we" or "our" works instead.
Analyze the following media coverage from the past {days_back} day(s) and provide:

1. EXECUTIVE SUMMARY: A concise 2-3 sentence overview of the most important developments
2. KEY NARRATIVES: 3-5 major themes. For each: a short title, 1 sentence description (max 25 words), 1 sentence on implications for us (max 20 words), and 3 specific multi-word search keywords (e.g. "bitcoin ETF", "sanctions enforcement", "stablecoin regulation"). Keep it tight — headlines not essays.
{item_3}
4. SENTIMENT BREAKDOWN: Count how many of our mentions are positive (favourable, thought leadership), neutral (factual reference), or negative (critical, reputational risk). Return as "X positive, Y neutral, Z negative".
{item_5}

CHAINALYSIS DIRECT MENTIONS ({len(mentions)} articles):
{mention_lines}

NARRATIVE COVERAGE ({len(narrative)} articles):
{narrative_lines}

COMPETITOR ACTIVITY:
{comp_text}
{history_text}
Respond in valid JSON only, no markdown fences:
{{
  "executive_summary": "...",
  "key_narratives": [
    {{"title": "...", "description": "...", "relevance": "high|medium|low", "chainalysis_relevance": "What this means for us — use first person", "search_keywords": ["multi word keyword 1", "keyword 2", "keyword 3"], "trend": {{"matching_dates": [], "momentum": "new"}}}}
  ],
{json_extra}
  "sentiment_breakdown": "X positive, Y neutral, Z negative"
}}"""

    client = get_client(model="claude-haiku-4-5")
    response = client.invoke(prompt)

    try:
        content = response.content
        if "```json" in content: content = content.split("```json")[1].split("```")[0]
        elif "```" in content: content = content.split("```")[1].split("```")[0]
        result = json.loads(content.strip())
    except (json.JSONDecodeError, IndexError) as parse_err:
        step_ctx.logger.warning(f"Failed to parse AI response: {parse_err}", extra={"raw_response": content[:500]})
        result = {"executive_summary": f"Media monitor captured {len(mentions)} mentions of us and {len(narrative)} narrative articles.",
                  "key_narratives": [], "action_items": [], "coverage_analysis": "Unable to generate coverage analysis."}

    # ── Post-process trend data ──────────────────────────────────
    from datetime import datetime, timezone
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_history_dates = sorted(set(h["date"] for h in narrative_history))
    window_days = (datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(all_history_dates[0], "%Y-%m-%d")).days + 1 if all_history_dates else 1

    for n in result.get("key_narratives", []):
        raw = n.get("trend", {})
        matching_dates = sorted(set(raw.get("matching_dates", [])))
        all_dates = sorted(set(matching_dates + [today_str]))
        appearances = len(all_dates)
        momentum = raw.get("momentum", "new")
        first_seen = all_dates[0] if all_dates else today_str
        last_before = [d for d in matching_dates if d < today_str]
        last_seen = last_before[-1] if last_before else None
        gap_days = None
        if last_seen:
            try: gap_days = (datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(last_seen, "%Y-%m-%d")).days
            except ValueError: pass
        if appearances <= 1:
            cadence, recurrence = "new", "new"
        else:
            try:
                span = max((datetime.strptime(all_dates[-1], "%Y-%m-%d") - datetime.strptime(all_dates[0], "%Y-%m-%d")).days, 1)
                density = appearances / (span + 1)
            except ValueError: density = 0
            cadence = "persistent" if density > 0.6 else ("recurring" if density > 0.3 else "sporadic")
            recurrence = f"{appearances}x over {window_days} days"
        n["trend"] = {"appearances": appearances, "window_days": window_days, "recurrence": recurrence,
                      "cadence": cadence, "momentum": momentum, "first_seen": first_seen,
                      "last_seen": last_seen, "gap_days": gap_days,
                      "is_trending": appearances >= 3 or cadence in ("persistent", "recurring")}

    trending = sum(1 for n in result.get("key_narratives", []) if n.get("trend", {}).get("is_trending"))
    step_ctx.logger.info(f"AI complete: {len(result.get('key_narratives', []))} narratives, {trending} trending")
    return result


# ── Step: Enrich Narratives with Article Links ───────────────────────

@durable_step
def enrich_narratives(step_ctx: StepContext, narratives: list, all_articles: list) -> list:
    """Match narratives to articles. Keywords split into words and scored by overlap."""
    step_ctx.logger.info("Enriching narratives", extra={"count": len(narratives), "pool": len(all_articles)})

    enriched = []
    for n in narratives:
        # Split multi-word keywords into individual terms
        kw_terms = set()
        for kw in n.get("search_keywords", []):
            for word in kw.lower().split():
                if len(word) > 3:
                    kw_terms.add(word)
        # Also add narrative title words
        title_words = set(w for w in n.get("title", "").lower().split() if len(w) > 4)

        scored = []
        for a in all_articles:
            text = a["title"].lower()
            # Score keyword term hits (weighted 3x)
            kw_hits = sum(1 for t in kw_terms if t in text)
            # Score title word hits
            tw_hits = sum(1 for w in title_words if w in text)
            total = kw_hits * 3 + tw_hits
            if total >= 3:  # require meaningful overlap
                scored.append((total, a))

        scored.sort(key=lambda x: x[0], reverse=True)
        seen, linked = set(), []
        for _, a in scored:
            if a["url"] not in seen:
                seen.add(a["url"])
                linked.append({"title": a["title"][:200], "url": a["url"], "source": a["source"]})
            if len(linked) >= 3:
                break
        enriched.append({**n, "articles": linked})
    return enriched


# ── Step: Generate Email Digest ──────────────────────────────────────

@durable_step
def generate_email_digest(step_ctx: StepContext, mentions: list, narrative: list, competitor_data: dict,
                          ai_analysis: dict, stats: dict, days_back: int) -> str:
    """Generate email-optimised HTML digest with domain heatmap."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%B %d, %Y")
    period = f"Past {days_back} day{'s' if days_back > 1 else ''}"
    total_comp = sum(len(a) for a in competitor_data.get("articles", {}).values())

    # ── Domain heatmap ───────────────────────────────────────────
    domain_cov = stats.get("domain_coverage", {})
    top_domains = sorted(domain_cov.values(), key=lambda d: d["count"], reverse=True)[:8]
    max_count = top_domains[0]["count"] if top_domains else 1
    domain_bars = ""
    for d in top_domains:
        pct = min(int(d["count"] / max_count * 100), 100)
        domain_bars += (
            f'<div style="margin-bottom:6px;"><div style="display:flex;align-items:center;">'
            f'<span style="width:200px;font-size:12px;color:#444;flex-shrink:0;">{d["name"]}</span>'
            f'<div style="flex:1;background:#eee;border-radius:4px;height:16px;margin:0 8px;">'
            f'<div style="width:{pct}%;background:linear-gradient(90deg,#1a73e8,#4dabf5);height:16px;border-radius:4px;"></div></div>'
            f'<span style="font-size:12px;font-weight:600;color:#333;width:30px;text-align:right;">{d["count"]}</span>'
            f'</div></div>'
        )

    # ── Mention rows ─────────────────────────────────────────────
    mention_rows = ""
    for a in mentions[:10]:
        mention_rows += (
            '<tr><td style="padding:12px 0;border-bottom:1px solid #f0f0f0;">'
            f'<a href="{a["url"]}" style="color:#1a73e8;text-decoration:none;font-size:15px;font-weight:500;">{a["title"][:120]}</a>'
            f'<br><span style="color:#888;font-size:12px;">{a["source"]} · {a.get("published","")[:10]}</span></td></tr>')

    # ── Narrative rows ───────────────────────────────────────────
    narrative_rows = ""
    for a in narrative[:10]:
        domains = ", ".join(d["name"] for d in a.get("matched_domains", [])[:2])
        narrative_rows += (
            '<tr><td style="padding:12px 0;border-bottom:1px solid #f0f0f0;">'
            f'<a href="{a["url"]}" style="color:#1a73e8;text-decoration:none;font-size:14px;">{a["title"][:120]}</a>'
            f'<br><span style="color:#888;font-size:12px;">{a["source"]} · {domains}</span></td></tr>')

    # ── Competitor section ───────────────────────────────────────
    comp_highlights = ""
    for ck, articles in competitor_data.get("articles", {}).items():
        if articles:
            top = articles[0]
            comp_highlights += (
                f'<div style="margin-bottom:12px;"><strong style="color:#333;">{top.get("competitor", ck)}</strong>'
                f' <span style="color:#888;">({len(articles)} articles)</span><br>'
                f'<a href="{top["url"]}" style="color:#1a73e8;text-decoration:none;font-size:13px;">{top["title"][:100]}</a> \u2014 {top.get("source", "")}</div>')

    # ── Key narratives ───────────────────────────────────────────
    narrative_bullets = ""
    for n in ai_analysis.get("key_narratives", []):
        links_html = ""
        for linked in n.get("articles", [])[:2]:
            links_html += (
                f'<br><a href="{linked["url"]}" style="color:#1a73e8;text-decoration:none;font-size:13px;">'
                f'{linked["source"]}: {linked["title"][:100]}</a>')
        relevance_html = ""
        if n.get("chainalysis_relevance"):
            relevance_html = f'<br><em style="color:#1a1a2e;font-size:12px;">For us: {n["chainalysis_relevance"]}</em>'
        trend = n.get("trend", {})
        cadence = trend.get("cadence", "new")
        trend_html = ""
        if trend.get("is_trending"):
            ce = {"persistent": "\U0001f525", "recurring": "\U0001f501", "sporadic": "\U0001f4cc"}.get(cadence, "\U0001f501")
            me = {"rising": "\U0001f4c8", "stable": "\u27a1\ufe0f", "fading": "\U0001f4c9"}.get(trend.get("momentum", ""), "")
            bg = {"persistent": "#d32f2f", "recurring": "#ff6f00", "sporadic": "#f9a825"}.get(cadence, "#ff6f00")
            gap = trend.get("gap_days")
            gap_l = f" \u00b7 back after {gap}d" if gap and gap > 1 else ""
            trend_html = (f' <span style="display:inline-block;padding:1px 6px;background:{bg};color:#fff;border-radius:8px;font-size:10px;font-weight:600;">{ce} {cadence.title()}</span>'
                          f' <span style="color:#888;font-size:11px;">{me} {trend.get("recurrence","")}{gap_l}</span>')
        elif cadence == "new":
            trend_html = ' <span style="display:inline-block;padding:1px 6px;background:#e3f2fd;color:#1565c0;border-radius:8px;font-size:10px;font-weight:600;">\u2728 New</span>'
        narrative_bullets += (f'<li style="margin-bottom:14px;"><strong>{n["title"]}</strong>{trend_html} \u2014 {n["description"]}'
                              f'{relevance_html}{links_html}</li>')

    # ── Assemble email (mentions ABOVE narratives) ────────────────
    email = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table role="presentation" style="width:100%;border-collapse:collapse;">
<tr><td style="padding:20px 0;" align="center">
<table role="presentation" style="width:600px;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <tr><td style="background:#1a1a2e;padding:28px 32px;text-align:center;">
    <h1 style="margin:0;color:#fff;font-size:22px;letter-spacing:0.5px;">CHAINALYSIS NEWS DIGEST</h1>
    <p style="margin:8px 0 0;color:rgba(255,255,255,0.7);font-size:14px;">{now} \u2014 {period}</p>
    <p style="margin:8px 0 0;color:rgba(255,255,255,0.5);font-size:12px;">Tracking our media coverage, competitor activity, and industry narratives across 22 RSS feeds and Google News.</p>
  </td></tr>
  <tr><td style="padding:20px 32px;background:#f8f9fa;border-bottom:1px solid #eee;">
    <table role="presentation" style="width:100%;border-collapse:collapse;"><tr>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#1a73e8;">{stats.get('chainalysis_mentions',0)}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Our Mentions</div></td>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#34a853;">{stats.get('narrative_relevant',0)}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Narrative</div></td>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#f57c00;">{total_comp}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Competitor</div></td>
    </tr></table>
  </td></tr>
  <tr><td style="padding:24px 32px;">
    <h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">Executive Summary</h2>
    <p style="margin:0;color:#444;line-height:1.6;font-size:14px;">{ai_analysis.get('executive_summary','No summary available.')}</p>
  </td></tr>
  <tr><td style="padding:0 32px 24px;">
    <h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f4f0 Chainalysis in the News</h2>
    <table role="presentation" style="width:100%;border-collapse:collapse;">
      {mention_rows or '<tr><td style="padding:12px 0;color:#888;">No direct mentions today.</td></tr>'}
    </table>
  </td></tr>
  {"" if not narrative_bullets else (
    '<tr><td style="padding:0 32px 24px;">'
    '<h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f50d Key Narratives</h2>'
    f'<ul style="margin:0;padding-left:20px;color:#444;font-size:14px;line-height:1.5;">{narrative_bullets}</ul>'
    '</td></tr>')}
  {"" if not domain_bars else (
    '<tr><td style="padding:0 32px 24px;">'
    '<h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f4ca Topic Coverage — articles by policy domain</h2>'
    + domain_bars + '</td></tr>')}
  {"" if not narrative_rows else (
    '<tr><td style="padding:0 32px 24px;">'
    '<h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f9ed Industry Coverage</h2>'
    f'<table role="presentation" style="width:100%;border-collapse:collapse;">{narrative_rows}</table>'
    '</td></tr>')}
  {"" if not comp_highlights else (
    '<tr><td style="padding:0 32px 24px;">'
    '<h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f3e2 Competitor Watch</h2>'
    + comp_highlights + '</td></tr>')}
  {"" if not ai_analysis.get("coverage_analysis") else ('<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f9e0 Coverage Analysis</h2><p style="margin:0;color:#444;line-height:1.6;font-size:14px;">' + ai_analysis.get("coverage_analysis", "") + '</p></td></tr>')}
  <tr><td style="padding:20px 32px;background:#f8f9fa;text-align:center;border-top:1px solid #eee;">
    <p style="margin:0;color:#999;font-size:11px;">Chainalysis Daily Media Monitor \u00b7 Generated {now}</p>
  </td></tr>
</table></td></tr></table></body></html>"""
    return email


# ── Step: Generate Weekly Email Digest ───────────────────────────────

@durable_step
def generate_weekly_email_digest(step_ctx: StepContext, mentions: list, narrative: list, competitor_data: dict,
                                  ai_analysis: dict, stats: dict, days_back: int,
                                  seizure_stats: dict = None) -> str:
    """Strategic weekly email for leadership."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%B %d, %Y")
    total_comp = sum(len(a) for a in competitor_data.get("articles", {}).values())
    narratives = ai_analysis.get("key_narratives", [])
    persistent = [n for n in narratives if n.get("trend", {}).get("cadence") in ("persistent", "recurring")]
    emerging = [n for n in narratives if n.get("trend", {}).get("cadence") in ("new", "sporadic")]

    persistent_html = ""
    for n in persistent:
        t = n.get("trend", {})
        ce = {"persistent": "\U0001f525", "recurring": "\U0001f501"}.get(t.get("cadence", ""), "\U0001f4cc")
        me = {"rising": "\U0001f4c8", "stable": "\u27a1\ufe0f", "fading": "\U0001f4c9"}.get(t.get("momentum", ""), "")
        gap = t.get("gap_days"); gap_n = f" \u00b7 resurfaced after {gap}d" if gap and gap > 1 else ""
        arts = "".join(f'<a href="{a["url"]}" style="color:#1a73e8;text-decoration:none;font-size:12px;display:block;margin-top:3px;">{a["source"]}: {a["title"][:80]}</a>' for a in n.get("articles", [])[:2])
        rel = f'<p style="margin:6px 0 0;color:#1a1a2e;font-size:12px;font-style:italic;">\U0001f3e2 {n["chainalysis_relevance"]}</p>' if n.get("chainalysis_relevance") else ""
        persistent_html += (f'<div style="margin-bottom:16px;padding:14px;background:#fff8e1;border-radius:6px;border-left:4px solid #ff6f00;">'
                            f'<strong style="font-size:14px;color:#222;">{n["title"]}</strong>'
                            f' <span style="font-size:11px;color:#888;">{ce} {t.get("recurrence","")} {me}{gap_n}</span>'
                            f'<p style="margin:4px 0 0;color:#555;font-size:13px;">{n["description"]}</p>{rel}{arts}</div>')

    emerging_html = ""
    for n in emerging:
        emerging_html += (f'<div style="margin-bottom:10px;padding:10px;background:#e3f2fd;border-radius:4px;border-left:3px solid #1565c0;">'
                          f'<strong style="font-size:13px;color:#222;">{n["title"]}</strong> <span style="font-size:10px;color:#888;">\u2728 New</span>'
                          f'<p style="margin:3px 0 0;color:#555;font-size:12px;">{n["description"]}</p></div>')

    mention_html = "".join(
        f'<tr><td style="padding:8px 0;border-bottom:1px solid #f0f0f0;"><a href="{a["url"]}" style="color:#1a73e8;text-decoration:none;font-size:13px;">{a["title"][:100]}</a>'
        f'<br><span style="color:#888;font-size:11px;">{a["source"]} \u00b7 {a.get("published","")[:10]}</span></td></tr>' for a in mentions[:15])

    comp_html = ""
    for ck, articles in competitor_data.get("articles", {}).items():
        if articles:
            top = articles[0]
            comp_name = top.get("competitor", ck)
            comp_html += (f'<div style="margin-bottom:10px;">'
                f'<strong style="color:#333;font-size:13px;">{comp_name}</strong>'
                f' <span style="color:#888;font-size:12px;">({len(articles)} articles)</span><br>'
                f'<a href="{top["url"]}" style="color:#1a73e8;text-decoration:none;font-size:13px;">{top["title"][:100]}</a> \u2014 {top.get("source", "")}')
            if len(articles) > 1:
                also_links = ", ".join(f'<a href="{a["url"]}" style="color:#1a73e8;text-decoration:none;font-size:12px;">{a.get("source", a.get("competitor", ""))}</a>' for a in articles[1:6])
                comp_html += f'<br><span style="color:#888;font-size:11px;">Also: {also_links}</span>'
            comp_html += '</div>'

    # Domain heatmap
    domain_cov = stats.get("domain_coverage", {})
    top_domains = sorted(domain_cov.values(), key=lambda d: d["count"], reverse=True)[:6]
    max_count = top_domains[0]["count"] if top_domains else 1
    domain_bars = ""
    for d in top_domains:
        pct = min(int(d["count"] / max_count * 100), 100)
        domain_bars += (f'<div style="margin-bottom:4px;display:flex;align-items:center;">'
                        f'<span style="width:180px;font-size:11px;color:#444;">{d["name"]}</span>'
                        f'<div style="flex:1;background:#eee;border-radius:3px;height:14px;margin:0 8px;">'
                        f'<div style="width:{pct}%;background:#1a73e8;height:14px;border-radius:3px;"></div></div>'
                        f'<span style="font-size:11px;font-weight:600;color:#333;width:25px;text-align:right;">{d["count"]}</span></div>')


    # ── Seizure Scorecard HTML ───────────────────────────────────
    seizure_html = ""
    if seizure_stats:
        ss = seizure_stats
        seizure_html += (
            '<div style="padding:16px;background:#e8f5e9;border-radius:6px;border-left:4px solid #2e7d32;">'
            f'<div style="font-size:14px;color:#222;margin-bottom:8px;">'
            f'\U0001f517 <strong>{ss.get("chain_involved_value", "N/A")}</strong> '
            f'seized & frozen with Chainalysis involvement \u2014 '
            f'<strong>{ss.get("chain_involved_cases", "N/A")} cases</strong></div>')
    email = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table role="presentation" style="width:100%;border-collapse:collapse;">
<tr><td style="padding:20px 0;" align="center">
<table role="presentation" style="width:600px;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <tr><td style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:32px;text-align:center;">
    <h1 style="margin:0;color:#fff;font-size:22px;">CHAINALYSIS WEEKLY BRIEFING</h1>
    <p style="margin:8px 0 0;color:rgba(255,255,255,0.7);font-size:14px;">Week ending {now}</p>
    <p style="margin:8px 0 0;color:rgba(255,255,255,0.5);font-size:12px;">Weekly strategic overview of our media presence, competitive positioning, and emerging industry narratives across crypto and mainstream outlets.</p>
  </td></tr>
  <tr><td style="padding:20px 32px;background:#f8f9fa;border-bottom:1px solid #eee;">
    <table role="presentation" style="width:100%;border-collapse:collapse;"><tr>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#1a73e8;">{stats.get('chainalysis_mentions',0)}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Our Mentions</div></td>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#34a853;">{stats.get('narrative_relevant',0)}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Narrative</div></td>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#f57c00;">{total_comp}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Competitor</div></td>
      <td style="text-align:center;padding:8px;"><div style="font-size:24px;font-weight:700;color:#7b1fa2;">{len(persistent)}</div><div style="font-size:11px;color:#888;text-transform:uppercase;">Persistent</div></td>
    </tr></table>
  </td></tr>
  <tr><td style="padding:24px 32px;">
    <h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">Strategic Summary</h2>
    <p style="margin:0;color:#444;line-height:1.6;font-size:14px;">{ai_analysis.get('executive_summary','')}</p>
  </td></tr>
  <tr><td style="padding:0 32px 24px;">
    <h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">Chainalysis in the News</h2>
    <table role="presentation" style="width:100%;border-collapse:collapse;">{mention_html or '<tr><td style="padding:8px 0;color:#888;">No direct mentions this week.</td></tr>'}</table>
  </td></tr>
  {"" if not persistent_html else '<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 4px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f525 Persistent Themes</h2><p style="margin:0 0 14px;color:#888;font-size:12px;">Narratives shaping the landscape.</p>' + persistent_html + '</td></tr>'}
  {"" if not emerging_html else '<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 4px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\u2728 Emerging This Week</h2><p style="margin:0 0 14px;color:#888;font-size:12px;">New themes to watch.</p>' + emerging_html + '</td></tr>'}
  {"" if not domain_bars else '<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f4ca Topic Coverage — articles by policy domain</h2>' + domain_bars + '</td></tr>'}
  {"" if not seizure_html else ('<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f4ca Seize & Freeze Scorecard</h2>' + seizure_html + '</td></tr>')}
  {"" if not comp_html else '<tr><td style="padding:0 32px 24px;"><h2 style="margin:0 0 12px;font-size:16px;color:#1a1a2e;text-transform:uppercase;letter-spacing:1px;">\U0001f3e2 Competitor Activity</h2>' + comp_html + '</td></tr>'}
  <tr><td style="padding:20px 32px;background:#f8f9fa;text-align:center;border-top:1px solid #eee;">
    <p style="margin:0;color:#999;font-size:11px;">Chainalysis Weekly Briefing \u00b7 {now}</p>
  </td></tr>
</table></td></tr></table></body></html>"""
    return email


# ── Step: Build Slack Payload ────────────────────────────────────────

@durable_step
def build_slack_payload(step_ctx: StepContext, mentions: list, narrative: list, competitor_data: dict,
                        ai_analysis: dict, stats: dict, days_back: int, mode: str,
                        seizure_stats: dict = None) -> dict:
    """Build Slack Block Kit payload. Order: Summary → Mentions → Competitors → Coverage Analysis → Narratives → rest."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%B %d, %Y")
    total_comp = sum(len(a) for a in competitor_data.get("articles", {}).values())
    narratives = ai_analysis.get("key_narratives", [])
    is_weekly = mode == "weekly"
    header = f"\U0001f4ca Weekly Briefing \u2014 {now}" if is_weekly else f"\U0001f4e1 Daily Media Monitor \u2014 {now}"
    period = f"past {days_back} day{'s' if days_back > 1 else ''}"

    # ── Category + WoW + Sentiment breakdown ────────────────────
    cats = stats.get("mention_categories", {})
    cat_parts = []
    for cat_key, cat_label in [("mainstream", "mainstream"), ("crypto", "crypto"), ("fintech", "fintech"), ("blog", "blog"), ("niche", "niche")]:
        count = cats.get(cat_key, 0)
        if count > 0:
            cat_parts.append(f"{count} {cat_label}")
    tier_note = f" ({', '.join(cat_parts)})" if cat_parts else ""
    wow = stats.get("wow_delta", {})
    wow_note = ""
    if wow:
        m_delta = wow.get("mentions_delta", 0)
        arrow = "\U0001f4c8" if m_delta > 0 else ("\U0001f4c9" if m_delta < 0 else "\u27a1\ufe0f")
        if m_delta != 0:
            period_label = "vs last week" if is_weekly else "vs yesterday"
            wow_note = f" {arrow} {'+' if m_delta > 0 else ''}{m_delta} {period_label}"
    sentiments = stats.get("sentiment_summary", {})
    sent_note = ""
    neg = sentiments.get("negative", 0)
    if neg > 0:
        sent_note = f" \u00b7 \U0001f6a8 {neg} negative"

    desc = ("Weekly strategic overview of our media presence, competitive positioning, and emerging industry narratives across crypto and mainstream outlets." if is_weekly
            else "Tracking our media coverage, competitor activity, and industry narratives across 22 RSS feeds and Google News.")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": header, "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"_{desc}_"}},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*{stats.get('chainalysis_mentions',0)}* our mentions{tier_note}{wow_note} \u00b7 "
            f"*{stats.get('narrative_relevant',0)}* narrative \u00b7 "
            f"*{total_comp}* competitor articles ({period}){sent_note}"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*\U0001f4cb {'Strategic Summary' if is_weekly else 'Executive Summary'}*\n{ai_analysis.get('executive_summary', '')}"}},
    ]

    # ── Our Mentions ─────────────────────────────────────
    if mentions:
        blocks.append({"type": "divider"})
        text = f"*\U0001f4f0 Our Mentions* ({len(mentions)} total)\n"
        if is_weekly:
            for a in mentions[:15]:
                text += f"\u2022 <{a['url']}|{a['title'][:90]}> \u2014 _{a['source']}_\n"
        else:
            for a in mentions[:20]:
                text += f"\u2022 <{a['url']}|{a['title'][:90]}> \u2014 _{a['source']}_\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # ── Competitor Watch (immediately after mentions) ────────────
    if total_comp > 0:
        blocks.append({"type": "divider"})
        text = f"*\U0001f3e2 Competitor Watch* ({total_comp} total)\n"
        for ck, articles in competitor_data.get("articles", {}).items():
            if articles:
                top = articles[0]
                comp_name = top.get("competitor", ck)
                if is_weekly:
                    text += f"\u2022 *{comp_name}* ({len(articles)}): <{top['url']}|{top['title'][:70]}>\n"
                else:
                    text += f"\u2022 *{comp_name}* ({len(articles)}): <{top['url']}|{top['title'][:70]}>"
                    if len(articles) > 1:
                        rest = ", ".join(f"<{a['url']}|{a['source']}>" for a in articles[1:8])
                        text += f"\n    also: {rest}"
                    text += "\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # ── Coverage Analysis / Market Landscape (mode-dependent) ────
    if not is_weekly:
        coverage = ai_analysis.get("coverage_analysis", "")
        if coverage:
            blocks.append({"type": "divider"})
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                f"*\U0001f9e0 Coverage Analysis*\n{coverage}"}})

    # ── Key Narratives ───────────────────────────────────────────
    if narratives:
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*\U0001f50d Key Narratives*"}})

        if is_weekly:
            persistent = [n for n in narratives if n.get("trend", {}).get("cadence") in ("persistent", "recurring")]
            emerging = [n for n in narratives if n.get("trend", {}).get("cadence") in ("new", "sporadic")]
            if persistent:
                for n in persistent:
                    t = n.get("trend", {})
                    ce = {"persistent": "\U0001f525", "recurring": "\U0001f501"}.get(t.get("cadence", ""), "\U0001f4cc")
                    me = {"rising": "\U0001f4c8", "stable": "\u27a1\ufe0f", "fading": "\U0001f4c9"}.get(t.get("momentum", ""), "")
                    gap = t.get("gap_days"); gn = f" _(back after {gap}d)_" if gap and gap > 1 else ""
                    text = f"{ce} *{n['title']}* \u00b7 `{t.get('recurrence','')}` {me}{gn}\n{n['description']}\n"
                    if n.get("chainalysis_relevance"):
                        text += f"> \U0001f3e2 _{n['chainalysis_relevance']}_\n"
                    for linked in n.get("articles", [])[:2]:
                        text += f"\U0001f4ce _{linked['source']}:_ <{linked['url']}|{linked['title'][:90]}>\n"
                    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
            if emerging:
                text = "*\u2728 Emerging*\n"
                for n in emerging:
                    text += f"\u2022 *{n['title']}* \u2014 {n['description'][:100]}\n"
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        else:
            for n in narratives[:5]:
                emoji = {"high": "\U0001f534", "medium": "\U0001f7e1", "low": "\U0001f7e2"}.get(n.get("relevance", "medium"), "\u26aa")
                t = n.get("trend", {}); cadence = t.get("cadence", "new"); tl = ""
                if t.get("is_trending"):
                    ce = {"persistent": "\U0001f525", "recurring": "\U0001f501", "sporadic": "\U0001f4cc"}.get(cadence, "\U0001f501")
                    me = {"rising": "\U0001f4c8", "stable": "\u27a1\ufe0f", "fading": "\U0001f4c9"}.get(t.get("momentum", ""), "")
                    gap = t.get("gap_days"); gn = f" _(back after {gap}d)_" if gap and gap > 1 else ""
                    tl = f" {ce} `{t.get('recurrence','')}` {me}{gn}"
                elif cadence == "new":
                    tl = " \u2728 `New`"
                text = f"{emoji} *{n['title']}*{tl}\n{n['description']}\n"
                if n.get("chainalysis_relevance"):
                    text += f"> \U0001f3e2 _{n['chainalysis_relevance']}_\n"
                for linked in n.get("articles", [])[:2]:
                    text += f"\U0001f4ce _{linked['source']}:_ <{linked['url']}|{linked['title'][:90]}>\n"
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # ── Action items (daily) / Competitor Spotlight (weekly) ─────
    action_items = ai_analysis.get("action_items", [])
    if action_items and not is_weekly:
        blocks.append({"type": "divider"})
        text = "*\u26a1 Action Items*\n"
        for item in action_items[:3]:
            emoji = {"high": "\U0001f6a8", "medium": "\u26a0\ufe0f"}.get(item.get("urgency", "medium"), "\u2022")
            text += f"{emoji} *{item['title']}* \u2014 {item.get('reason','')}\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # ── TRM & Elliptic Competitor Spotlight (weekly only) ─────────
    # ── Seizure Scorecard (weekly only, no countries) ────────────
    if is_weekly and seizure_stats:
        blocks.append({"type": "divider"})
        text = "*\U0001f4ca Seize & Freeze Scorecard*\n"
        text += f"\U0001f517 *{seizure_stats.get('chain_involved_value', 'N/A')}* seized & frozen with Chainalysis involvement \u2014 *{seizure_stats.get('chain_involved_cases', 'N/A')} cases*\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # ── Feed health warning ──────────────────────────────────────
    unhealthy = stats.get("unhealthy_feeds", [])
    if unhealthy and not is_weekly:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"\u26a0\ufe0f _Feed issues: {', '.join(unhealthy[:5])}{'...' if len(unhealthy) > 5 else ''}_"}]})

    blocks.append({"type": "divider"})
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
        f"_Chainalysis {'Weekly Briefing' if is_weekly else 'Daily Media Monitor'} \u00b7 {now} \u00b7 {stats.get('total_unique',0)} articles analysed_"}]})

    # ── GitHub Pages link (weekly only) ──────────────────────────
    if is_weekly:
        gh_repo = GITHUB_PAGES_REPO
        if gh_repo:
            from datetime import datetime as _dt
            date_str = _dt.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d")
            owner = gh_repo.split("/")[0]
            repo_name = gh_repo.split("/")[1] if "/" in gh_repo else gh_repo
            full_url = f"https://{owner}.github.io/{repo_name}/drafts/{date_str}.html"
            archive_url = f"https://{owner}.github.io/{repo_name}/"
            blocks.append({"type": "actions", "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "\u270f\ufe0f Review Draft", "emoji": True},
                 "url": full_url, "style": "primary"},
                {"type": "button", "text": {"type": "plain_text", "text": "\U0001f4cb Browse Archive", "emoji": True},
                 "url": archive_url},
            ]})

    fallback = (f"{'Weekly Briefing' if is_weekly else 'Daily Media Monitor'} ({now}): "
                f"{stats.get('chainalysis_mentions',0)} our mentions, "
                f"{stats.get('narrative_relevant',0)} narrative, {total_comp} competitor")
    return {"text": fallback, "blocks": blocks}


# ── Step: Send to Slack ──────────────────────────────────────────────

@durable_step
def send_to_slack(step_ctx: StepContext, payload: dict, mode: str = "daily", webhook_key: str = "") -> dict:
    """Post digest to Slack via incoming webhook. Uses webhook_key if provided, else selects by mode."""
    step_ctx.logger.info(f"Webhook routing: mode={mode}, webhook_key={webhook_key!r}")
    if webhook_key:
        webhook_url = context.get(f"SLACK_WEBHOOK_URL_{webhook_key.upper()}", "")
    else:
        webhook_url = context.get(f"SLACK_WEBHOOK_URL_{mode.upper()}", "") or (context.get("SLACK_WEBHOOK_URL") or "")
    if not webhook_url:
        step_ctx.logger.info(f"No Slack webhook configured for mode={mode}, webhook_key={webhook_key!r}"); return {"sent": False, "reason": f"no webhook for {webhook_key or mode}"}
    try:
        resp = create_session().post(webhook_url, json=payload, timeout=15, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        step_ctx.logger.info("Slack message sent successfully")
        return {"sent": True, "status_code": resp.status_code}
    except Exception as e:
        step_ctx.logger.error(f"Slack delivery failed: {e}")
        return {"sent": False, "reason": str(e)[:200]}


def _compute_topic_velocity(domain_by_week):
    """Compute which topics are accelerating or decelerating across weeks."""
    velocities = []
    for domain, weeks in domain_by_week.items():
        sorted_weeks = sorted(weeks.items())
        if len(sorted_weeks) < 2:
            continue
        # Compare last 2 weeks vs first 2 weeks
        first_half = sum(c for _, c in sorted_weeks[:len(sorted_weeks)//2])
        second_half = sum(c for _, c in sorted_weeks[len(sorted_weeks)//2:])
        if first_half > 0:
            change_pct = int(((second_half - first_half) / first_half) * 100)
        elif second_half > 0:
            change_pct = 100
        else:
            continue
        total = sum(c for _, c in sorted_weeks)
        velocities.append({"domain": domain, "total": total, "change_pct": change_pct,
                           "direction": "accelerating" if change_pct > 20 else ("decelerating" if change_pct < -20 else "stable")})
    velocities.sort(key=lambda v: abs(v["change_pct"]), reverse=True)
    return velocities[:8]


# ── Step: Load Monthly Data (aggregates past 30 days of runs) ────────

@durable_step
def load_monthly_data(step_ctx: StepContext) -> dict:
    """Pull all successful execution outputs from the past 30 days and aggregate."""
    slug = (context.get("WORKFLOW_SLUG") or "")
    if not slug:
        step_ctx.logger.warning("WORKFLOW_SLUG not set"); return {}

    from chainalysis_skill_workflows import WorkflowsClient
    from datetime import datetime, timezone, timedelta

    step_ctx.logger.info("Loading 30 days of execution data for monthly analysis")
    client = WorkflowsClient()
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    try:
        executions = client.list_executions(slug, status="SUCCEEDED", limit=100)
    except Exception as e:
        step_ctx.logger.warning(f"Could not list executions: {e}"); return {}

    # Aggregate across all runs
    weekly_summaries = []
    weekly_mention_trend = {}  # week_start → mention count
    domain_by_week = {}  # domain_name → {week → count}
    mention_titles = []  # all mention titles for citation quality analysis
    all_narratives = {}
    total_mentions = 0
    total_narrative = 0
    mention_cats_agg = {"mainstream": 0, "crypto": 0, "fintech": 0, "blog": 0, "niche": 0}
    sentiment_agg = {"positive": 0, "neutral": 0, "negative": 0}
    domain_agg = {}
    competitor_agg = {}  # competitor → total count
    all_sources = {}  # source → count of our mentions

    for ex in executions.get("executions", []):
        finished = ex.get("completedAt") or ex.get("startedAt", "")
        try:
            ex_dt = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        if ex_dt < cutoff:
            continue
        try:
            full = client.get_execution(slug, ex["id"])
        except Exception:
            continue
        output_str = full.get("output", "")
        if not output_str:
            continue
        try:
            output = json.loads(output_str) if isinstance(output_str, str) else output_str
        except (json.JSONDecodeError, TypeError):
            continue

        run_mode = output.get("mode", "")
        stats = output.get("classification_stats", {})
        date_str = ex_dt.strftime("%Y-%m-%d")

        # Aggregate mentions
        m = stats.get("chainalysis_mentions", 0)
        total_mentions += m
        total_narrative += stats.get("narrative_relevant", 0)

        # Weekly trend bucket
        from datetime import datetime as _dt
        week_start = (_dt.strptime(date_str, "%Y-%m-%d") - timedelta(days=_dt.strptime(date_str, "%Y-%m-%d").weekday())).strftime("%Y-%m-%d")
        weekly_mention_trend[week_start] = weekly_mention_trend.get(week_start, 0) + m

        # Category breakdown
        cats = stats.get("mention_categories", {})
        for k in mention_cats_agg:
            mention_cats_agg[k] += cats.get(k, 0)

        # Sentiment
        sent = stats.get("sentiment_summary", {})
        for k in sentiment_agg:
            sentiment_agg[k] += sent.get(k, 0)

        # Domain coverage
        for dk, dv in stats.get("domain_coverage", {}).items():
            if dk not in domain_agg:
                domain_agg[dk] = {"name": dv["name"], "count": 0}
            domain_agg[dk]["count"] += dv["count"]
            # Track per-week for velocity
            if dv["name"] not in domain_by_week:
                domain_by_week[dv["name"]] = {}
            domain_by_week[dv["name"]][week_start] = domain_by_week[dv["name"]].get(week_start, 0) + dv["count"]

        # Narratives (fuzzy dedup by normalised title)
        for n in output.get("key_narratives", []):
            title = n.get("title", "")
            if not title:
                continue
            # Normalise for dedup: lowercase, strip filler words
            norm = " ".join(w for w in title.lower().split() if len(w) > 3)[:60]
            # Find existing match
            matched_key = None
            for existing_key, existing_data in all_narratives.items():
                existing_norm = " ".join(w for w in existing_key.lower().split() if len(w) > 3)[:60]
                # Check if >60% of words overlap
                words_a = set(norm.split())
                words_b = set(existing_norm.split())
                if words_a and words_b:
                    overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
                    if overlap > 0.6:
                        matched_key = existing_key
                        break
            if matched_key:
                all_narratives[matched_key]["count"] += 1
                all_narratives[matched_key]["last_seen"] = max(all_narratives[matched_key]["last_seen"], date_str)
            else:
                all_narratives[title] = {"count": 0, "first_seen": date_str, "last_seen": date_str}
                all_narratives[title]["count"] += 1

        # Competitors
        for h in output.get("competitor_intelligence", {}).get("highlights", []):
            comp = h.get("competitor", "")
            if comp:
                competitor_agg[comp] = competitor_agg.get(comp, 0) + h.get("count", 0)

        # Mention titles for citation quality
        for a in output.get("chainalysis_mentions", []):
            mention_titles.append(a.get("title", ""))

        # Source tracking (with URLs)
        for a in output.get("chainalysis_mentions", []):
            src = a.get("source", "")
            if src:
                if src not in all_sources:
                    all_sources[src] = {"count": 0, "url": a.get("url", "")}
                all_sources[src]["count"] += 1

        # Weekly summaries
        if run_mode == "weekly":
            weekly_summaries.append({
                "date": date_str,
                "summary": output.get("executive_summary", "")[:200],
                "mentions": m,
                "coverage": output.get("coverage_analysis", "")[:200],
            })

    # Sort narratives by frequency
    top_narratives = sorted(all_narratives.items(), key=lambda x: x[1]["count"], reverse=True)[:15]
    top_sources = sorted(all_sources.items(), key=lambda x: x[1]["count"], reverse=True)[:15]
    top_competitors = sorted(competitor_agg.items(), key=lambda x: x[1], reverse=True)
    top_domains = sorted(domain_agg.values(), key=lambda x: x["count"], reverse=True)[:10]

    result = {
        "total_mentions": total_mentions,
        "total_narrative": total_narrative,
        "mention_categories": mention_cats_agg,
        "sentiment_totals": sentiment_agg,
        "weekly_summaries": weekly_summaries,
        "top_narratives": [{"title": t, "appearances": d["count"], "first_seen": d["first_seen"], "last_seen": d["last_seen"]} for t, d in top_narratives],
        "top_sources": [{"source": s, "count": d["count"], "url": d["url"]} for s, d in top_sources],
        "top_competitors": [{"competitor": c, "total_articles": n} for c, n in top_competitors],
        "top_domains": top_domains,
        "weekly_mention_trend": [{"week": w, "mentions": c} for w, c in sorted(weekly_mention_trend.items())],
        "mention_titles": mention_titles[:50],
        "topic_velocity": _compute_topic_velocity(domain_by_week),
        "runs_analysed": len([e for e in executions.get("executions", [])]),
    }

    step_ctx.logger.info(f"Monthly data: {result['total_mentions']} mentions, {len(top_narratives)} narratives, {len(weekly_summaries)} weekly summaries from {result['runs_analysed']} runs")
    return result


# ── Step: Monthly AI Analysis ────────────────────────────────────────

@durable_step
def monthly_analysis(step_ctx: StepContext, monthly_data: dict) -> dict:
    """Strategic monthly analysis from aggregated data."""
    from chainalysis_skill_ai import get_client

    step_ctx.logger.info("Monthly strategic analysis")

    # Build the context for the LLM
    narratives_text = "\n".join(
        f"- {n['title']} (appeared {n['appearances']}x, {n['first_seen']} to {n['last_seen']})"
        for n in monthly_data.get("top_narratives", []))

    sources_text = "\n".join(
        f"- {s['source']}: {s['count']} mentions"
        for s in monthly_data.get("top_sources", []))

    competitors_text = "\n".join(
        f"- {c['competitor']}: {c['total_articles']} articles"
        for c in monthly_data.get("top_competitors", []))

    weekly_text = "\n".join(
        f"- Week of {w['date']}: {w['mentions']} mentions. {w['summary']}"
        for w in monthly_data.get("weekly_summaries", []))

    cats = monthly_data.get("mention_categories", {})
    cats_text = ", ".join(f"{v} {k}" for k, v in cats.items() if v > 0)

    sent = monthly_data.get("sentiment_totals", {})
    domains_text = "\n".join(f"- {d['name']}: {d['count']} articles" for d in monthly_data.get("top_domains", []))

    velocity_text = "\n".join(
        f"- {v['domain']}: {v['total']} articles, {v['direction']} ({'+' if v['change_pct'] > 0 else ''}{v['change_pct']}% month-over-month)"
        for v in monthly_data.get("topic_velocity", []))

    # Sample of mention titles for citation quality analysis
    titles_sample = "\n".join(f"- {t}" for t in monthly_data.get("mention_titles", [])[:20])

    prompt = f"""You are our internal strategic media intelligence analyst at Chainalysis. Write as part of the team — use "we", "our", "us" instead of "Chainalysis" wherever possible.
Analyse the following 30-day aggregated media data and produce a monthly strategic briefing for the comms team.

MONTHLY STATS:
- Total mentions of us: {monthly_data.get('total_mentions', 0)}
- Breakdown: {cats_text}
- Sentiment: {sent.get('positive', 0)} positive, {sent.get('neutral', 0)} neutral, {sent.get('negative', 0)} negative
- Total narrative articles: {monthly_data.get('total_narrative', 0)}

WEEKLY SUMMARIES:
{weekly_text or 'No weekly summaries available.'}

TOP RECURRING NARRATIVES (by frequency across all runs):
{narratives_text or 'No narrative data.'}

PUBLICATIONS COVERING CHAINALYSIS (by frequency):
{sources_text or 'No source data.'}

COMPETITOR SHARE OF VOICE (total articles over 30 days):
{competitors_text or 'No competitor data.'}

TOP POLICY DOMAINS BY COVERAGE:
{domains_text or 'No domain data.'}

TOPIC VELOCITY (which domains are accelerating or fading):
{velocity_text or 'No velocity data.'}

SAMPLE CHAINALYSIS MENTION TITLES (for citation quality analysis — determine if Chainalysis is cited as authority/source vs mentioned in passing):
{titles_sample or 'No title data.'}

Provide a strategic monthly briefing in valid JSON (no markdown fences):
{{
  "monthly_headline": "One bold sentence summarising the month for Chainalysis media presence",
  "share_of_voice": "2-3 sentences on our vs competitor media share. Include specific numbers. Who gained ground, who lost it?",
  "narrative_lifecycle": "2-3 sentences on which narratives persisted all month, which emerged late, which faded. What does this tell us about where the industry conversation is heading?",
  "publication_penetration": "2-3 sentences on which outlets covered us most. Are we breaking into mainstream or stuck in crypto press? Any new outlets this month?",
  "earned_vs_published": "1-2 sentences on the balance of earned media vs self-published/blog content across the month",
  "coverage_gaps": "2-3 sentences on topics that dominated industry coverage where we were absent. Where should comms invest?",
  "competitor_strategy": "2-3 sentences reading competitor patterns over 30 days. What are they deliberately positioning on?",
  "topic_velocity_insight": "2-3 sentences on which policy domains are accelerating vs fading in coverage. What should comms prepare for?",
  "citation_quality": "2-3 sentences assessing whether we are being cited as an authoritative source (e.g. 'according to our research') vs mentioned in passing. What proportion is authority positioning vs passive mention?",
  "recommendations": ["Actionable recommendation 1", "Actionable recommendation 2", "Actionable recommendation 3"]
}}"""

    client = get_client(model="claude-haiku-4-5")
    response = client.invoke(prompt)

    try:
        text = response.content
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        result = json.loads(text.strip())
    except (json.JSONDecodeError, IndexError):
        step_ctx.logger.warning("Failed to parse monthly AI response")
        result = {"monthly_headline": "Monthly analysis could not be generated.", "recommendations": []}

    return result


def _safe_slack_section(blocks, text, max_len=2900):
    """Append a mrkdwn section, truncating if needed to stay within Slack limits."""
    if len(text) > max_len:
        text = text[:max_len] + "\n_...truncated_"
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})


# ── Step: Build Monthly Slack Payload ────────────────────────────────

@durable_step
def build_monthly_slack_payload(step_ctx: StepContext, monthly_data: dict, analysis: dict, seizure_stats: dict) -> dict:
    """Build Slack payload for monthly strategic briefing."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%B %Y")
    cats = monthly_data.get("mention_categories", {})
    cat_parts = [f"{v} {k}" for k, v in [("mainstream", cats.get("mainstream",0)), ("crypto", cats.get("crypto",0)), ("fintech", cats.get("fintech",0)), ("blog", cats.get("blog",0)), ("niche", cats.get("niche",0))] if v > 0]
    sent = monthly_data.get("sentiment_totals", {})
    total_comp = sum(c.get("total_articles", 0) for c in monthly_data.get("top_competitors", []))

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"\U0001f4ca Monthly Media Roundup \u2014 {now}", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            "_30-day strategic overview of Chainalysis media presence, competitive positioning, and narrative trends for the comms team._"}},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*{monthly_data.get('total_mentions', 0)}* our mentions ({', '.join(cat_parts)}) \u00b7 "
            f"*{total_comp}* competitor articles \u00b7 "
            f"Sentiment: {sent.get('positive',0)} pos / {sent.get('neutral',0)} neutral / {sent.get('negative',0)} neg"}},
        {"type": "divider"},
    ]

    # Truncate long AI fields to stay within Slack limits
    for field in ["share_of_voice", "narrative_lifecycle", "publication_penetration", 
                  "earned_vs_published", "coverage_gaps", "competitor_strategy",
                  "topic_velocity_insight", "citation_quality"]:
        val = analysis.get(field, "")
        if len(val) > 500:
            analysis[field] = val[:500] + "..."

    # Headline
    _safe_slack_section(blocks, f"*\U0001f4e3 {analysis.get('monthly_headline', '')}*")

    # Share of Voice
    sov = analysis.get("share_of_voice", "")
    if sov:
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*\U0001f4ca Share of Voice*\n{sov}"}})

    # Weekly mention trend
    trend = monthly_data.get("weekly_mention_trend", [])
    if trend:
        bars = ["\u2581","\u2582","\u2583","\u2584","\u2585","\u2586","\u2587","\u2588"]
        max_m = max(w["mentions"] for w in trend) or 1
        sparkline = "".join(bars[min(int(w["mentions"] / max_m * 7), 7)] for w in trend)
        trend_text = "*\U0001f4c8 Weekly Mention Trend*\n"
        trend_text += f"`{sparkline}` "
        trend_text += " \u2192 ".join(f"{w['mentions']}" for w in trend[:6])
        trend_text += f"\n_{trend[0]['week']} to {trend[-1]['week']}_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": trend_text}})

    # Narrative Lifecycle
    nl = analysis.get("narrative_lifecycle", "")
    if nl:
        blocks.append({"type": "divider"})
        text = f"*\U0001f501 Narrative Lifecycle*\n{nl}\n"
        for n in monthly_data.get("top_narratives", [])[:5]:
            text += f"\u2022 *{n['title']}* \u2014 {n['appearances']}x ({n['first_seen']} to {n['last_seen']})\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # Publication Penetration
    pp = analysis.get("publication_penetration", "")
    if pp:
        blocks.append({"type": "divider"})
        text = f"*\U0001f4f0 Publication Penetration*\n{pp}\n"
        for s in monthly_data.get("top_sources", [])[:8]:
            if s.get("url"):
                text += f"\u2022 <{s['url']}|{s['source']}>: *{s['count']}*\n"
            else:
                text += f"\u2022 {s['source']}: *{s['count']}*\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # Earned vs Published
    ep = analysis.get("earned_vs_published", "")
    if ep:
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*\U0001f4dd Earned vs Self-Published*\n{ep}"}})

    # Coverage Gaps
    cg = analysis.get("coverage_gaps", "")
    if cg:
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*\U0001f6a7 Coverage Gaps*\n{cg}"}})

    # Competitor Strategy
    cs = analysis.get("competitor_strategy", "")
    if cs:
        blocks.append({"type": "divider"})
        text = f"*\U0001f3e2 Competitor Strategy Read*\n{cs}\n"
        for c in monthly_data.get("top_competitors", [])[:5]:
            text += f"\u2022 *{c['competitor']}*: {c['total_articles']} articles\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    # Topic Velocity
    tv = analysis.get("topic_velocity_insight", "")
    if tv:
        blocks.append({"type": "divider"})
        text = f"*\U0001f680 Topic Velocity*\n{tv}\n"
        for v in monthly_data.get("topic_velocity", [])[:4]:
            arrow = "\U0001f4c8" if v["direction"] == "accelerating" else ("\U0001f4c9" if v["direction"] == "decelerating" else "\u27a1\ufe0f")
            text += f"{arrow} *{v['domain']}*: {v['total']} articles ({'+' if v['change_pct'] > 0 else ''}{v['change_pct']}%)\n"
        _safe_slack_section(blocks, text)

    # Citation Quality
    cq = analysis.get("citation_quality", "")
    if cq:
        blocks.append({"type": "divider"})
        _safe_slack_section(blocks, f"*\U0001f3c6 Citation Quality*\n{cq}")

    # Recommendations
    recs = analysis.get("recommendations", [])
    if recs:
        blocks.append({"type": "divider"})
        text = "*\U0001f3af Recommendations*\n"
        for i, r in enumerate(recs[:3], 1):
            text += f"{i}. {r[:200]}\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    blocks.append({"type": "divider"})
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
        f"_Chainalysis Monthly Media Roundup \u00b7 {now} \u00b7 {monthly_data.get('runs_analysed', 0)} runs analysed_"}]})

    return {"text": f"Monthly Media Roundup — {now}", "blocks": blocks}


# ── Handler ──────────────────────────────────────────────────────────


# ── Step: Push Draft to GitHub Pages ─────────────────────────────────

@durable_step
def push_draft_to_github(step_ctx: StepContext, mentions: list, narrative: list,
                          competitor_data: dict, ai_analysis: dict, stats: dict,
                          seizure_stats: dict) -> dict:
    """Push weekly briefing as a draft (JSON + HTML template) to GitHub Pages for review."""
    import base64
    from datetime import datetime, timezone
    from chainalysis_skill_github import GitHubClient

    repo = GITHUB_PAGES_REPO
    if not repo:
        step_ctx.logger.info("GITHUB_PAGES_REPO not set — skipping GitHub publish")
        return {"published": False, "reason": "GITHUB_PAGES_REPO not configured"}

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_display = datetime.now(timezone.utc).strftime("%B %d, %Y")
    step_ctx.logger.info(f"Pushing weekly briefing draft to GitHub: {repo}", extra={"date": date_str})

    client = GitHubClient()

    # ── 1. Build structured data for the draft ───────────────────
    narratives = ai_analysis.get("key_narratives", [])
    persistent = [n for n in narratives if n.get("trend", {}).get("cadence") in ("persistent", "recurring")]
    emerging = [n for n in narratives if n.get("trend", {}).get("cadence") in ("new", "sporadic")]
    total_comp = sum(len(a) for a in competitor_data.get("articles", {}).values())

    # Flatten narratives for the template
    flat_narratives = []
    for n in narratives:
        t = n.get("trend", {})
        flat_narratives.append({
            "title": n.get("title", ""),
            "description": n.get("description", ""),
            "relevance": n.get("chainalysis_relevance", ""),
            "cadence": t.get("cadence", "new"),
            "momentum": t.get("momentum", "new"),
            "appearances": t.get("appearances", 1),
            "window_days": t.get("window_days", 1),
            "articles": [{"title": a.get("title", ""), "url": a.get("url", ""), "source": a.get("source", "")}
                         for a in n.get("articles", [])[:3]],
        })

    # Flatten competitors
    flat_comps = []
    comp_entries = sorted(competitor_data.get("articles", {}).items(), key=lambda x: len(x[1]), reverse=True)
    for ck, articles in comp_entries:
        name = articles[0].get("competitor", ck) if articles else competitor_data.get("stats", {}).get(ck, {}).get("name", ck)
        top = articles[0] if articles else {}
        flat_comps.append({
            "name": name, "count": len(articles),
            "top_title": top.get("title", "")[:100] if top else "",
            "top_url": top.get("url", "") if top else "",
            "top_source": top.get("source", "") if top else "",
        })

    # Domain coverage
    domain_cov = stats.get("domain_coverage", {})
    top_domains = sorted(domain_cov.values(), key=lambda d: d["count"], reverse=True)[:8]

    draft_data = {
        "date": date_str,
        "date_display": date_display,
        "mentions_count": stats.get("chainalysis_mentions", 0),
        "narrative_count": stats.get("narrative_relevant", 0),
        "competitor_total": total_comp,
        "persistent_count": len(persistent),
        "articles_analysed": stats.get("total_unique", 0),
        "executive_summary": ai_analysis.get("executive_summary", ""),
        "mentions": [{"title": a["title"], "url": a["url"], "source": a["source"]} for a in mentions[:20]],
        "narratives": flat_narratives,
        "top_domains": [{"name": d["name"], "count": d["count"]} for d in top_domains],
        "competitors": flat_comps,
        "market_landscape": ai_analysis.get("market_landscape", ""),
        "competitor_reads": ai_analysis.get("competitor_reads", {}),
        "seizure_value": seizure_stats.get("chain_involved_value", "") if seizure_stats else "",
        "seizure_cases": seizure_stats.get("chain_involved_cases", "") if seizure_stats else "",
        "mention_categories": stats.get("mention_categories", {}),
        "sentiment": stats.get("sentiment_summary", {}),
    }

    # ── 2. Push the draft data JSON ──────────────────────────────
    data_json = json.dumps(draft_data, indent=2)
    draft_data_path = f"drafts/{date_str}.json"
    encoded_data = base64.b64encode(data_json.encode("utf-8")).decode("utf-8")

    try:
        existing = client.request("GET", f"repos/{repo}/contents/{draft_data_path}")
        sha = existing["sha"]
        client.request("PUT", f"repos/{repo}/contents/{draft_data_path}", json={
            "message": f"Draft data {date_str}", "content": encoded_data, "sha": sha, "branch": "main"})
    except Exception:
        client.request("PUT", f"repos/{repo}/contents/{draft_data_path}", json={
            "message": f"Draft data {date_str}", "content": encoded_data, "branch": "main"})
    step_ctx.logger.info(f"Pushed {draft_data_path}")

    # ── 3. Push the draft HTML (template with embedded data) ─────
    draft_template_path = os.path.join(os.path.dirname(__file__), "draft-template.html")
    with open(draft_template_path, "r") as f:
        template = f.read()

    html = template.replace("{{DATA_JSON}}", data_json)
    html = html.replace("{{REPO}}", repo)
    html = html.replace("{{DATE}}", date_str)

    draft_html_path = f"drafts/{date_str}.html"
    encoded_html = base64.b64encode(html.encode("utf-8")).decode("utf-8")

    try:
        existing = client.request("GET", f"repos/{repo}/contents/{draft_html_path}")
        sha = existing["sha"]
        client.request("PUT", f"repos/{repo}/contents/{draft_html_path}", json={
            "message": f"Draft briefing {date_str}", "content": encoded_html, "sha": sha, "branch": "main"})
    except Exception:
        client.request("PUT", f"repos/{repo}/contents/{draft_html_path}", json={
            "message": f"Draft briefing {date_str}", "content": encoded_html, "branch": "main"})
    step_ctx.logger.info(f"Pushed {draft_html_path}")

    owner = repo.split("/")[0]
    repo_name = repo.split("/")[1] if "/" in repo else repo
    draft_url = f"https://{owner}.github.io/{repo_name}/drafts/{date_str}.html"
    return {"published": True, "draft_url": draft_url, "date": date_str}


def _chunk_mention_blocks(mentions, max_chars=2800):
    """Split mention lines into Slack section blocks that stay under the char limit."""
    header = f"*\U0001f4f0 Our Mentions* ({len(mentions)} total)"
    lines = [f"\u2022 <{a['url']}|{a['title'][:90]}> \u2014 _{a['source']}_" for a in mentions[:15]]
    
    blocks = []
    current = header
    for line in lines:
        if len(current) + len(line) + 1 > max_chars:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": current}})
            current = line
        else:
            current += "\n" + line
    if current:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": current}})
    return blocks


@chainalysis_workflow
def handler(event: dict, context: DurableContext) -> dict:
    """Media Monitor — daily or weekly digest."""
    mode = event.get("mode", "daily")
    days_back = event.get("days_back", 30 if mode == "monthly" else (7 if mode == "weekly" else 1))

    # ── Monthly mode: aggregate past runs, no live fetch ─────────
    if mode == "monthly":
        context.logger.info("Monthly roundup starting")
        monthly_data = context.step(load_monthly_data(), name="load_monthly_data")
        if not monthly_data:
            return {"status": "no_data", "mode": "monthly"}
        analysis = context.step(monthly_analysis(monthly_data), name="monthly_analysis")
        slack_payload = context.step(build_monthly_slack_payload(monthly_data, analysis, {}), name="build_monthly_slack")
        webhook_key = event.get("webhook_key", "")
        slack_result = context.step(send_to_slack(slack_payload, "monthly", webhook_key), name="send_slack")
        return {
            "status": "completed", "mode": "monthly",
            "monthly_data": {
                "total_mentions": monthly_data.get("total_mentions", 0),
                "runs_analysed": monthly_data.get("runs_analysed", 0),
            },
            "analysis": analysis,
            "slack_delivery": slack_result,
        }
    include_slack = event.get("include_slack", True)
    include_competitors = event.get("include_competitors", True)
    include_email = event.get("include_email", True)
    webhook_key = event.get("webhook_key", "")  # Override webhook: e.g. "APAC" → SLACK_WEBHOOK_URL_APAC

    context.logger.info("Media Monitor starting", extra={"mode": mode, "days_back": days_back})

    classified = context.step(fetch_and_classify(days_back), name="fetch_classify")
    mentions, narrative, stats = classified["mentions"], classified["narrative"], classified["stats"]
    feed_stats, publications, pub_count = classified["feed_stats"], classified["publications"], classified["publication_count"]

    competitor_data = {"articles": {}, "stats": {}}
    if include_competitors:
        competitor_data = context.step(fetch_competitors(days_back), name="fetch_competitors")

    total_comp = sum(len(a) for a in competitor_data.get("articles", {}).values())
    if len(mentions) + len(narrative) == 0 and total_comp == 0:
        return {"status": "no_articles", "mode": mode, "period_days": days_back, "feed_stats": feed_stats, "classification_stats": stats}

    trend_lookback = event.get("trend_lookback_days", 30 if mode == "weekly" else 14)
    history = context.step(load_narrative_history(trend_lookback), name="load_history")

    ai_result = context.step(analyze_and_detect_trends(mentions, narrative, competitor_data, history, days_back, mode), name="ai_analysis")

    enriched = context.step(enrich_narratives(ai_result.get("key_narratives", []), mentions + narrative), name="enrich_narratives")
    ai_result["key_narratives"] = enriched

    # ── Weekly-only data enrichment ──────────────────────────────
    seizure_stats = {}
    if mode == "weekly":
        seizure_stats = context.step(fetch_seizure_stats(), name="fetch_seizure_stats")

    # ── Compute WoW delta from history ──────────────────────────
    if history:
        last = history[0].get("stats", {})
        stats["wow_delta"] = {
            "mentions_delta": stats.get("chainalysis_mentions", 0) - last.get("mentions", 0),
            "narrative_delta": stats.get("narrative_relevant", 0) - last.get("narrative", 0),
            "prev_mentions": last.get("mentions", 0),
        }

    # ── Sentiment summary from AI ────────────────────────────────
    sent_str = ai_result.get("sentiment_breakdown", "")
    sent_counts = {"positive": 0, "neutral": 0, "negative": 0}
    if sent_str:
        import re
        for match in re.finditer(r"(\d+)\s*(positive|neutral|negative)", sent_str.lower()):
            sent_counts[match.group(2)] = int(match.group(1))
    stats["sentiment_summary"] = sent_counts
    stats["unhealthy_feeds"] = classified.get("unhealthy_feeds", [])

    output = {
        "status": "completed", "mode": mode, "period_days": days_back,
        "classification_stats": stats, "feed_stats": feed_stats,
        "google_news_publications": publications, "google_news_publication_count": pub_count,
        "executive_summary": ai_result.get("executive_summary", ""),
        "key_narratives": enriched,
        "chainalysis_mentions": [{"title": a["title"][:120], "url": a["url"], "source": a["source"], "published": a.get("published", "")} for a in mentions[:20]],
        "narrative_coverage": [{"title": a["title"][:120], "url": a["url"], "source": a["source"],
                                "domains": [d["name"] for d in a.get("matched_domains", [])[:3]]} for a in narrative[:20]],
        "action_items": ai_result.get("action_items", []),
        "coverage_analysis": ai_result.get("coverage_analysis", ""),
    }

    if mode == "weekly":
        output["seizure_scorecard"] = seizure_stats
        output["competitor_reads"] = ai_result.get("competitor_reads", {})
        output["market_landscape"] = ai_result.get("market_landscape", "")

    if include_competitors:
        output["competitor_intelligence"] = {
            "summary": ai_result.get("coverage_analysis", ""),
            "highlights": [{"competitor": arts[0].get("competitor", k), "count": len(arts), "top": arts[0]["title"]}
                           for k, arts in competitor_data.get("articles", {}).items() if arts],
            "stats": competitor_data.get("stats", {})}

    if include_email:
        if mode == "weekly":
            pass  # Weekly email is now generated from the edited draft on Approve (via publish_briefing.py)
        else:
            context.step(generate_email_digest(mentions, narrative, competitor_data, ai_result, stats, days_back), name="generate_email")
        output["email_digest_generated"] = True

    if include_slack:
        if mode == "weekly":
            # Weekly: send a short "draft ready" notification — full Slack is sent on Approve
            from datetime import datetime as _dt
            _now = _dt.now(__import__("datetime").timezone.utc)
            _date_str = _now.strftime("%Y-%m-%d")
            _now_fmt = _now.strftime("%B %d, %Y")
            gh_repo = GITHUB_PAGES_REPO
            if gh_repo:
                _owner = gh_repo.split("/")[0]
                _repo_name = gh_repo.split("/")[1] if "/" in gh_repo else gh_repo
                _draft_url = f"https://{_owner}.github.io/{_repo_name}/drafts/{_date_str}.html"
            else:
                _draft_url = ""
            draft_notify = {
                "text": f"Weekly Briefing draft ready for review ({_now_fmt})",
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "\U0001f4dd Weekly Briefing Draft Ready", "emoji": True}},
                    {"type": "section", "text": {"type": "mrkdwn", "text":
                        "_Weekly strategic overview of our media presence, competitive positioning, and emerging industry narratives. Review the draft, make edits, then approve to publish._"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text":
                        f"*{stats.get('chainalysis_mentions',0)}* mentions \u00b7 "
                        f"*{stats.get('narrative_relevant',0)}* narrative \u00b7 "
                        f"*{sum(len(a) for a in competitor_data.get('articles', {}).values())}* competitor \u00b7 "
                        f"_{_now_fmt}_"}},
                    {"type": "divider"},
                    {"type": "section", "text": {"type": "mrkdwn", "text":
                        f"*\U0001f4cb Executive Summary*\n{ai_result.get('executive_summary', '')}"}},
                ]
                # ── Our Mentions (chunked to stay under Slack 3000 char limit)
                + ([{"type": "divider"}] + _chunk_mention_blocks(mentions) if mentions else [])
                # ── Competitor Watch ─────────────────────────────
                + ([{"type": "divider"},
                    {"type": "section", "text": {"type": "mrkdwn", "text":
                        "*\U0001f3e2 Competitor Watch* (" + str(sum(len(a) for a in competitor_data.get('articles', {}).values())) + " total)\n"
                        + "".join(
                            f"\u2022 *{arts[0].get('competitor', ck)}* ({len(arts)}): <{arts[0]['url']}|{arts[0]['title'][:70]}>\n"
                            for ck, arts in competitor_data.get("articles", {}).items() if arts)
                    }}] if sum(len(a) for a in competitor_data.get('articles', {}).values()) > 0 else [])
                # ── Review button ────────────────────────────────
                + [{"type": "divider"}]
                + ([{"type": "actions", "elements": [
                    {"type": "button", "text": {"type": "plain_text", "text": "\u270f\ufe0f Review & Edit Draft", "emoji": True},
                     "url": _draft_url, "style": "primary"},
                    {"type": "button", "text": {"type": "plain_text", "text": "\U0001f4ca Share of Voice Dashboard", "emoji": True},
                     "url": f"https://{_owner}.github.io/{_repo_name}/internal/dashboard.html"},
                ]}] if _draft_url else []) + [
                    {"type": "context", "elements": [{"type": "mrkdwn", "text":
                        "_Review the draft, make edits, then hit Approve to publish to the website and send the final version to the main channel._"}]},
                ],
            }
            output["slack_delivery"] = context.step(send_to_slack(draft_notify, mode, webhook_key), name="send_slack")
        else:
            # Daily: send the full Slack digest as before
            slack_payload = context.step(build_slack_payload(mentions, narrative, competitor_data, ai_result, stats, days_back, mode, seizure_stats), name="build_slack")
            output["slack_delivery"] = context.step(send_to_slack(slack_payload, mode, webhook_key), name="send_slack")

    # ── Publish to GitHub Pages (weekly only) ────────────────────
    if mode == "weekly":
        gh_result = context.step(push_draft_to_github(mentions, narrative, competitor_data, ai_result, stats, seizure_stats), name="push_draft_github")
        output["github_pages"] = gh_result

    context.logger.info("Media Monitor complete", extra={"mentions": len(mentions), "narrative": len(narrative), "competitors": total_comp})
    return output
