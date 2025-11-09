#!/usr/bin/env python3
"""
Script to import web pages into Papis by fetching, parsing, and converting to various formats.

This module provides functionality to scrape web pages, simplify their content,
and integrate them into a Papis library with metadata, PDF, and Markdown versions.
"""

import hashlib
import html
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from markdownify import markdownify

import papis.config
import papis.database
import papis.document
import papis.id
import papis.logging
from papis.commands.add import run as doc_add
from papis.commands.addto import run as addto
from papis.commands.rm import run as doc_remove

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

logger = papis.logging.get_logger(__name__)

# Constants
REQUEST_TIMEOUT = 10
MAX_LINE_WIDTH = 75
DEFAULT_EDITOR = "vim"
FROM_WEB_TAG = "from-web"
AUTO_IMPORTED_TAG = "auto-imported"

# --- Auto-detect optional dependencies ---

try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False
    logger.info("WeasyPrint not found. Skipping PDF generation.")
    logger.info("For PDF functionality, run: pip install weasyprint")

# Check if mercury-parser is available in PATH
MERCURY_AVAILABLE = shutil.which("mercury-parser") is not None
if not MERCURY_AVAILABLE:
    logger.info("mercury-parser not found. Falling back to Python parser.")
    logger.info("For best parsing, install: npm install -g @postlight/parser")
    logger.info("Fallback requires: pip install readability-lxml beautifulsoup4 lxml")

HTML_HEADER_TEMPLATE = """
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta charset="UTF-8">
  <title>{safe_title}</title>
  <style type="text/css">
      html {{ font-size: 8pt; }}
      body {{
          margin: 3rem auto; max-width: 75rem; padding: 8rem 5rem;
          border-radius:1.5rem; background-color: white;
          font-family: 'LXGW WenKai', 'Source Han Sans CN', 'Noto Sans CJK SC', sans-serif;
          font-size: 1.5rem; letter-spacing: 0.05ex;
      }}
      p {{ padding-top: 0.8rem; font-size: 1.5rem !important; line-height: 3.2rem; }}
      h1, h2, h3 {{ font-size: 2.5rem; line-height: 3rem; padding-top: 3rem; }}
      h1.reader-title {{
          font-size: 3.2rem; line-height: 3.2em; width: 100%;
          padding-top: 6rem; margin: 0 0;
      }}
      a {{ color: #805020; }}
      img {{ max-width:100%; height:auto; }}
      table, th, td {{
          border: 0.1rem solid grey; border-collapse: collapse;
          padding: 0.6rem; vertical-align: top;
      }}
      pre {{
          padding: 1.6rem; overflow: auto; line-height: 2rem;
          background-color: #fdfefb;
      }}
      pre, code {{
          font-family: 'Fira Code', Monaco, 'Noto Mono', Consolas, 'LXGW WenKai Mono', monospace !important;
          font-size: 1.5rem;
      }}
      li {{ line-height: 3.2rem; }}
      blockquote {{
          border-inline-start: 0.2rem solid grey !important; padding: 0rem;
          padding-inline-start: 1.6rem; margin-inline-start: 2.4rem;
          border-radius: 0.5rem;
      }}
  </style>
  <script type="text/javascript" async
      src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/MathJax.js?config=TeX-MML-AM_CHTML">
  </script>
</head>
"""
USER_AGENT = " ".join([
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "AppleWebKit/537.36 (KHTML, like Gecko)",
    "Chrome/58.0.3029.110 Safari/537.36"
])


def get_html_header(title: str) -> str:
    """
    Generate the HTML header with escaped title and predefined CSS styles.

    Args:
        title (str): The title of the web page.

    Returns:
        str: The HTML header string.
    """
    safe_title = html.escape(title)
    return HTML_HEADER_TEMPLATE.format(safe_title=safe_title)


def _generate_id_from_url(url: str) -> str:
    """
    Generate a stable, unique ID from a URL using MD5 hash.
    The URL is normalized before hashing to handle case and Unicode encoding differences.

    Args:
        url (str): The original URL.

    Returns:
        str: The 32-character MD5 hash of the URL.
    """
    try:
        # Parse the URL into components
        parsed = urllib.parse.urlparse(url)

        # Normalize case for scheme and netloc (case-insensitive)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Normalize encoding to handle Unicode and percent-encoding
        # Unquote then quote to standardize representations
        path = urllib.parse.quote(urllib.parse.unquote(parsed.path), safe="/:@")
        query = urllib.parse.quote_plus(urllib.parse.unquote_plus(parsed.query), safe="&=")
        fragment = urllib.parse.quote(urllib.parse.unquote(parsed.fragment), safe="")

        # Reconstruct the canonical URL
        canonical_url = urllib.parse.urlunparse((scheme, netloc, path, parsed.params, query, fragment,))
    except ValueError:
        # Fallback for malformed URLs: convert to lowercase
        canonical_url = url.lower()

    # Compute MD5 hash of the canonical URL
    hash_object = hashlib.md5(canonical_url.encode("utf-8"))
    return hash_object.hexdigest()


def _fetch_raw_html(url: str, file_path: str) -> str:
    """
    Fetch raw HTML from the URL and save it to a file.

    This function sends an HTTP GET request to the specified URL with a custom User-Agent,
    retrieves the HTML content, and saves it to the given file path for further processing.

    Args:
        url (str): The URL to fetch.
        file_path (str): Path to save the raw HTML.

    Returns:
        str: The raw HTML content.
    """
    logger.info("1/5: Fetching %s ...", url)
    headers = {
        "User-Agent": USER_AGENT,
    }
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    raw_html_content = response.text

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(raw_html_content)
    return raw_html_content


def _parse_with_mercury(url: str) -> Dict[str, Any]:
    """
    Parse content using mercury-parser CLI.

    This function uses the mercury-parser command-line tool to extract structured
    metadata and content from the web page URL.

    Args:
        url (str): The URL to parse.

    Returns:
        Dict[str, Any]: Parsed metadata and content.
    """
    cmd = ["mercury-parser", url]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        encoding="utf-8"
    )
    return json.loads(result.stdout)


def _parse_with_python(html_content: str) -> Dict[str, Any]:
    """
    Parse content using readability-lxml and BeautifulSoup as fallback.

    This function attempts to extract title, content, author, date, and excerpt
    from the HTML using readability and BeautifulSoup libraries.

    Args:
        html_content (str): The HTML content to parse.

    Returns:
        Dict[str, Any]: Parsed metadata and content.
    """
    try:
        from bs4 import BeautifulSoup
        from readability import Document
    except ImportError:
        logger.error("Fallback parsing failed: missing 'readability-lxml' or 'beautifulsoup4'/'lxml'.")
        logger.error("Run: pip install readability-lxml beautifulsoup4 lxml")
        return {
            "title": "Parsing failed (missing libraries)",
            "content": (
                "<p>Missing Python parsing libraries. Please install readability-lxml,"
                "beautifulsoup4, and lxml.</p>"),
            "author": "N.A.",
            "date_published": "",
            "excerpt": ""
        }

    doc = Document(html_content)
    soup = BeautifulSoup(html_content, "lxml")

    data = {}
    data["title"] = doc.title()
    data["content"] = doc.summary(html_partial=True)

    author_tag = soup.find("meta", attrs={"name": "author"})
    data["author"] = author_tag.get("content") if author_tag else "N.A."

    date_tag = soup.find("meta", property="article:published_time")
    data["date_published"] = date_tag.get("content") if date_tag else ""

    excerpt_tag = soup.find("meta", attrs={"name": "description"})
    data["excerpt"] = excerpt_tag.get("content") if excerpt_tag else ""

    return data


def _parse_content(url: str, html_content: str) -> Dict[str, Any]:
    """
    Intelligently parse content, preferring mercury-parser.

    This function chooses the best available parser: mercury-parser if available,
    otherwise falls back to Python-based parsing with readability.

    Args:
        url (str): The URL.
        html_content (str): The HTML content.

    Returns:
        Dict[str, Any]: Parsed data.
    """
    logger.info("2/5: Parsing metadata and content...")
    if MERCURY_AVAILABLE:
        logger.info("...using 'mercury-parser' (recommended).")
        return _parse_with_mercury(url)
    else:
        logger.info("...using 'readability-lxml' (fallback).")
        return _parse_with_python(html_content)


def _generate_simplified_html(mercury_data: Dict[str, Any], url: str, file_path: str) -> str:
    """
    Generate simplified HTML with custom CSS.

    This function creates a clean HTML version of the parsed content,
    embedding metadata like title, URL, and download date at the bottom.

    Args:
        mercury_data (Dict[str, Any]): Parsed data.
        url (str): The URL.
        file_path (str): Path to save the HTML.

    Returns:
        str: The generated HTML content.
    """
    logger.info("3/5: Generating simplified HTML...")
    title = mercury_data.get("title", "No title")
    content_html = mercury_data.get("content", "<p>Unable to extract content.</p>")
    url_decoded = urllib.parse.unquote(url)
    download_date = datetime.now().strftime("%Y-%m-%d")

    header_html = get_html_header(title)
    full_html = f"""
<!DOCTYPE html>
<html>
  {header_html}
  <body class="qute-readability">
    <h1 class="reader-title">{html.escape(title)}</h1>
    <hr>
    {content_html}
    <hr />
    <p>TITLE:</p>
    <ul> <li>{html.escape(title)}</li> </ul>
    <p>URL:</p>
    <ul> <li><a href="{html.escape(url)}">{html.escape(url_decoded)}</a></li> </ul>
    <p>DOWNLOAD_DATE</p>
    <ul> <li>{download_date}</li> </ul>
  </body>
</html>
"""
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(full_html)
    return full_html


def _edit_metadata_with_editor(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Edit metadata using an external editor.

    This function allows users to modify metadata by opening it in an external
    editor (like vim) via a temporary YAML file. It handles file creation,
    editing, validation, and cleanup.

    Args:
        metadata (Dict[str, Any]): The current metadata to edit.

    Returns:
        Optional[Dict[str, Any]]: Updated metadata if successful, None otherwise.
    """
    if not YAML_AVAILABLE:
        logger.error("Error: Editing requires 'PyYAML' library. Run: pip install PyYAML")
        return None

    editor = os.environ.get("EDITOR", DEFAULT_EDITOR)
    temp_path = ""
    result: Optional[Dict[str, Any]] = None

    try:
        # Create temporary YAML file with metadata
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".yaml", encoding="utf-8") as temp_file:
            yaml.dump(metadata, temp_file, allow_unicode=True, sort_keys=False)
            temp_path = temp_file.name

        mtime_before = os.path.getmtime(temp_path)

        # Launch external editor
        subprocess.run([editor, temp_path], check=False)

        # Check if file was modified
        mtime_after = os.path.getmtime(temp_path)
        if mtime_after <= mtime_before:
            logger.info("No changes detected.")
            result = None
        else:
            # Load and validate new metadata
            with open(temp_path, encoding="utf-8") as f:
                new_metadata = yaml.safe_load(f)

                if not new_metadata:
                    logger.warning("Warning: File is empty or invalid. No changes applied.")
                    result = None
                else:
                    logger.info("Metadata updated.")
                    result = new_metadata

    except FileNotFoundError:
        logger.error("Error: Editor '%s' not found.", editor)
        logger.info("Please set your EDITOR environment variable.")
        result = None
    except yaml.YAMLError as e:
        logger.error("Error: YAML parsing failed: %s. No changes applied.", e)
        result = None
    except Exception as e:
        logger.error("Error during editing: %s", e)
        result = None
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    return result


def _print_metadata_and_ask(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Print metadata, ask for confirmation, and allow editing.

    This function iteratively displays the metadata, prompts the user for
    confirmation (y/n) or editing (e), and handles user input accordingly.
    It exits if the user chooses to abort.

    Args:
        metadata (Dict[str, Any]): Initial metadata to confirm or edit.

    Returns:
        Dict[str, Any]: Confirmed (and possibly edited) metadata.

    Raises:
        SystemExit: If user chooses to abort.
    """
    while True:
        # Print current metadata
        logger.info("\n--- Metadata to Import ---")
        for key, value in metadata.items():
            if isinstance(value, list) and value:
                logger.info("%s:", key)
                for item in value:
                    logger.info("  - %s", item)
            else:
                logger.info("%s: %s", key, value)
        logger.info("--------------------------")

        # Build prompt
        prompt = "Import? (y/n"
        if YAML_AVAILABLE:
            prompt += "/e[dit]"
        prompt += "): "

        choice = input(prompt).strip().lower()

        # Handle choices
        if choice == "y":
            return metadata
        elif choice == "n":
            logger.info("Aborting.")
            sys.exit(0)
        elif choice == "e" and YAML_AVAILABLE:
            new_metadata = _edit_metadata_with_editor(metadata)
            if new_metadata:
                metadata = new_metadata
            continue
        else:
            logger.warning("Invalid choice. Enter 'y', 'n', or 'e'.")


def _create_papis_document(url: str, mercury_data: Dict[str, Any], files_to_add: list, link: bool,
                           doc_id: str) -> tuple[str, Optional[papis.document.Document]]:
    """
    Create a Papis document entry.

    This function generates initial metadata from parsed data, allows user
    confirmation and editing, then adds the document to the Papis database
    with specified files.

    Args:
        url (str): The URL.
        mercury_data (Dict[str, Any]): Parsed data.
        files_to_add (list): Files to add to the document.
        link (bool): Whether to link or copy files.
        doc_id (str): The pre-computed document ID to use.

    Returns:
        tuple: Document ID and document object.
    """
    logger.info("4/5: Creating Papis entry...")
    metadata = {
        "url": url,
        "title": mercury_data.get("title", "No title"),
        "author": mercury_data.get("author", "N.A."),
        "date": mercury_data.get("date_published", ""),
        "abstract": mercury_data.get("excerpt", ""),
        "tags": [FROM_WEB_TAG, AUTO_IMPORTED_TAG],
        "add-date": datetime.now().strftime("%Y-%m-%d")
    }
    key_name = papis.id.key_name()
    metadata[key_name] = doc_id

    metadata = _print_metadata_and_ask(metadata)
    # Ensure ID persists after editing
    metadata[key_name] = doc_id

    doc_add(files_to_add, metadata, link=link)
    logger.info("  ✅ Successfully created entry: %s", doc_id)

    db = papis.database.get()
    doc = db.find_by_id(doc_id)

    return doc_id, doc


def _generate_pdf(doc: papis.document.Document, html_content: str, pdf_path: str, base_url: str) -> None:
    """
    Generate PDF from HTML and add to Papis document.

    This function uses WeasyPrint to convert HTML to PDF if available,
    then adds the PDF to the Papis document.

    Args:
        doc (papis.document.Document): The Papis document.
        html_content (str): The HTML content.
        pdf_path (str): Path to save the PDF.
        base_url (str): Base URL for relative links.
    """
    if not WEASYPRINT_AVAILABLE:
        logger.info("5/6: Skipping PDF generation (missing 'weasyprint').")
        return

    logger.info("5/6: Converting to PDF...")
    try:
        HTML(string=html_content, base_url=base_url).write_pdf(pdf_path)
        addto(doc, [pdf_path])
        logger.info("  ...PDF added successfully.")
    except Exception as e:
        logger.warning("PDF generation failed: %s. Skipping PDF.", e)


def _generate_markdown(doc: papis.document.Document, html_content: str, md_path: str) -> None:
    """
    Generate Markdown from HTML and add to Papis document.

    This function converts HTML to Markdown using markdownify, optionally
    cleaning with BeautifulSoup, and adds the Markdown file to the Papis document.

    Args:
        doc (papis.document.Document): The Papis document.
        html_content (str): The HTML content.
        md_path (str): Path to save the Markdown.
    """
    logger.info("6/6: Converting to Markdown...")

    html_to_convert = html_content

    try:
        from bs4 import BeautifulSoup

        # Remove script and style tags as they are useless in markdown
        soup = BeautifulSoup(html_content, "lxml")
        for s in soup(["script", "style"]):
            s.decompose()

        # Convert only body if present, else whole
        html_to_convert = str(soup.body) if soup.body else str(soup)

    except ImportError:
        logger.warning("Missing 'beautifulsoup4'/'lxml'. Markdown may include unnecessary <script> tags.")
        logger.info("Run: pip install beautifulsoup4 lxml")

    try:
        # Convert and save
        simp_md_content = markdownify(html_to_convert, heading_style="ATX")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(simp_md_content)

        # Add to Papis
        addto(doc, [md_path])
        logger.info("  ...Markdown added successfully.")
    except Exception as e:
        logger.warning("Markdown generation failed: %s. Skipping Markdown.", e)


def process_url_main(url: str, link: bool, precomputed_id: str) -> None:
    """
    Main process to fetch, parse, and import a web page into Papis.

    This function orchestrates the entire workflow: fetching HTML, parsing content,
    generating simplified HTML, creating the Papis document, and optionally
    generating PDF and Markdown versions, with proper cleanup.

    Args:
        url (str): The URL to process.
        link (bool): Whether to link files instead of copying.
        precomputed_id (str): The pre-computed ID to assign to the document.
    """
    temp_dir = tempfile.mkdtemp(prefix="papis_importer_")
    logger.info("Temporary Workspace: %s", temp_dir)

    # Define all temporary file paths
    file_raw_html = os.path.join(temp_dir, "raw.html")
    file_simp_html = os.path.join(temp_dir, "simplified.html")
    file_simp_pdf = os.path.join(temp_dir, "simplified.pdf")
    file_simp_md = os.path.join(temp_dir, "simplified.md")

    try:
        # Step 1: Fetch
        raw_html_content = _fetch_raw_html(url, file_raw_html)

        # Step 2: Parse
        mercury_data = _parse_content(url, raw_html_content)

        # Step 3: Generate Simplified HTML
        simp_html_content = _generate_simplified_html(mercury_data, url, file_simp_html)

        # Step 4: Create Papis Document
        doc_id, doc = _create_papis_document(
            url,
            mercury_data,
            [file_raw_html, file_simp_html],
            link,
     precomputed_id
        )
        if doc is None:
            logger.warning("Warning: Cannot find document with papis_id %s in database", doc_id)
            shutil.rmtree(temp_dir, ignore_errors=True)
            return

        # Step 5: Generate PDF (optional)
        _generate_pdf(doc, simp_html_content, file_simp_pdf, base_url=url)

        # Step 6: Generate Markdown (optional)
        _generate_markdown(doc, simp_html_content, file_simp_md)

        # Final Save
        doc.save()
        logger.info("✅ All tasks completed: %s", doc_id)

    except requests.RequestException as e:
        logger.error("Error: Unable to fetch raw HTML: %s", e)
    except subprocess.CalledProcessError as e:
        logger.error("Error: Subprocess failed (likely 'mercury-parser'): %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        # Clean up temporary directory
        logger.info("Cleaning up temp directory: %s", temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)


def add_from_url(url: str, out_folder: Optional[str] = None, link: Optional[bool] = False) -> None:
    """
    Entry point to add a document from a URL.

    This function sets the Papis library if specified and initiates the main processing.

    Args:
        url (str): The URL to add.
        out_folder (Optional[str]): The output folder/library name.
        link (Optional[bool]): Whether to link files.
    """
    if out_folder is not None:
        papis.config.set_lib_from_name(out_folder)

    # 1. Load database
    db = papis.database.get()

    # 2. Generate predictable ID from URL
    precomputed_id = _generate_id_from_url(url)
    logger.info("Generated potential ID: %s", precomputed_id)

    # 3. Check if document already exists
    existing_doc = db.find_by_id(precomputed_id)

    if existing_doc:
        logger.warning("Entry %s already exists.", precomputed_id)
        choice = input("Do you want to force update (this will delete the old entry)? (y/n): ").strip().lower()

        if choice == "y":
            logger.info("Deleting old entry: %s...", precomputed_id)
            doc_remove(existing_doc)
            logger.info("Old entry deleted.")
        else:
            logger.info("Aborting.")
            sys.exit(0)

    process_url_main(url, link, precomputed_id)


# --- Main executor (if script is run directly) ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python %s <URL> [library_name]", sys.argv[0])
        sys.exit(1)

    url_to_add = sys.argv[1]
    lib_name = sys.argv[2] if len(sys.argv) > 2 else None

    add_from_url(url_to_add, out_folder=lib_name)
