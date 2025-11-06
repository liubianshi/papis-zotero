#!/usr/bin/env python3
"""
Script to import web pages into Papis by fetching, parsing, and converting to various formats.

This module provides functionality to scrape web pages, simplify their content,
and integrate them into a Papis library with metadata, PDF, and Markdown versions.
"""

import html
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from datetime import datetime
from typing import Optional

import requests

import papis.database
import papis.document
import papis.logging
from papis.commands.add import run as doc_add
from papis.commands.addto import run as addto

logger = papis.logging.get_logger(__name__)

try:
    from weasyprint import HTML
except ImportError:
    logger.error("未找到 'weasyprint' 库。请安装: pip install weasyprint")
    sys.exit(1)


def check_dependencies():
    """
    Check if all required CLI tools are installed.

    Returns:
        bool: True if all dependencies are available, False otherwise.
    """
    tools = ["mercury-parser", "pandoc"]
    missing = [tool for tool in tools if not shutil.which(tool)]
    if missing:
        for tool in missing:
            error_info = f"错误: 依赖项 '{tool}' 未在 PATH 中找到。"
            logger.error(error_info)
        if "mercury-parser" in missing:
            logger.error("请运行: npm install -g @postlight/parser")
        return False
    return True


def get_html_header(title):
    """
    Generate the HTML header with escaped title and predefined CSS styles.

    Args:
        title (str): The title of the document to be escaped and included.

    Returns:
        str: The complete HTML header string.
    """
    safe_title = html.escape(title)
    return f"""
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
          font-family: 'FiraCode Nerd Font Mono', 'Fira Code', 'Source Code pro', Monaco, 'Noto Mono', Consolas, 'Courier New','LXGW WenKai Mono', monospace !important;
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


def process_url_main(url):
    """
    Main process to fetch, parse, and import a web page into Papis.

    This function performs the following steps:
    1. Fetch the raw HTML from the URL.
    2. Parse metadata and content using Mercury Parser.
    3. Generate simplified HTML.
    4. Create a Papis document entry.
    5. Convert to PDF and Markdown, then attach to the document.

    Args:
        url (str): The URL of the web page to process.
    """
    temp_dir = tempfile.mkdtemp(prefix="papis_importer_")
    print(f"Temporary Workspace: {temp_dir}")

    file_raw_html = os.path.join(temp_dir, "raw.html")
    file_simp_html = os.path.join(temp_dir, "simplified.html")
    file_simp_pdf = os.path.join(temp_dir, "simplified.pdf")
    file_simp_md = os.path.join(temp_dir, "simplified.md")

    try:
        # Step 1: Fetch raw HTML
        print(f"1/5: 正在抓取 {url} ...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        raw_html_content = response.text

        with open(file_raw_html, "w", encoding="utf-8") as f:
            f.write(raw_html_content)

        # Step 2: Parse with Mercury Parser
        print("2/5: 正在解析元数据和内容...")
        cmd = ["mercury-parser", url]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding="utf-8")
        mercury_data = json.loads(result.stdout)

        # Step 3: Generate simplified HTML
        print("3/5: 正在生成简化的 HTML...")
        title = mercury_data.get("title", "无标题")
        content_html = mercury_data.get("content", "<p>无法提取内容。</p>")
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
        with open(file_simp_html, "w", encoding="utf-8") as f:
            f.write(full_html)

        # Step 4: Create Papis document
        print("4/5: 正在创建 Papis 条目...")
        metadata = {
            "url": url,
            "title": title,
            "author": mercury_data.get("author", "N.A."),
            "date": mercury_data.get("date_published", ""),
            "abstract": mercury_data.get("excerpt", ""),
            "tags": ["from-web", "auto-imported"],
            "add-date": download_date
        }
        key_name = papis.id.key_name()
        tmp_doc = papis.document.from_data(metadata)
        metadata[key_name] = papis.id.compute_an_id(tmp_doc, "\n")
        doc_add([file_raw_html, file_simp_html], metadata, link=False)

        doc_id = metadata[key_name]
        print(f"  ✅ 成功创建条目: {doc_id}")

        # Retrieve the document for further processing
        db = papis.database.get()
        doc = db.find_by_id(doc_id)
        if not doc:
            logger.error("错误: 在后台任务中未找到 doc_id")
            return

        # Step 5: Generate PDF
        print("5/5: 进行 PDF 转换...")
        with open(file_simp_html, encoding="utf-8") as f:
            simp_html_content = f.read()
        HTML(string=simp_html_content, base_url=url).write_pdf(file_simp_pdf)
        addto(doc, [file_simp_pdf])

        # Step 6: Generate Markdown
        print("6/6: 进行 Markdown 转换...")
        re = subprocess.run(
            ["pandoc", file_simp_html, "-f", "html", "-t", "markdown", "-o", file_simp_md],
            check=True,
            capture_output=True,
            encoding="utf-8",
            text=True
        )
        addto(doc, [file_simp_md])

        # Save document changes
        doc.save()
        logger.info("✅ 主任务完成。PDF 和 Markdown 将在后台附加。")

    except requests.RequestException as e:
        print(f"  错误: 无法获取原始 HTML: {e}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  错误: 子进程失败: {e.stderr}", file=sys.stderr)
    except Exception as e:
        print(f"  错误: {e}", file=sys.stderr)
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)


def add_from_url(url: str, out_folder: Optional[str] = None, link: bool = False) -> None:
    """
    Entry point to add a document from a URL to Papis.

    Checks dependencies, sets the library if specified, and processes the URL.

    Args:
        url (str): The URL to import.
        out_folder (Optional[str]): The Papis library name to use.
        link (bool): Whether to link files (unused in current implementation).
    """
    if not check_dependencies():
        sys.exit(1)

    if out_folder is not None:
        papis.config.set_lib_from_name(out_folder)

    process_url_main(url)
