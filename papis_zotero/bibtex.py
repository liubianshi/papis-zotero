import os
import re
from typing import Any, Dict, List, Optional

import papis.bibtex
import papis.commands.add
import papis.config
import papis.logging

logger = papis.logging.get_logger(__name__)

RE_SEPARATOR = re.compile(r"\s*,\s*")


def add_from_bibtex(bib_file: str,
                    out_folder: Optional[str] = None,
                    link: bool = False) -> None:
    """
    Add entries from a BibTeX file to the Papis library.

    This function parses a BibTeX file, processes each entry by cleaning up
    fields like date, tags, and ref, handles associated files, and adds them
    to the library. If there's only one entry, it also includes all files
    from the BibTeX file's directory.

    Args:
        bib_file (str): Path to the BibTeX file.
        out_folder (Optional[str]): Name of the output library folder. If provided,
            sets the library to this folder.
        link (bool): Whether to link files instead of copying them.

      Returns:
          None
      """
    if out_folder is not None:
        papis.config.set_lib_from_name(out_folder)

    entries = papis.bibtex.bibtex_to_dict(bib_file)
    nentries = len(entries)
    for i, entry in enumerate(entries):
        result: Dict[str, Any] = entry.copy()

        # Clean up date field: extract year and month
        _process_date(result)

        # Clean up keywords into tags
        _process_keywords(result)

        # Clean up or create reference
        _process_reference(result)

        # Process associated files
        files = _process_files(result, bib_file)

        # If only one entry, add all files from the BibTeX file's directory
        if nentries == 1:
            files = _add_directory_files(files, bib_file)

        # Add to library
        logger.info("[%4d/%-4d] Exporting item with ref '%s'.",
                    i + 1, nentries, result["ref"])

        papis.commands.add.run(files, data=result, link=link, confirm=False)


def _process_date(result: Dict[str, Any]) -> None:
    """Extract year and month from date field."""
    if "date" in result:
        date_parts = str(result.pop("date")).split("-")
        if date_parts[0]:
            try:
                result["year"] = int(date_parts[0])
            except ValueError:
                logger.warning("Invalid year in date field: '%s'.", date_parts[0])
        if len(date_parts) >= 2 and date_parts[1]:
            try:
                result["month"] = int(date_parts[1])
            except ValueError:
                logger.warning("Invalid month in date field: '%s'.", date_parts[1])


def _process_keywords(result: Dict[str, Any]) -> None:
    """Convert keywords to tags."""
    if "keywords" in result:
        result["tags"] = RE_SEPARATOR.split(result.pop("keywords"))


def _process_reference(result: Dict[str, Any]) -> None:
    """Clean up or create reference."""
    if "ref" in result:
        result["ref"] = papis.bibtex.ref_cleanup(result["ref"])
    else:
        result["ref"] = papis.bibtex.create_reference(result)


def _process_files(result: Dict[str, Any], bib_file: str) -> List[str]:
    """Process associated files from the entry."""
    files = []
    file_entries = result.pop("file", None)
    if file_entries:
        for entry in file_entries.split(";"):
            entry = entry.strip()
            if not entry:
                continue

            file_path = _extract_file_path(entry)
            file_path = os.path.normpath(file_path)

            # Make path absolute if not already
            if not os.path.isabs(file_path):
                file_path = os.path.join(os.path.dirname(bib_file), file_path)

            if os.path.exists(file_path):
                logger.info("Document file found: '%s'.", file_path)
                files.append(file_path)
            else:
                logger.warning("Document file not found: '%s'.", file_path)
    return files


def _extract_file_path(entry: str) -> str:
    """Extract the file path from a file entry."""
    parts = entry.split(":")
    if len(parts) > 1:
        # Handle Windows paths (e.g., C:\... or description:C:\...)
        if os.name == "nt" and len(parts[0]) == 1 and parts[0].isalpha():
            return ":".join(parts)
        elif os.name == "nt" and len(parts) > 1 and len(parts[1]) == 1 and parts[1].isalpha():
            return ":".join(parts[1:])
        else:
            # POSIX paths (e.g., description:/path/...)
            return parts[1]
    else:
        return entry


def _add_directory_files(files: List[str], bib_file: str) -> List[str]:
    """Add all files from the BibTeX file's directory if there's only one entry."""
    bib_file_dir = os.path.dirname(bib_file)
    if not bib_file_dir:
        bib_file_dir = "."

    if os.path.isdir(bib_file_dir):
        # Get basenames of explicitly listed files to avoid duplicates
        explicit_basenames = {os.path.basename(f) for f in files}

        # Add directory files that aren't already in the explicit list
        for filename in os.listdir(bib_file_dir):
            file_path = os.path.join(bib_file_dir, filename)
            if (os.path.isfile(file_path) and
                filename != os.path.basename(bib_file) and
                filename not in explicit_basenames):
                files.append(file_path)

    return files
