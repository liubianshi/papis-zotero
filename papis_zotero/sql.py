import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import papis.logging

from papis_zotero.utils import (
    ZOTERO_EXCLUDED_FIELDS,
    ZOTERO_EXCLUDED_ITEM_TYPES,
    ZOTERO_SUPPORTED_MIMETYPES_TO_EXTENSION,
    ZOTERO_TO_PAPIS_FIELDS,
    ZOTERO_TO_PAPIS_TYPES,
)

logger = papis.logging.get_logger(__name__)

# fuzzy date matching
ISO_DATE_RE = re.compile(r"(?P<year>\d{4})-?(?P<month>\d{1,2})?-?(?P<day>\d{1,2})?")


ZOTERO_QUERY_ITEM_FIELD = """
SELECT
    fields.fieldName,
    itemDataValues.value
FROM
    fields,
    itemData,
    itemDataValues
WHERE
    itemData.itemID = ? AND
    fields.fieldID = itemData.fieldID AND
    itemDataValues.valueID = itemData.valueID
"""


def get_fields(connection: sqlite3.Connection, item_id: str) -> Dict[str, str]:
    """
    :arg item_id: an identifier for the item to query.
    :returns: a dictionary mapping fields to their values, e.g. ``"doi"``.
    """
    cursor = connection.cursor()
    cursor.execute(ZOTERO_QUERY_ITEM_FIELD, (item_id,))

    # get fields
    fields = {}
    for name, value in cursor:
        if name in ZOTERO_EXCLUDED_FIELDS:
            continue

        papis_name = ZOTERO_TO_PAPIS_FIELDS.get(name, name)
        fields[papis_name] = value

    # get year and month from date if available
    date = fields.pop("date", None)
    if date is not None:
        m = ISO_DATE_RE.match(date)
        if m:
            if m.group("year"):
                fields["year"] = int(m.group("year"))
            if m.group("month"):
                fields["month"] = int(m.group("month"))
        else:
            # NOTE: didn't manage to match, so just save the whole date
            fields["date"] = date

    return fields


ZOTERO_QUERY_ITEM_CREATORS = """
SELECT
    creatorTypes.creatorType,
    creators.firstName,
    creators.lastName
FROM
    creatorTypes,
    creators,
    itemCreators
WHERE
    itemCreators.itemID = ? AND
    creatorTypes.creatorTypeID = itemCreators.creatorTypeID AND
    creators.creatorID = itemCreators.creatorID
ORDER BY
    creatorTypes.creatorType,
    itemCreators.orderIndex
"""


def get_creators(connection: sqlite3.Connection,
                 item_id: str) -> Dict[str, List[str]]:
    from papis.document import author_list_to_author

    cursor = connection.cursor()
    cursor.execute(ZOTERO_QUERY_ITEM_CREATORS, (item_id,))

    # gather creators
    creators_by_type: Dict[str, List[Dict[str, str]]] = {}
    for ctype, given_name, family_name in cursor:
        creators_by_type.setdefault(ctype.lower(), []).append({
            "given": given_name,
            "family": family_name,
            })

    # convert to papis format
    result: Dict[str, Any] = {}
    for ctype, creators in creators_by_type.items():
        result[ctype] = author_list_to_author({"author_list": creators})
        result[f"{ctype}_list"] = creators

    return result


ZOTERO_QUERY_ITEM_ATTACHMENTS = """
SELECT
    items.key,
    itemAttachments.path,
    itemAttachments.contentType
FROM
    itemAttachments,
    items
WHERE
    itemAttachments.parentItemID = ? AND
    itemAttachments.contentType IN ({}) AND
    items.itemID = itemAttachments.itemID
""".format(",".join(["?"] * len(ZOTERO_SUPPORTED_MIMETYPES_TO_EXTENSION)))


def get_files(connection: sqlite3.Connection, item_id: str, input_path: str, attachments_folder: str) -> List[str]:
    cursor = connection.cursor()
    cursor.execute(
        ZOTERO_QUERY_ITEM_ATTACHMENTS,
        (item_id, *ZOTERO_SUPPORTED_MIMETYPES_TO_EXTENSION))

    files = []
    for key, path, mime_type in cursor:
        if path is None:
            logger.warning("Attachment %s (with type %s) skipped. Path not specified.",
                           key, mime_type)
            continue

        logger.debug("Processing attachment %s (with type %s) from '%s'.",
                     key, mime_type, path)
        if match := re.match(r"storage:(.*)", path):
            file_name = match.group(1)
            file_path = os.path.join(input_path, "storage", key, file_name)
            if os.path.exists(file_path):
                files.append(file_path)
            else:
                logger.warning("Storage file not found: %s", file_path)
        elif match := re.match(r"attachments:(.*)", path):
            file_name = match.group(1)
            file_path = os.path.join(attachments_folder, file_name)
            if os.path.exists(file_path):
                files.append(file_path)
            else:
                logger.warning("Attachment file not found: %s", file_path)
        elif os.path.exists(path):
            # NOTE: this is likely a symlink to some other on-disk location
            files.append(path)
        else:
            logger.error("Failed to export attachment %s (with type %s) from '%s'.",
                         key, mime_type, path)

    return files


ZOTERO_QUERY_ITEM_TAGS = """
SELECT
    tags.name
FROM
    tags,
    itemTags
WHERE
    itemTags.itemID = ? AND
    tags.tagID = itemTags.tagID
"""


def get_tags(connection: sqlite3.Connection, item_id: str) -> Dict[str, List[str]]:
    cursor = connection.cursor()
    cursor.execute(ZOTERO_QUERY_ITEM_TAGS, (item_id,))

    tags = [str(row[0]) for row in cursor]
    return {"tags": tags} if tags else {}


ZOTERO_QUERY_ITEM_COLLECTIONS = """
WITH RECURSIVE collection_hierarchy (id, name, parent, depth) AS (
    -- 初始: 使用子查询取 item's 第一个直接集合（按 collectionID 升序）
    SELECT id, name, parent, depth
    FROM (
        SELECT
            c.collectionID AS id,
            c.collectionName AS name,
            c.parentCollectionID AS parent,
            0 AS depth
        FROM
            collections c
        JOIN
            collectionItems ci ON c.collectionID = ci.collectionID
        WHERE
            ci.itemID = ?
        ORDER BY c.collectionID
        LIMIT 1
    )

    UNION ALL

    -- 递归: 向上找父级，depth 加 1
    SELECT
        c.collectionID AS id,
        c.collectionName AS name,
        c.parentCollectionID AS parent,
        h.depth + 1 AS depth
    FROM
        collections c
    JOIN
        collection_hierarchy h ON c.collectionID = h.parent
)
-- 返回名称，按 depth 降序（根先），去重（虽 unlikely）
SELECT DISTINCT name
FROM collection_hierarchy
ORDER BY depth DESC;
"""


def get_collections(connection: sqlite3.Connection,
                    item_id: str) -> Dict[str, List[str]]:
    cursor = connection.cursor()
    cursor.execute(ZOTERO_QUERY_ITEM_COLLECTIONS, (item_id,))

    collections = [name for name, in cursor]
    return {"collections": collections} if collections else {}


ZOTERO_QUERY_ITEM_COUNT = """
SELECT
    COUNT(item.itemID)
FROM
    items item,
    itemTypes itemType
WHERE
    itemType.itemTypeID = item.itemTypeID AND
    itemType.typeName NOT IN ({})
ORDER BY
    item.itemID
""".format(",".join(["?"] * len(ZOTERO_EXCLUDED_ITEM_TYPES)))

ZOTERO_QUERY_ITEMS = """
SELECT
    item.itemID,
    itemType.typeName,
    key,
    dateAdded
FROM
    items item,
    itemTypes itemType
WHERE
    itemType.itemTypeID = item.itemTypeID AND
    itemType.typeName NOT IN ({})
ORDER BY
    item.itemID
""".format(",".join(["?"] * len(ZOTERO_EXCLUDED_ITEM_TYPES)))


def add_from_sql(input_path: str,
                 out_folder: Optional[str] = None,
                 attachments_folder: Optional[str] = None,
                 link: bool = False) -> None:
    """
    :param inpath: path to zotero SQLite database "zoter.sqlite" and
        "storage" to be imported
    :param outpath: path where all items will be exported to created if not
        existing
    """
    from papis.config import get_lib_dirs, getstring, set_lib_from_name

    if out_folder is None:
        out_folder = get_lib_dirs()[0]

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"[Errno 2] No such file or directory: '{input_path}'")

    if not os.path.exists(out_folder):
        raise FileNotFoundError(
            f"[Errno 2] No such file or directory: '{out_folder}'")

    zotero_sqlite_file = os.path.join(input_path, "zotero.sqlite")
    if not os.path.exists(zotero_sqlite_file):
        raise FileNotFoundError(
            f"No 'zotero.sqlite' file found in '{input_path}'")

    connection = sqlite3.connect(zotero_sqlite_file)
    cursor = connection.cursor()

    cursor.execute(ZOTERO_QUERY_ITEM_COUNT, ZOTERO_EXCLUDED_ITEM_TYPES)
    for row in cursor:
        items_count = row[0]

    cursor.execute(ZOTERO_QUERY_ITEMS, ZOTERO_EXCLUDED_ITEM_TYPES)
    if out_folder is not None:
        set_lib_from_name(out_folder)

    from papis.strings import time_format

    folder_name = getstring("add-folder-name")

    # Determine attachments folder once, outside the loop
    default_attachments_folder = attachments_folder or input_path

    for i, (item_id, zitem_type, item_key, zdate_added) in enumerate(cursor, start=1):
        # convert fields
        date_added = (
            datetime.strptime(zdate_added, "%Y-%m-%d %H:%M:%S")
            .strftime(time_format))
        item_type = ZOTERO_TO_PAPIS_TYPES.get(zitem_type, zitem_type)

        # get Zotero metadata
        fields = get_fields(connection, item_id)

        files = get_files(connection,
                          item_id,
                          input_path=input_path,
                          attachments_folder=default_attachments_folder)

        item = {"type": item_type, "time-added": date_added, "files": files}
        item.update(fields)
        item.update(get_creators(connection, item_id))
        item.update(get_tags(connection, item_id))
        item.update(get_collections(connection, item_id))

        logger.info("[%4d/%-4d] Exporting item '%s' to library '%s'.",
                    i, items_count, item_key, out_folder)

        subfolder = "Misc"
        collections = item.get("collections")
        if collections and len(collections):
            subfolder = os.path.join(*collections)
        from papis.commands.add import run as add
        add(paths=files, data=item, link=link, folder_name=folder_name, subfolder=subfolder)

    logger.info("Finished exporting from '%s'.", input_path)
    logger.info("Exported files can be found at '%s'.", out_folder)
