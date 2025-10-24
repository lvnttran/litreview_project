#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import json
from datetime import datetime, timezone

DB_PATH = "documents.db"

def init_db(path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # documents table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ArticleTitle TEXT UNIQUE,
        Authors TEXT,
        Affiliations TEXT,
        PublicationYear TEXT,
        Abstracts TEXT,
        AuthorKeywords TEXT,
        KeywordsPlus TEXT,
        WoSCategories TEXT,
        ResearchAreas TEXT,
        ExtraProperties TEXT,
        CreationTime TEXT,
        SubKeyWords TEXT,
        Notes TEXT,
        ExcelIndex INTEGER
    )
    """)
    # Migrate: add Notes column if missing (for older DBs)
    try:
        cur.execute("PRAGMA table_info(documents)")
        cols = [r[1] for r in cur.fetchall()]
        if "Notes" not in cols:
            cur.execute("ALTER TABLE documents ADD COLUMN Notes TEXT")
        if "ExcelIndex" not in cols:
            cur.execute("ALTER TABLE documents ADD COLUMN ExcelIndex INTEGER")
    except Exception:
        pass
    # FTS5 virtual table for fast fulltext search (if available)
    try:
        cur.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(ArticleTitle, Authors, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, SubKeyWords, Notes, content='documents', content_rowid='id')")
    except sqlite3.OperationalError:
        # FTS may not be compiled; ignore but searches will be slower
        pass
    conn.commit()
    conn.close()


def upsert_document_record(conn, record):
    """
    record: dict with keys matching documents table columns (except id)
    SubKeyWords should be a python list (will be stored as JSON)
    """
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # normalize fields
    doc = {
        "ArticleTitle": record.get("ArticleTitle", ""),
        "Authors": record.get("Authors", ""),
        "Affiliations": record.get("Affiliations", ""),
        "PublicationYear": record.get("PublicationYear", ""),
        "Abstracts": record.get("Abstracts", ""),
        "AuthorKeywords": record.get("AuthorKeywords", ""),
        "KeywordsPlus": record.get("KeywordsPlus", ""),
        "WoSCategories": record.get("WoSCategories", ""),
        "ResearchAreas": record.get("ResearchAreas", ""),
        "ExtraProperties": record.get("ExtraProperties", ""),
        "CreationTime": record.get("CreationTime", now),
        "SubKeyWords": json.dumps(record.get("SubKeyWords", []), ensure_ascii=False),
        "Notes": record.get("Notes", ""),
        "ExcelIndex": record.get("ExcelIndex")
    }
    # Check exist by ArticleTitle
    cur.execute("SELECT id, SubKeyWords FROM documents WHERE ArticleTitle = ?", (doc["ArticleTitle"],))
    row = cur.fetchone()
    if row:
        doc_id = row[0]
        existing_sub = []
        try:
            existing_sub = json.loads(row[1])
        except Exception:
            pass
        # merge subkeywords uniquely
        new_sub = list(dict.fromkeys(existing_sub + json.loads(doc["SubKeyWords"])))
        cur.execute("""
            UPDATE documents SET Authors=?,Affiliations=?,PublicationYear=?,Abstracts=?,AuthorKeywords=?,KeywordsPlus=?,WoSCategories=?,ResearchAreas=?,ExtraProperties=?,SubKeyWords=?,Notes=COALESCE(NULLIF(?, ''), Notes), ExcelIndex=COALESCE(?, ExcelIndex)
            WHERE id=?
        """, (
            doc["Authors"], doc["Affiliations"], doc["PublicationYear"], doc["Abstracts"],
            doc["AuthorKeywords"], doc["KeywordsPlus"], doc["WoSCategories"], doc["ResearchAreas"],
            doc["ExtraProperties"], json.dumps(new_sub, ensure_ascii=False), doc["Notes"], doc["ExcelIndex"], doc_id
        ))
    else:
        cur.execute("""
            INSERT INTO documents (ArticleTitle,Authors,Affiliations,PublicationYear,Abstracts,AuthorKeywords,KeywordsPlus,WoSCategories,ResearchAreas,ExtraProperties,CreationTime,SubKeyWords,Notes,ExcelIndex)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            doc["ArticleTitle"], doc["Authors"], doc["Affiliations"], doc["PublicationYear"], doc["Abstracts"],
            doc["AuthorKeywords"], doc["KeywordsPlus"], doc["WoSCategories"], doc["ResearchAreas"],
            doc["ExtraProperties"], doc["CreationTime"], doc["SubKeyWords"], doc["Notes"], doc["ExcelIndex"]
        ))
        doc_id = cur.lastrowid
    conn.commit()

    # Update FTS table if available
    try:
        cur.execute(
            "INSERT INTO documents_fts(rowid, ArticleTitle, Authors, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, SubKeyWords, Notes) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (doc_id, doc["ArticleTitle"], doc["Authors"], doc["Abstracts"], doc["AuthorKeywords"], doc["KeywordsPlus"],
             doc["WoSCategories"], doc["ResearchAreas"],
             json.dumps(json.loads(doc["SubKeyWords"]), ensure_ascii=False), doc["Notes"]))
    except sqlite3.IntegrityError:
        # rowid exists, replace
        try:
            cur.execute(
                "DELETE FROM documents_fts WHERE rowid=?", (doc_id,))
            cur.execute(
                "INSERT INTO documents_fts(rowid, ArticleTitle, Authors, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, SubKeyWords, Notes) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (doc_id, doc["ArticleTitle"], doc["Authors"], doc["Abstracts"], doc["AuthorKeywords"],
                 doc["KeywordsPlus"], doc["WoSCategories"], doc["ResearchAreas"],
                 json.dumps(json.loads(doc["SubKeyWords"]), ensure_ascii=False), doc["Notes"]))
        except Exception:
            pass
    except Exception:
        pass
    conn.commit()


def fetch_documents_page(conn, page=0, page_size=20, search_query=None, doc_id=None):
    """
    Truy vấn danh sách tài liệu có phân trang và tìm kiếm.
    - Nếu có doc_id → chỉ lấy đúng tài liệu có id đó.
    - Nếu có search_query → ưu tiên dùng FTS (Full Text Search), fallback sang LIKE.
    - Kết quả được sắp theo độ trùng khớp cao nhất (FTS hoặc LIKE).
    """
    cur = conn.cursor()
    offset = page * page_size

    # ⚡ 1️⃣ Nếu có doc_id → chỉ lấy đúng tài liệu đó
    if doc_id is not None:
        cur.execute("""
            SELECT id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear,
                   Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas,
                   ExtraProperties, CreationTime, SubKeyWords, Notes
            FROM documents
            WHERE id = ?
        """, (doc_id,))
        rows = cur.fetchall()
        total = len(rows)
        return rows, total

    # ⚡ 2️⃣ Nếu có từ khóa tìm kiếm
    if search_query and search_query.strip():
        try:
            # 🧠 Thử tìm bằng FTS5 với độ liên quan (bm25)
            cur.execute("""
                WITH matched AS (
                    SELECT d.*, bm25(documents_fts) AS score
                    FROM documents d
                    JOIN documents_fts f ON d.id = f.rowid
                    WHERE documents_fts MATCH ?
                    ORDER BY score ASC
                    LIMIT ? OFFSET ?
                )
                SELECT 
                    id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear,
                    Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas,
                    ExtraProperties, CreationTime, SubKeyWords, Notes
                FROM matched
            """, (search_query, page_size, offset))
            rows = cur.fetchall()

            # Đếm tổng số kết quả FTS
            cur.execute("SELECT count(*) FROM documents_fts WHERE documents_fts MATCH ?", (search_query,))
            total = cur.fetchone()[0]

        except Exception:
            # 🧩 Fallback sang LIKE nếu FTS lỗi
            search_pattern = f"%{search_query}%"
            cur.execute("""
                WITH matched AS (
                    SELECT *,
                        (
                            (CASE WHEN ArticleTitle LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN Authors LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN Abstracts LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN AuthorKeywords LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN KeywordsPlus LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN WoSCategories LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN ResearchAreas LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN SubKeyWords LIKE ? THEN 1 ELSE 0 END) +
                            (CASE WHEN Notes LIKE ? THEN 1 ELSE 0 END)
                        ) AS match_score
                    FROM documents
                    WHERE ArticleTitle LIKE ? 
                        OR Authors LIKE ? 
                        OR Abstracts LIKE ?
                        OR AuthorKeywords LIKE ?
                        OR KeywordsPlus LIKE ?
                        OR WoSCategories LIKE ?
                        OR ResearchAreas LIKE ?
                        OR SubKeyWords LIKE ?
                        OR Notes LIKE ?
                    ORDER BY match_score DESC, CreationTime DESC
                    LIMIT ? OFFSET ?
                )
                SELECT 
                    id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear,
                    Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas,
                    ExtraProperties, CreationTime, SubKeyWords, Notes
                FROM matched
            """, (search_pattern,) * 18 + (page_size, offset))
            rows = cur.fetchall()

            # Đếm tổng kết quả LIKE
            cur.execute("""
                SELECT count(*)
                FROM documents 
                WHERE ArticleTitle LIKE ? 
                    OR Authors LIKE ? 
                    OR Abstracts LIKE ?
                    OR AuthorKeywords LIKE ?
                    OR KeywordsPlus LIKE ?
                    OR WoSCategories LIKE ?
                    OR ResearchAreas LIKE ?
                    OR SubKeyWords LIKE ?
                    OR Notes LIKE ?
            """, (search_pattern,) * 9)
            total = cur.fetchone()[0]

    else:
        # ⚙️ 3️⃣ Không có search_query → lấy toàn bộ theo CreationTime DESC
        cur.execute("""
            SELECT id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear,
                   Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas,
                   ExtraProperties, CreationTime, SubKeyWords, Notes
            FROM documents
            ORDER BY CreationTime DESC
            LIMIT ? OFFSET ?
        """, (page_size, offset))
        rows = cur.fetchall()

        cur.execute("SELECT count(*) FROM documents")
        total = cur.fetchone()[0]

    return rows, total
