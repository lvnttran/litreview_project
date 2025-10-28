#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Document Classifier GUI with PyQt5
- Tab 1: Import Excel, select columns, choose filter mode, token statistics, save tokens to DB (add to documents' SubKeyWords)
- Tab 2: Show saved documents, search (FTS), paginate, preview

Dependencies:
    pip install pyqt5 pandas openpyxl
Run:
    python doc_classifier_qt.py
"""

import sys
import os
import re
import json
import sqlite3
from datetime import datetime, timezone
from collections import Counter, defaultdict
import html

import pandas as pd
from PyQt5 import QtCore, QtWidgets, QtGui
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

DB_PATH = "documents.db"

# Main columns priority
PRIORITY_COLUMNS = [
    "Article Title",
    "Authors",
    "Affiliations",
    "Publication Year",
    "Abstracts",
    "Author Keywords",
    "Keywords Plus",
    "WoS Categories",
    "Research Areas",
]

# Columns that use semicolon-separated keyword phrases
SPECIAL_KEYWORD_COLUMNS = {
    "Authors",
    "Affiliations",
    "Author Keywords",
    "Keywords Plus",
    "WoS Categories",
    "Research Areas",
}

# Basic english stopwords (connective words) - extendable
ENGLISH_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "if", "then", "else", "when", "at", "by", "for", "with", "about",
    "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from",
    "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
    "there", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
    "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
    "should", "now", "of", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did"
}

# Simple tokenizer
WORD_RE = re.compile(r"[A-Za-z0-9'\-]+")  # includes words with apostrophes or dashes

# --- User-editable recommended tag dictionary for Notes ---
RECOMMENDED_TAGS = {
    "Type": ["product_config;", "process_configuration;", "product_process_config"],
    "Motiv": [""],
    "AI tech": ["_cbr;", "_dt;", "_owl;", "_swlr;"],
    "Optimization": ["_ga;"],
    "System Eng": [""],
    "KPI": ["_cost;", "_time;"],
    "Use case": ["_yes; detail;", "_no;"],
    "Data": ["_real;", "_simulated;"],
    "Context": ["_eto;", "_cto/mass/ato;"],
    "Futur": [""],
}

# ---------------------------
# Database utilities
# ---------------------------
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
            existing_sub = json.loads(row[1] or "[]")
        except Exception:
            existing_sub = []
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
            "INSERT INTO documents_fts(rowid, ArticleTitle, Authors, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, SubKeyWords, Notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (doc_id, doc["ArticleTitle"], doc["Authors"], doc["Abstracts"], doc["AuthorKeywords"], doc["KeywordsPlus"],
             doc["WoSCategories"], doc["ResearchAreas"],
             json.dumps(json.loads(doc["SubKeyWords"]), ensure_ascii=False), doc["Notes"]))
    except sqlite3.IntegrityError:
        # rowid exists, replace
        try:
            cur.execute(
                "INSERT OR REPLACE INTO documents_fts(rowid, ArticleTitle, Authors, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, SubKeyWords, Notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (doc_id, doc["ArticleTitle"], doc["Authors"], doc["Abstracts"], doc["AuthorKeywords"],
                 doc["KeywordsPlus"], doc["WoSCategories"], doc["ResearchAreas"],
                 json.dumps(json.loads(doc["SubKeyWords"]), ensure_ascii=False), doc["Notes"]))
        except Exception:
            pass
    except Exception:
        pass
    conn.commit()


def fetch_documents_page(conn, page=0, page_size=20, search_query=None):
    cur = conn.cursor()
    offset = page * page_size
    if search_query and search_query.strip():
        # try FTS first
        try:
            # Split search terms by ';' to match Notes segments individually
            terms = [t.strip() for t in re.split(r"[;]", search_query) if t.strip()]
            note_like_clauses = []
            note_like_params = []
            for t in terms:
                note_like_clauses.append("documents.Notes LIKE ?")
                note_like_params.append(f"%{t}%")
            note_where = (" OR " + " OR ".join(note_like_clauses)) if note_like_clauses else ""

            base_where = "documents_fts MATCH ?" + note_where
            order_probe = (note_like_params[0] if note_like_params else f"%{search_query}%")

            sql = (
                "SELECT documents.id, documents.ExcelIndex, documents.ArticleTitle, documents.Authors, documents.Affiliations, documents.PublicationYear, documents.Abstracts, documents.AuthorKeywords, documents.KeywordsPlus, documents.WoSCategories, documents.ResearchAreas, documents.ExtraProperties, documents.CreationTime, documents.SubKeyWords, documents.Notes "
                "FROM documents_fts JOIN documents ON documents_fts.rowid = documents.id "
                f"WHERE {base_where} "
                "ORDER BY CASE "
                "WHEN documents.SubKeyWords LIKE ? THEN 0 "
                "WHEN documents.ArticleTitle LIKE ? THEN 1 "
                "WHEN documents.Abstracts LIKE ? THEN 2 "
                "WHEN documents.Notes LIKE ? THEN 3 ELSE 4 END, documents.CreationTime DESC "
                "LIMIT ? OFFSET ?"
            )
            params = [search_query] + note_like_params + [order_probe, order_probe, order_probe, order_probe, page_size,
                                                          offset]
            cur.execute(sql, params)
            rows = cur.fetchall()
            count_sql = (
                "SELECT count(*) FROM documents_fts JOIN documents ON documents_fts.rowid = documents.id "
                f"WHERE {base_where}"
            )
            count_params = [search_query] + note_like_params
            cur.execute(count_sql, count_params)
            total = cur.fetchone()[0]
        except Exception:
            # fallback to naive LIKE search
            terms = [t.strip() for t in re.split(r"[;]", search_query) if t.strip()]
            like_fields = [
                "ArticleTitle", "Authors", "Abstracts", "AuthorKeywords", "KeywordsPlus", "WoSCategories",
                "ResearchAreas", "SubKeyWords"
            ]
            q = f"%{search_query}%"
            where = " OR ".join([f"{f} LIKE ?" for f in like_fields])
            note_where = " OR ".join(["Notes LIKE ?" for _ in terms]) if terms else "Notes LIKE ?"
            where = where + " OR " + note_where
            order_probe = (f"%{terms[0]}%" if terms else q)

            sql = (
                "SELECT id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, ExtraProperties, CreationTime, SubKeyWords, Notes FROM documents "
                f"WHERE {where} "
                "ORDER BY CASE WHEN SubKeyWords LIKE ? THEN 0 WHEN ArticleTitle LIKE ? THEN 1 WHEN Abstracts LIKE ? THEN 2 WHEN Notes LIKE ? THEN 3 ELSE 4 END, CreationTime DESC LIMIT ? OFFSET ?"
            )
            params = [q] * len(like_fields) + ([(f"%{t}%" for t in terms)] if terms else [q])
            # flatten generator if terms present
            if terms:
                params = [q] * len(like_fields) + [f"%{t}%" for t in terms]
            params += [order_probe, order_probe, order_probe, order_probe, page_size, offset]
            cur.execute(sql, params)
            rows = cur.fetchall()
            count_sql = (
                    "SELECT count(*) FROM documents WHERE " + " OR ".join(
                [f"{f} LIKE ?" for f in like_fields]) + " OR " + (
                        " OR ".join(["Notes LIKE ?" for _ in terms]) if terms else "Notes LIKE ?")
            )
            count_params = [q] * len(like_fields) + ([f"%{t}%" for t in terms] if terms else [q])
            cur.execute(count_sql, count_params)
            total = cur.fetchone()[0]
    else:
        cur.execute(
            "SELECT id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, ExtraProperties, CreationTime, SubKeyWords, Notes FROM documents ORDER BY CreationTime DESC LIMIT ? OFFSET ?",
            (page_size, offset))
        rows = cur.fetchall()
        cur.execute("SELECT count(*) FROM documents")
        total = cur.fetchone()[0]
    return rows, total

    # ---------------------------
    # Helper text functions
    # ---------------------------
    if text is None:
        return []
    if lower:
        text = text.lower()
    tokens = WORD_RE.findall(text)
    # strip tokens that are just numbers or too short
    tokens = [t.strip("-'") for t in tokens if len(t.strip("-'")) >= 2]
    if remove_stopwords:
        tokens = [t for t in tokens if t not in ENGLISH_STOPWORDS]
    return tokens


def extract_tokens_from_field(value, field_name=None, lower=True, remove_stopwords=False):
    """Return list of tokens or phrases, preserving multi-word keywords for special columns."""
    if value is None or pd.isna(value):
        return []
    value = str(value).strip()
    if not value:
        return []
    if lower:
        value = value.lower()

    # Special columns → split by semicolon and keep phrases intact
    if field_name in SPECIAL_KEYWORD_COLUMNS:
        tokens = [p.strip() for p in value.split(";") if p.strip()]
        return tokens

    # Regular text → word-based tokenization
    tokens = WORD_RE.findall(value)
    tokens = [t.strip("-'") for t in tokens if len(t.strip("-'")) >= 2]
    if remove_stopwords:
        tokens = [t for t in tokens if t not in ENGLISH_STOPWORDS]
    return tokens


def tokenise_text_row(row, columns, lower=True, remove_stopwords=False):
    """Tokenize text from multiple columns of a pandas row using appropriate rules per column."""
    tokens = []
    for col in columns:
        if col not in row:
            continue
        val = row[col]
        tokens.extend(extract_tokens_from_field(val, field_name=col, lower=lower, remove_stopwords=remove_stopwords))
    # Deduplicate but keep order
    return list(dict.fromkeys(tokens))


def build_extra_properties_from_row(row: pd.Series, keep_cols):
    # join all other columns not in keep_cols as described
    extras = []
    for col, val in row.items():
        if col in keep_cols:
            continue
        s = "" if pd.isna(val) else str(val)
        extras.append(f"{col}:\n{s}")
    return "\n\n".join(extras)


# ---------------------------
# Qt Models & Widgets
# ---------------------------
class DataFrameModel(QtCore.QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), parent=None):
        super().__init__(parent)
        self._df = df

    def update(self, df):
        self.beginResetModel()
        self._df = df.copy()
        self.endResetModel()

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 0 if self._df is None else len(self._df.columns)

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if not index.isValid() or self._df is None:
            return None
        if role == QtCore.Qt.DisplayRole:
            val = self._df.iat[index.row(), index.column()]
            if pd.isna(val):
                return ""
            return str(val)
        return None

    def headerData(self, section, orientation, role):
        if role != QtCore.Qt.DisplayRole:
            return None
        if orientation == QtCore.Qt.Horizontal:
            try:
                return self._df.columns[section]
            except Exception:
                return None
        else:
            return str(section + 1)


# ---------------------------
# Main Window
# ---------------------------
class ZoomableTextEdit(QtWidgets.QTextEdit):
    """QTextEdit that supports zooming with Ctrl + Mouse Wheel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._zoom_level = 0  # Track zoom relative to default

    def wheelEvent(self, event):
        # Check if Ctrl is held
        modifiers = QtWidgets.QApplication.keyboardModifiers()
        if modifiers == QtCore.Qt.ControlModifier:
            delta = event.angleDelta().y()
            if delta > 0:
                self.zoomIn(1)
                self._zoom_level += 1
            elif delta < 0:
                self.zoomOut(1)
                self._zoom_level -= 1
            event.accept()
        else:
            super().wheelEvent(event)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Document Classifier")
        self.resize(1200, 800)
        init_db()

        self.df = pd.DataFrame()
        self.current_file = None

        self._setup_ui()

    def _setup_ui(self):
        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)

        self.tab_import = QtWidgets.QWidget()
        self.tab_saved = QtWidgets.QWidget()
        self.tab_analytics = QtWidgets.QWidget()

        self.tabs.addTab(self.tab_import, "Import & Classify")
        self.tabs.addTab(self.tab_saved, "Saved Documents")
        self.tabs.addTab(self.tab_analytics, "Analytics")

        self._setup_tab_import()
        self._setup_tab_saved()
        self._setup_tab_analytics()

    # ---------------------------
    # Tab 1: Import & Classify
    # ---------------------------
    def _setup_tab_import(self):
        layout = QtWidgets.QHBoxLayout(self.tab_import)

        # Left pane (half)
        left = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(left)

        btn_load = QtWidgets.QPushButton("Import Excel")
        btn_load.clicked.connect(self.load_excel)
        left_layout.addWidget(btn_load)

        self.label_file = QtWidgets.QLabel("No file loaded")
        left_layout.addWidget(self.label_file)

        left_layout.addWidget(QtWidgets.QLabel("Available columns (check to include in token extraction):"))

        # checkbox list for columns
        self.list_columns = QtWidgets.QListWidget()
        self.list_columns.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.list_columns.itemChanged.connect(self.on_columns_changed)
        left_layout.addWidget(self.list_columns, stretch=1)

        # Filter type
        left_layout.addWidget(QtWidgets.QLabel("Filter type:"))
        self.filter_group = QtWidgets.QButtonGroup()
        hfilter = QtWidgets.QHBoxLayout()
        rb_all = QtWidgets.QRadioButton("Include all tokens")
        rb_remove_stop = QtWidgets.QRadioButton("Remove English stopwords")
        rb_all.setChecked(True)
        self.filter_group.addButton(rb_all, 0)
        self.filter_group.addButton(rb_remove_stop, 1)
        for rb in (rb_all, rb_remove_stop):
            hfilter.addWidget(rb)
            rb.toggled.connect(self.on_filter_changed)
        left_layout.addLayout(hfilter)

        # --- Search bar for token table ---
        search_layout = QtWidgets.QHBoxLayout()
        self.search_token_edit = QtWidgets.QLineEdit()
        self.search_token_edit.setPlaceholderText("Search token (Ctrl+F)...")
        btn_next = QtWidgets.QPushButton("Find Next")
        search_layout.addWidget(QtWidgets.QLabel("Find:"))
        search_layout.addWidget(self.search_token_edit, stretch=1)
        search_layout.addWidget(btn_next)
        left_layout.addLayout(search_layout)

        # Shortcut: Ctrl+F to focus search bar
        shortcut_find = QtWidgets.QShortcut(QtGui.QKeySequence("Ctrl+F"), self)
        shortcut_find.activated.connect(self.search_token_edit.setFocus)

        # Connect search logic
        self.search_token_edit.textChanged.connect(self._filter_or_jump_token)
        btn_next.clicked.connect(self._find_next_token)
        self._last_search_row = 0

        # Token stats table
        left_layout.addWidget(QtWidgets.QLabel("Token statistics:"))
        self.table_tokens = QtWidgets.QTableWidget(0, 5)
        self.table_tokens.setHorizontalHeaderLabels(["#", "Token", "Total Count", "Document Count", "Rows (indices)"])
        self.table_tokens.cellDoubleClicked.connect(self.on_token_cell_double_clicked)
        self.table_tokens.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Interactive)
        self.table_tokens.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        left_layout.addWidget(self.table_tokens, stretch=2)

        # Save selected token(s) button
        btn_save_selected = QtWidgets.QPushButton("Save Selected Token -> add to documents")
        btn_save_selected.clicked.connect(self.save_selected_tokens)
        left_layout.addWidget(btn_save_selected)

        layout.addWidget(left, 1)

        # Right pane (split vertically into top table and bottom preview)
        right = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right)

        # Top - Data table view and Bottom - Preview in a resizable splitter
        self.model_table = DataFrameModel(pd.DataFrame())
        self.view_table = QtWidgets.QTableView()
        self.view_table.setModel(self.model_table)
        self.view_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.view_table.selectionModel().selectionChanged.connect(self.on_table_selection_changed)
        self.view_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Interactive)

        self.preview = ZoomableTextEdit()
        self.preview.setReadOnly(True)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter.addWidget(self.view_table)
        splitter.addWidget(self.preview)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        right_layout.addWidget(splitter, stretch=1)

        layout.addWidget(right, 2)

    def load_excel(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open Excel file", "",
                                                        "Excel Files (*.xlsx *.xls);;All Files (*)")
        if not path:
            return
        try:
            df = pd.read_excel(path, dtype=str)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to read Excel: {e}")
            return
        self.df = df.fillna("")
        self.current_file = path
        self.label_file.setText(os.path.basename(path))
        # Verify required columns
        missing = [c for c in PRIORITY_COLUMNS if c not in self.df.columns]
        if missing:
            QtWidgets.QMessageBox.critical(self, "Missing columns",
                                           f"The following required columns are missing:\n{', '.join(missing)}")
            # still let them proceed but don't allow extraction until fixed
        # populate checkbox list with priority columns first
        self.list_columns.clear()
        cols_ordered = [c for c in PRIORITY_COLUMNS if c in self.df.columns] + [c for c in self.df.columns if
                                                                                c not in PRIORITY_COLUMNS]
        for col in cols_ordered:
            item = QtWidgets.QListWidgetItem(col)
            item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
            # default checked for Article Title and Abstracts
            if col in ("Article Title", "Abstracts"):
                item.setCheckState(QtCore.Qt.Checked)
            else:
                item.setCheckState(QtCore.Qt.Unchecked)
            self.list_columns.addItem(item)
        # update table view (display prioritized columns first)
        desired_cols = [c for c in PRIORITY_COLUMNS if c in self.df.columns] + [c for c in self.df.columns if
                                                                                c not in PRIORITY_COLUMNS]
        display_df = self.df[desired_cols].copy()
        self.model_table.update(display_df)
        # initial auto-size to content
        self.view_table.resizeColumnsToContents()
        self.view_table.resizeRowsToContents()
        # reset preview & tokens
        self.update_token_stats()

    def get_checked_columns(self):
        cols = []
        for i in range(self.list_columns.count()):
            item = self.list_columns.item(i)
            if item.checkState() == QtCore.Qt.Checked:
                cols.append(item.text())
        return cols

    def on_columns_changed(self, *_):
        self.update_token_stats()

    def on_filter_changed(self, *_):
        self.update_token_stats()

    def update_token_stats(self):
        # compute tokens according to checked columns and filter
        if self.df is None or self.df.empty:
            self.table_tokens.setRowCount(0)
            return
        checked = self.get_checked_columns()
        if not checked:
            self.table_tokens.setRowCount(0)
            return
        remove_stop = (self.filter_group.checkedId() == 1)
        # For each row, concat text from checked columns
        doc_tokens = []
        tokens_per_row = []
        #        for idx, row in self.df.iterrows():
        #            parts = []
        #            for col in checked:
        #                if col in row:
        #                    parts.append("" if pd.isna(row[col]) else str(row[col]))
        #            text = "\n".join(parts)
        #            toks = tokenise_text(text, lower=True, remove_stopwords=remove_stop)

        for idx, row in self.df.iterrows():
            toks = tokenise_text_row(row, checked, lower=True, remove_stopwords=remove_stop)

            tokens_per_row.append((int(idx), toks))
            doc_tokens.append(toks)
        # aggregate counts and document frequencies
        total_counter = Counter()
        doc_freq = defaultdict(int)
        rows_containing = defaultdict(list)
        for row_idx, toks in tokens_per_row:
            c = Counter(toks)
            for t, cnt in c.items():
                total_counter[t] += cnt
                doc_freq[t] += 1
                rows_containing[t].append(row_idx + 1)  # show 1-based row indices for user
        # sort tokens by total count desc
        items = sorted(total_counter.items(), key=lambda x: (-x[1], x[0]))  # (token, totcount)
        # populate table
        self.table_tokens.setRowCount(len(items))
        for r, (token, totcount) in enumerate(items):
            doc_count = doc_freq[token]
            rows_list = rows_containing[token]
            self.table_tokens.setItem(r, 0, QtWidgets.QTableWidgetItem(str(r + 1)))
            self.table_tokens.setItem(r, 1, QtWidgets.QTableWidgetItem(token))
            self.table_tokens.setItem(r, 2, QtWidgets.QTableWidgetItem(str(totcount)))
            self.table_tokens.setItem(r, 3, QtWidgets.QTableWidgetItem(str(doc_count)))
            self.table_tokens.setItem(r, 4, QtWidgets.QTableWidgetItem(", ".join(map(str, rows_list))))
        # initial auto-size to content
        self.table_tokens.resizeColumnsToContents()
        self.table_tokens.resizeRowsToContents()

    def on_table_selection_changed(self, selected, deselected):
        # show preview for first selected row
        indexes = self.view_table.selectionModel().selectedRows()
        if not indexes:
            self.preview.clear()
            return
        row = indexes[0].row()
        df_display = self.model_table._df
        if row < 0 or row >= len(df_display):
            self.preview.clear()
            return
        series = df_display.iloc[row]
        # build preview text with bold headings
        html_parts = []

        def bold(k):
            return f"<b>{k}:</b> "

        for col in PRIORITY_COLUMNS:
            if col in series.index:
                html_parts.append(f"{bold(col)}{html.escape(str(series[col]))}<br/>")
        # add other columns
        for col in series.index:
            if col in PRIORITY_COLUMNS:
                continue
            html_parts.append(f"{bold(col)}{html.escape(str(series[col]))}<br/>")
        self.preview.setHtml("<br/>".join(html_parts))

    def save_selected_tokens(self):
        # For each selected token row, add that token to the SubKeyWords of documents where it appears
        selected_rows = set([r.row() for r in self.table_tokens.selectionModel().selectedRows()])
        if not selected_rows:
            QtWidgets.QMessageBox.information(self, "No selection", "Please select token row(s) to save.")
            return
        checked_cols = self.get_checked_columns()
        if not checked_cols:
            QtWidgets.QMessageBox.warning(self, "No columns",
                                          "Please check at least one column to extract tokens from.")
            return
        remove_stop = (self.filter_group.checkedId() == 1)
        # Recompute token positions mapping similar to update_token_stats
        doc_tokens = []
        tokens_per_row = []
        #        for idx, row in self.df.iterrows():
        #            parts = []
        #            for col in checked_cols:
        #                if col in row:
        #                    parts.append("" if pd.isna(row[col]) else str(row[col]))
        #            text = "\n".join(parts)
        #            toks = tokenise_text(text, lower=True, remove_stopwords=remove_stop)
        #            tokens_per_row.append((int(idx), toks, row))

        for idx, row in self.df.iterrows():
            toks = tokenise_text_row(row, checked_cols, lower=True, remove_stopwords=remove_stop)
            tokens_per_row.append((int(idx), toks, row))

        # Build mapping token -> list of row indices
        token_to_rows = defaultdict(list)
        for r_idx, toks, row in tokens_per_row:
            unique = set(toks)
            for t in unique:
                token_to_rows[t].append(r_idx)
        # Determine selected tokens
        tokens_to_save = []
        for r in selected_rows:
            tok_item = self.table_tokens.item(r, 1)
            if tok_item:
                tokens_to_save.append(tok_item.text())
        # Save into DB
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        saved_count = 0
        for tok in tokens_to_save:
            rows_indices = token_to_rows.get(tok, [])
            for ridx in rows_indices:
                row_series = self.df.iloc[ridx]
                # prepare document record
                keep_cols = PRIORITY_COLUMNS.copy()
                extras = build_extra_properties_from_row(row_series, keep_cols)
                record = {
                    "ArticleTitle": str(row_series.get("Article Title", "")).strip(),
                    "Authors": str(row_series.get("Authors", "")),
                    "Affiliations": str(row_series.get("Affiliations", "")),
                    "PublicationYear": str(row_series.get("Publication Year", "")),
                    "Abstracts": str(row_series.get("Abstracts", "")),
                    "AuthorKeywords": str(row_series.get("Author Keywords", "")),
                    "KeywordsPlus": str(row_series.get("Keywords Plus", "")),
                    "WoSCategories": str(row_series.get("WoS Categories", "")),
                    "ResearchAreas": str(row_series.get("Research Areas", "")),
                    "ExtraProperties": extras,
                    "CreationTime": datetime.now(timezone.utc).isoformat(),
                    "SubKeyWords": [tok],
                    "ExcelIndex": int(ridx) + 1
                }
                # If no ArticleTitle, we still allow creating with empty but better to skip
                if not record["ArticleTitle"]:
                    # create a generated title using row number to avoid collisions
                    record["ArticleTitle"] = f"(Untitled row {ridx + 1})"
                upsert_document_record(conn, record)
                saved_count += 1
        conn.close()
        QtWidgets.QMessageBox.information(self, "Saved",
                                          f"Token(s) saved and added to documents (affected entries: {saved_count}).")
        # refresh saved tab
        self.refresh_saved_tab()

    def _filter_or_jump_token(self):
        """Highlight the first token matching the text."""
        text = self.search_token_edit.text().strip().lower()
        if not text:
            # Clear selection
            self.table_tokens.clearSelection()
            return

        rows = self.table_tokens.rowCount()
        for row in range(rows):
            token_item = self.table_tokens.item(row, 1)  # column 1 = Token
            if token_item and text in token_item.text().lower():
                self.table_tokens.selectRow(row)
                self.table_tokens.scrollToItem(token_item)
                self._last_search_row = row
                return

    def _find_next_token(self):
        """Continue search from the next row."""
        text = self.search_token_edit.text().strip().lower()
        if not text:
            return

        start_row = (self._last_search_row + 1) % max(1, self.table_tokens.rowCount())
        rows = self.table_tokens.rowCount()
        for i in range(rows):
            row = (start_row + i) % rows
            token_item = self.table_tokens.item(row, 1)
            if token_item and text in token_item.text().lower():
                self.table_tokens.selectRow(row)
                self.table_tokens.scrollToItem(token_item)
                self._last_search_row = row
                return
        QtWidgets.QMessageBox.information(self, "Find Token", f"'{text}' not found.")

    def on_token_cell_double_clicked(self, row, col):
        """When user double-clicks on token table, show preview for corresponding document(s)."""
        # Only handle clicks on "Rows (indices)" column (index 4)
        if col != 4:
            return

        cell = self.table_tokens.item(row, col)
        if not cell:
            return

        text = cell.text().strip()
        if not text:
            return

        # Parse comma-separated indices
        try:
            indices = [int(x.strip()) - 1 for x in text.split(",") if x.strip().isdigit()]
        except Exception:
            return

        if not indices:
            return

        # If multiple rows, let user pick one
        if len(indices) > 1:
            items = [f"Row {i + 1}" for i in indices]
            item, ok = QtWidgets.QInputDialog.getItem(self, "Select Row", "This token appears in multiple documents:",
                                                      items, 0, False)
            if not ok or not item:
                return
            selected_index = indices[items.index(item)]
        else:
            selected_index = indices[0]

        # Show that document in preview and select it in main table
        self.view_table.selectRow(selected_index)
        self.view_table.scrollTo(self.view_table.model().index(selected_index, 0))

        # Manually trigger preview update (since selectionChanged may not fire)
        df_display = self.model_table._df
        if selected_index < 0 or selected_index >= len(df_display):
            return
        series = df_display.iloc[selected_index]
        html_parts = []

        def bold(k):
            return f"<b>{k}:</b> "

        for col in PRIORITY_COLUMNS:
            if col in series.index:
                html_parts.append(f"{bold(col)}{html.escape(str(series[col]))}<br/>")
        for col in series.index:
            if col not in PRIORITY_COLUMNS:
                html_parts.append(f"{bold(col)}{html.escape(str(series[col]))}<br/>")
        self.preview.setHtml("<br/>".join(html_parts))

    # ---------------------------
    # Tab 2: Saved Documents
    # ---------------------------
    def _setup_tab_saved(self):
        layout = QtWidgets.QVBoxLayout(self.tab_saved)
        # search bar & controls
        h = QtWidgets.QHBoxLayout()
        self.input_search = QtWidgets.QLineEdit()
        self.input_search.setPlaceholderText("Enter search keywords (full-text). Leave empty to list all.")
        h.addWidget(self.input_search)
        btn_search = QtWidgets.QPushButton("Search")
        btn_search.clicked.connect(self.on_search_clicked)
        h.addWidget(btn_search)
        # allow Enter to trigger search
        self.input_search.returnPressed.connect(self.on_search_clicked)
        self.lbl_paging = QtWidgets.QLabel("")
        h.addWidget(self.lbl_paging)
        layout.addLayout(h)

        # Table view
        self.table_saved = QtWidgets.QTableWidget(0, 8)
        self.table_saved.setHorizontalHeaderLabels([
            #            "ID", "Index", "ArticleTitle", "Authors", "PublicationYear", "CreationTime", "SubKeyWords", "Notes"])
            "ID", "Index", "ArticleTitle", "Authors", "Year", "CreationTime", "SubKeyWords", "Notes"])
        self.table_saved.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Interactive)
        self.table_saved.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_saved.cellDoubleClicked.connect(self.on_saved_double_click)
        layout.addWidget(self.table_saved, stretch=1)

        # pagination controls
        ph = QtWidgets.QHBoxLayout()
        self.btn_prev = QtWidgets.QPushButton("Prev")
        self.btn_next = QtWidgets.QPushButton("Next")
        self.btn_prev.clicked.connect(self.on_prev_page)
        self.btn_next.clicked.connect(self.on_next_page)
        ph.addStretch(1)
        ph.addWidget(self.btn_prev)
        ph.addWidget(self.btn_next)
        layout.addLayout(ph)

        # preview and notes in a resizable splitter
        self.saved_preview = ZoomableTextEdit()
        self.saved_preview.setReadOnly(True)
        self.notes_edit = ZoomableTextEdit()
        saved_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        saved_splitter.addWidget(self.saved_preview)
        saved_splitter.addWidget(self.notes_edit)
        saved_splitter.setStretchFactor(0, 1)
        saved_splitter.setStretchFactor(1, 1)
        layout.addWidget(saved_splitter, stretch=1)

        notes_btn_row = QtWidgets.QHBoxLayout()
        self.btn_save_note = QtWidgets.QPushButton("Save Note")
        self.btn_save_note.clicked.connect(self.on_save_note_clicked)
        notes_btn_row.addStretch(1)
        notes_btn_row.addWidget(self.btn_save_note)
        # Add Delete button to notes_btn_row below self.btn_save_note
        self.btn_delete_doc = QtWidgets.QPushButton("Delete Paper")
        self.btn_delete_doc.clicked.connect(self.on_delete_doc_clicked)
        notes_btn_row.addWidget(self.btn_delete_doc)
        layout.addLayout(notes_btn_row)

        # internal paging state
        self.current_page = 0
        self.page_size = 20
        self.total_docs = 0
        self.selected_saved_doc_id = None

      # Add tag suggestion scroll area
        self.notes_tag_suggestion_widget = QtWidgets.QWidget()
        self.notes_tag_layout = QtWidgets.QHBoxLayout(self.notes_tag_suggestion_widget)
        self.notes_tag_layout.setContentsMargins(0, 0, 0, 0)
        self.notes_tag_layout.setSpacing(5)

        # Wrap in scroll area
        self.notes_tag_scroll = QtWidgets.QScrollArea()
        self.notes_tag_scroll.setWidgetResizable(True)
        self.notes_tag_scroll.setWidget(self.notes_tag_suggestion_widget)
        self.notes_tag_scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.notes_tag_scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)

        layout.addWidget(self.notes_tag_scroll)
        self.notes_tag_scroll.hide()

        # initial load
        self.refresh_saved_tab()

    def refresh_saved_tab(self, search_query=None, page=0):
        conn = sqlite3.connect(DB_PATH)
        rows, total = fetch_documents_page(conn, page=page, page_size=self.page_size, search_query=search_query)
        conn.close()
        self.current_page = page
        self.total_docs = total
        # populate table
        self.table_saved.setRowCount(len(rows))
        for r, row in enumerate(rows):
            # row: id, ExcelIndex, ArticleTitle, Authors, Affiliations, PublicationYear, Abstracts,
            # AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, ExtraProperties, CreationTime, SubKeyWords, Notes
            rid = row[0]
            excel_idx = row[1]
            art = row[2]
            authors = row[3]
            pyear = row[5] if len(row) > 5 else ""
            ctime = row[12] if len(row) > 12 else ""
            subkw = row[13] if len(row) > 13 else "[]"
            notes_val = row[14] if len(row) > 14 else ""
            try:
                subkw_disp = "; ".join(json.loads(subkw))
            except Exception:
                subkw_disp = subkw
            items = [str(rid), ("" if excel_idx is None else str(excel_idx)), art, authors, pyear, ctime, subkw_disp,
                     notes_val or ""]
            for c, val in enumerate(items):
                it = QtWidgets.QTableWidgetItem(val)
                self.table_saved.setItem(r, c, it)
        # initial auto-size to content
        self.table_saved.resizeColumnsToContents()
        self.table_saved.resizeRowsToContents()
        # paging label
        start = self.current_page * self.page_size + 1 if total > 0 else 0
        end = min((self.current_page + 1) * self.page_size, total)
        self.lbl_paging.setText(f"Showing {start} - {end} of {total}")
        # reset preview
        self.saved_preview.clear()
        self.notes_edit.clear()
        self.selected_saved_doc_id = None

    def on_search_clicked(self):
        q = self.input_search.text().strip()
        self.current_page = 0
        self.refresh_saved_tab(search_query=q, page=0)

    def on_prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.refresh_saved_tab(search_query=self.input_search.text().strip(), page=self.current_page)

    def on_next_page(self):
        max_page = (self.total_docs - 1) // self.page_size if self.total_docs > 0 else 0
        if self.current_page < max_page:
            self.current_page += 1
            self.refresh_saved_tab(search_query=self.input_search.text().strip(), page=self.current_page)

    def on_saved_double_click(self, row, col):
        # show preview of clicked saved doc
        item = self.table_saved.item(row, 0)
        if not item:
            return
        doc_id = item.text()
        self.selected_saved_doc_id = int(doc_id)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT ArticleTitle, Authors, Affiliations, PublicationYear, Abstracts, AuthorKeywords, KeywordsPlus, WoSCategories, ResearchAreas, ExtraProperties, CreationTime, SubKeyWords, Notes FROM documents WHERE id = ?",
            (doc_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return
        art, authors, aff, pyr, abs_, authkw, kwplus, wosc, reas, extras, ctime, sub, notes = row
        html_parts = []

        def hbold(k):
            return f"<b>{k}:</b> "

        html_parts.append(f"{hbold('Article Title')}{html.escape(art)}<br/><br/>")
        html_parts.append(f"{hbold('Authors')}{html.escape(authors)}<br/><br/>")
        html_parts.append(f"{hbold('Affiliations')}{html.escape(aff)}<br/><br/>")
        html_parts.append(f"{hbold('Publication Year')}{html.escape(pyr)}<br/><br/>")
        html_parts.append(f"{hbold('Abstract')}{html.escape(abs_)}<br/><br/>")
        html_parts.append(f"{hbold('Author Keywords')}{html.escape(authkw)}<br/><br/>")
        html_parts.append(f"{hbold('Keywords Plus')}{html.escape(kwplus)}<br/><br/>")
        html_parts.append(f"{hbold('WoS Categories')}{html.escape(wosc)}<br/><br/>")
        html_parts.append(f"{hbold('Research Areas')}{html.escape(reas)}<br/><br/>")
        html_parts.append(f"{hbold('Sub KeyWords')}{html.escape(', '.join(json.loads(sub) if sub else []))}<br/><br/>")
        html_parts.append(f"{hbold('Extra Properties')}<pre>{html.escape(extras)}</pre><br/>")
        if notes:
            html_parts.append(f"{hbold('Notes')}<pre>{html.escape(notes)}</pre><br/>")
        self.saved_preview.setHtml("".join(html_parts))
        # populate notes editor
        self.notes_edit.setText(notes or "")
        # Show tag suggestions
        self.update_notes_tag_suggestions()

    def update_notes_tag_suggestions(self):
        """Show tag suggestions below notes_edit using RECOMMENDED_TAGS dict, grouped by category."""
        layout = self.notes_tag_layout
        # Remove widgets from layout
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        any_tags = False
        for group, taglist in RECOMMENDED_TAGS.items():
            if not taglist:
                continue
            any_tags = True
            cat_label = QtWidgets.QLabel(str(group)+":")
            cat_label.setStyleSheet('font-weight: bold; padding:1px 4px;')
            layout.addWidget(cat_label)
            for tag in taglist:
                btn = QtWidgets.QPushButton(tag)
                btn.setStyleSheet('padding:2px 6px; font-size: 12px;')
                btn.clicked.connect(lambda _, t=tag: self.insert_tag_into_notes(t))
                layout.addWidget(btn)
        layout.addStretch(1)
        if any_tags:
            self.notes_tag_scroll.show()
        else:
            self.notes_tag_scroll.hide()

    def insert_tag_into_notes(self, tag):
        """Append tag to notes (add '; ' if needed) at cursor pos, avoid duplicate."""
        edit = self.notes_edit
        text = edit.toPlainText().rstrip()
        tags = [t.strip() for t in text.split(';') if t.strip()]
        if tag in tags:
            # Don't add duplicate
            return
        if text == "":
            new_text = tag
        elif text.endswith(';') or text.endswith('; '):
            new_text = text + tag
        else:
            new_text = text + '; ' + tag
        edit.setText(new_text)
        edit.setFocus()
        # Optionally, move cursor to end
        cursor = edit.textCursor()
        cursor.movePosition(cursor.End)
        edit.setTextCursor(cursor)

    def on_save_note_clicked(self):
        if not self.selected_saved_doc_id:
            QtWidgets.QMessageBox.information(self, "No selection", "Please open a saved document preview first.")
            return
        new_note = self.notes_edit.toPlainText()
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE documents SET Notes=? WHERE id=?", (new_note, self.selected_saved_doc_id))
        conn.commit()
        conn.close()
        # Refresh preview area to reflect updated note
        # Simulate double-click to reload preview
        rows = self.table_saved.selectionModel().selectedRows()
        if rows:
            self.on_saved_double_click(rows[0].row(), 0)
        QtWidgets.QMessageBox.information(self, "Saved", "Note saved.")

    def on_delete_doc_clicked(self):
        selected = self.table_saved.currentRow()
        if selected < 0:
            QtWidgets.QMessageBox.warning(self, "No selection", "Please select a row in the table to delete.")
            return
        id_item = self.table_saved.item(selected, 0)  # Col 0 is ID
        if not id_item:
            QtWidgets.QMessageBox.warning(self, "No selection", "Could not determine the selected document's ID.")
            return
        doc_id = id_item.text()
        reply = QtWidgets.QMessageBox.question(self, "Delete Paper", f"Are you sure you want to permanently delete document ID {doc_id}?", QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if reply == QtWidgets.QMessageBox.Yes:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM documents WHERE id=?", (doc_id,))
            conn.commit()
            conn.close()
            # If using FTS or other indexes, may need to update there as well
            self.refresh_saved_tab()
            self.saved_preview.clear()
            self.notes_edit.clear()
            self.selected_saved_doc_id = None
            QtWidgets.QMessageBox.information(self, "Deleted", f"Document ID {doc_id} has been deleted.")

    # ---------------------------
    # Tab 3: Analytics
    # ---------------------------
    def _setup_tab_analytics(self):
        layout = QtWidgets.QVBoxLayout(self.tab_analytics)

        # Controls
        controls = QtWidgets.QHBoxLayout()
        self.keyword_input = QtWidgets.QLineEdit()
        self.keyword_input.setPlaceholderText("Enter keywords to analyze (separated by semicolons)")
        controls.addWidget(QtWidgets.QLabel("Keywords:"))
        controls.addWidget(self.keyword_input)

        # Add database index range controls
        self.db_index_start = QtWidgets.QLineEdit()
        self.db_index_start.setValidator(QtGui.QIntValidator(0, 1000000, self))
        self.db_index_start.setFixedWidth(80)
        self.db_index_start.setPlaceholderText("Start DB idx")
        controls.addWidget(QtWidgets.QLabel("DB Start:"))
        controls.addWidget(self.db_index_start)
        self.db_index_end = QtWidgets.QLineEdit()
        self.db_index_end.setValidator(QtGui.QIntValidator(0, 1000000, self))
        self.db_index_end.setFixedWidth(80)
        self.db_index_end.setPlaceholderText("End DB idx")
        controls.addWidget(QtWidgets.QLabel("DB End:"))
        controls.addWidget(self.db_index_end)

        self.btn_analyze = QtWidgets.QPushButton("Analyze")
        self.btn_analyze.clicked.connect(self.generate_analytics)
        controls.addWidget(self.btn_analyze)

        controls.addStretch()
        layout.addLayout(controls)

        # Charts area with scrollable widget
        self.scroll_area = QtWidgets.QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scroll_area.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)

        self.charts_container = QtWidgets.QWidget()
        self.charts_layout = QtWidgets.QVBoxLayout(self.charts_container)

        self.scroll_area.setWidget(self.charts_container)
        layout.addWidget(self.scroll_area)

        # Initial empty charts
        self._clear_charts()

    def _clear_charts(self):
        """Clear all charts and show placeholder text."""
        # Clear all existing widgets
        for i in reversed(range(self.charts_layout.count())):
            self.charts_layout.itemAt(i).widget().setParent(None)

        # Add placeholder
        placeholder = QtWidgets.QLabel("Enter keywords and click Analyze to generate charts")
        placeholder.setAlignment(QtCore.Qt.AlignCenter)
        placeholder.setStyleSheet("font-size: 16px; color: gray; padding: 50px;")
        self.charts_layout.addWidget(placeholder)

    def generate_analytics(self):
        """Generate separate pie chart and line chart for each keyword."""
        keywords_text = self.keyword_input.text().strip()
        if not keywords_text:
            QtWidgets.QMessageBox.information(self, "No keywords", "Please enter keywords to analyze.")
            return

        # Split keywords by semicolon
        keywords = [k.strip() for k in keywords_text.split(';') if k.strip()]
        if not keywords:
            QtWidgets.QMessageBox.information(self, "No keywords", "Please enter valid keywords.")
            return

        # Clear existing charts
        self._clear_charts()

        # Get data from database
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Get all documents with their SubKeyWords and Notes and PublicationYear
        cur.execute("""
            SELECT SubKeyWords, Notes, PublicationYear FROM documents 
            WHERE (SubKeyWords IS NOT NULL AND SubKeyWords != '[]')
               OR (Notes IS NOT NULL AND Notes != '')
        """)
        rows = cur.fetchall()
        conn.close()

        # Process data for each keyword separately
        keyword_data = {}
        for keyword in keywords:
            keyword_data[keyword] = {
                'total_count': 0,
                'yearly_data': {}
            }

        for subkw_json, notes, year in rows:
            try:
                year_str = str(year).strip() if year else "Unknown"
                subkeywords = json.loads(subkw_json) if subkw_json else []
                # Split semicolon-separated fields
                note_parts = [p.strip() for p in (notes or "").split(';') if p.strip()]

                for keyword in keywords:
                    k = keyword.lower()
                    counted = False
                    # SubKeyWords match (list)
                    for doc_kw in subkeywords:
                        if k in str(doc_kw).lower():
                            keyword_data[keyword]['total_count'] += 1
                            keyword_data[keyword]['yearly_data'][year_str] = keyword_data[keyword]['yearly_data'].get(
                                year_str, 0) + 1
                            counted = True
                            break
                    if counted:
                        continue
                    # Notes match (semicolon-separated)
                    if any(k in part.lower() for part in note_parts):
                        keyword_data[keyword]['total_count'] += 1
                        keyword_data[keyword]['yearly_data'][year_str] = keyword_data[keyword]['yearly_data'].get(
                            year_str, 0) + 1
            except Exception:
                continue

        # Compose document list of all found papers (dedup by id, match any keyword)
        doc_list = []
        try:
            id_start = self.db_index_start.text()
            id_end = self.db_index_end.text()
            id_start = int(id_start) if id_start.strip() else None
            id_end = int(id_end) if id_end.strip() else None
        except Exception:
            id_start, id_end = None, None

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Compose query for DB id bounds
        sql_base = "SELECT id, ExcelIndex, ArticleTitle, Authors, PublicationYear, SubKeyWords, Notes FROM documents"
        if id_start is not None and id_end is not None:
            sql = sql_base + " WHERE id >= ? AND id < ?"
            params = (id_start, id_end)
        elif id_start is not None:
            sql = sql_base + " WHERE id >= ?"
            params = (id_start,)
        elif id_end is not None:
            sql = sql_base + " WHERE id < ?"
            params = (id_end,)
        else:
            sql = sql_base
            params = ()
        cur.execute(sql, params)
        all_docs = cur.fetchall()
        for row in all_docs:
            docid, excel_idx, art, authors, pyear, subkw_json, notes = row
            try:
                subkw_list = json.loads(subkw_json) if subkw_json else []
            except:
                subkw_list = []
            note_parts = [p.strip() for p in (notes or '').split(';') if p.strip()]
            matched = False
            for keyword in keywords:
                k = keyword.lower()
                if any(k in str(tag).lower() for tag in subkw_list):
                    matched = True
                elif any(k in part.lower() for part in note_parts):
                    matched = True
            if matched:
                doc_list.append({
                    'ExcelIndex': excel_idx,
                    'ArticleTitle': art,
                    'Authors': authors,
                    'PublicationYear': pyear,
                    'SubKeyWords': subkw_list,
                    'Notes': notes or ''
                })
        conn.close()

        # Clear analytics UI and create a new splitter to prevent stale widgets
        while self.charts_layout.count() > 0:
            item = self.charts_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)
                widget.deleteLater()

        self.analytics_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)

        # Table (top)
        self._display_analytics_table(doc_list)

        # Chart widget (bottom)
        chart_widget = QtWidgets.QWidget()
        summary_layout = QtWidgets.QVBoxLayout(chart_widget)
        # Add title
        title_label = QtWidgets.QLabel("Overall Keyword Distribution")
        title_label.setStyleSheet("font-size: 18px; font-weight: bold; padding: 10px;")
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        summary_layout.addWidget(title_label)
        # Create charts row
        charts_row = QtWidgets.QHBoxLayout()
        # Pie chart
        pie_figure = Figure(figsize=(6, 5))
        pie_canvas = FigureCanvas(pie_figure)
        ax_pie = pie_figure.add_subplot(111)
        labels = []
        sizes = []
        colors = []
        for i, (keyword, data) in enumerate(keyword_data.items()):
            if data['total_count'] > 0:
                labels.append(f"{keyword}\n({data['total_count']} papers)")
                sizes.append(data['total_count'])
                colors.append(plt.cm.Set3(i))
        if sizes:
            ax_pie.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90)
            ax_pie.set_title('Keyword Distribution\n(All Keywords Combined)')
        else:
            ax_pie.text(0.5, 0.5, 'No data found', ha='center', va='center',
                        transform=ax_pie.transAxes, fontsize=12)
            ax_pie.set_title('Keyword Distribution')
        charts_row.addWidget(pie_canvas)
        # Line chart
        line_figure = Figure(figsize=(6, 5))
        line_canvas = FigureCanvas(line_figure)
        ax_line = line_figure.add_subplot(111)
        has_data = False

        for i, (keyword, data) in enumerate(keyword_data.items()):
            if data['yearly_data']:
                # Convert valid numeric string years to integers for sorting
                numeric_years = sorted(
                    [int(y) for y in data['yearly_data'].keys() if y.isdigit()]
                )
                if numeric_years:
                    # Get counts using string keys
                    counts = [data['yearly_data'][str(y)] for y in numeric_years]
                    ax_line.plot(
                        numeric_years, counts,
                        marker='o', linewidth=2,
                        label=keyword, color=plt.cm.Set3(i)
                    )
                    has_data = True
        if has_data:
            ax_line.set_xlabel('Year')
            ax_line.set_ylabel('Number of Papers')
            ax_line.set_title('Yearly Trends (All Keywords)')
            ax_line.legend()
            ax_line.grid(True, alpha=0.3)
            plt.setp(ax_line.get_xticklabels(), rotation=45)
        else:
            ax_line.text(0.5, 0.5, 'No yearly data', ha='center', va='center',
                         transform=ax_line.transAxes, fontsize=12)
            ax_line.set_title('Yearly Trends')
        charts_row.addWidget(line_canvas)
        summary_layout.addLayout(charts_row)
        # Add chart widget to splitter
        self.analytics_splitter.addWidget(chart_widget)
        # Add splitter to layout
        self.charts_layout.addWidget(self.analytics_splitter)
        self.analytics_splitter.setSizes([300, 500])

        # Show summary (use doc_list & keyword_data only)
        total_papers = sum(data['total_count'] for data in keyword_data.values())
        summary_text = f"Analysis Complete!\n\nTotal papers found: {len(doc_list)}\n\n"
        for keyword, data in keyword_data.items():
            summary_text += f"{keyword}: {data['total_count']} papers\n"

        QtWidgets.QMessageBox.information(self, "Analysis Complete", summary_text)

    def _display_analytics_table(self, doc_list):
        # If table already in splitter, remove it
        if hasattr(self, 'analytics_splitter') and self.analytics_splitter.widget(0):
            self.analytics_splitter.widget(0).setParent(None)
        # Setup table
        table = QtWidgets.QTableWidget(len(doc_list), 6)
        table.setHorizontalHeaderLabels(["Index", "Title", "Authors", "Year", "SubKeyWords", "Notes"])
        # For cross-tab selection
        self._analytics_table_docrows = []
        for r, doc in enumerate(doc_list):
            items = [
                str(doc.get('ExcelIndex', '')),
                doc.get('ArticleTitle', ''),
                doc.get('Authors', ''),
                doc.get('PublicationYear', ''),
                ", ".join(doc.get('SubKeyWords', [])),
                doc.get('Notes', '')
            ]
            self._analytics_table_docrows.append(doc)
            for c, val in enumerate(items):
                it = QtWidgets.QTableWidgetItem(val)
                it.setFlags(it.flags() & ~QtCore.Qt.ItemIsEditable)
                table.setItem(r, c, it)
        table.resizeColumnsToContents()
        table.resizeRowsToContents()
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.itemSelectionChanged.connect(lambda: self._on_analytics_row_selected(table))
        self.analytics_splitter.insertWidget(0, table)

    def _on_analytics_row_selected(self, table):
        if not table.selectedItems():
            return
        row = table.currentRow()
        if row < 0 or row >= len(self._analytics_table_docrows):
            return

        doc = self._analytics_table_docrows[row]

        # Get all relevant search criteria from the selected row
        search_criteria = {
            'ArticleTitle': doc.get('ArticleTitle', ''),
            'Authors': doc.get('Authors', ''),
            'Year': doc.get('PublicationYear', ''),
            'SubKeyWords': ", ".join(doc.get('SubKeyWords', [])),
            'Notes': doc.get('Notes', '')
        }
        # print(doc)
        # Switch to Saved Documents tab
        self.tabs.setCurrentIndex(1)
        self.input_search.setText(doc.get('ArticleTitle', ''))  # Clear search box
        self.refresh_saved_tab(search_query=self.input_search.text().strip(), page=0)
        self.on_saved_double_click(0, 0)  # Show first row preview
        # def find_and_select_matching_row():
        #     table_saved = self.table_saved
        #     # Get the column indices for each field in the saved documents table
        #     col_indices = {
        #         'ArticleTitle': 2,  # Known column position in saved documents table
        #         'Authors': 3,
        #         'Year': 4,
        #         'SubKeyWords': 6,
        #         'Notes': 7
        #     }

        #     # Search through all rows in saved documents table
        #     for i in range(table_saved.rowCount()):
        #         matches = 0
        #         total_criteria = 0

        #         for field, search_value in search_criteria.items():
        #             if not search_value:  # Skip empty criteria
        #                 continue

        #             total_criteria += 1
        #             col_idx = col_indices.get(field)
        #             if col_idx is None:
        #                 continue

        #             cell_item = table_saved.item(i, col_idx)
        #             if cell_item and search_value in cell_item.text():
        #                 matches += 1

        #         # If most criteria match (allow for some differences due to formatting)
        #         if total_criteria > 0 and matches / total_criteria > 0.5:
        #             table_saved.selectRow(i)
        #             self.on_saved_double_click(i, 0)
        #             break

        # Give UI time to update, then search and select
        # QtCore.QTimer.singleShot(150, find_and_select_matching_row)


# ---------------------------
# Main run
# ---------------------------
def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.showMaximized()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
