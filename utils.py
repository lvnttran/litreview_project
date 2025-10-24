#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import pandas as pd
from typing import List, Dict

# Simple tokenizer
WORD_RE = re.compile(r"[A-Za-z0-9'\-]+")  # includes words with apostrophes or dashes

def extract_tokens_from_field(value: str, field_name: str = None, lower: bool = True, remove_stopwords: bool = False, special_columns: set = None, english_stopwords: set = None) -> List[str]:
    """Return list of tokens or phrases, preserving multi-word keywords for special columns."""
    if value is None or pd.isna(value):
        return []
    value = str(value).strip()
    if not value:
        return []
    if lower:
        value = value.lower()

    # Special columns → split by semicolon and keep phrases intact
    if special_columns and field_name in special_columns:
        tokens = [p.strip() for p in value.split(";") if p.strip()]
        return tokens

    # Regular text → word-based tokenization
    tokens = WORD_RE.findall(value)
    tokens = [t.strip("-'") for t in tokens if len(t.strip("-'")) >= 2]
    if remove_stopwords and english_stopwords:
        tokens = [t for t in tokens if t not in english_stopwords]
    return tokens


def tokenise_text_row(row: pd.Series, columns: List[str], lower: bool = True, remove_stopwords: bool = False, special_columns: set = None, english_stopwords: set = None) -> List[str]:
    """Tokenize text from multiple columns of a pandas row using appropriate rules per column."""
    tokens = []
    for col in columns:
        if col not in row:
            continue
        val = row[col]
        tokens.extend(extract_tokens_from_field(val, field_name=col, lower=lower, remove_stopwords=remove_stopwords, 
                                             special_columns=special_columns, english_stopwords=english_stopwords))
    # Deduplicate but keep order
    return list(dict.fromkeys(tokens))


def build_extra_properties_from_row(row: pd.Series, keep_cols: List[str]) -> str:
    """Build extra properties string from row, excluding kept columns."""
    extras = []
    for col, val in row.items():
        if col in keep_cols:
            continue
        s = "" if pd.isna(val) else str(val)
        extras.append(f"{col}:\n{s}")
    return "\n\n".join(extras)
from datetime import datetime

def format_datetime_iso_to_ddmmyyyy(value: str) -> str:
    """
    Chuyển chuỗi datetime ISO (vd: '2025-10-22T12:16:12.636172+00:00')
    sang định dạng 'dd/mm/yyyy'.
    Trả về chuỗi rỗng nếu không hợp lệ hoặc None.
    """
    if not value:
        return ""
    try:
        # Xử lý trường hợp có hoặc không có timezone "Z"
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return value  # Giữ nguyên nếu không parse được
