#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
InsightHub - Document Analysis Tool
Main application entry point

This module serves as the entry point for the InsightHub application,
which provides document analysis and classification capabilities.

Dependencies:
    pip install pyqt5 pandas openpyxl
Run:
    python main.py
"""

import sys
from PyQt5 import QtWidgets
from insight_hub_ui import MainWindow
from db import init_db

def main():
    """Main entry point of the application."""
    app = QtWidgets.QApplication(sys.argv)
    init_db()  # Initialize database
    win = MainWindow()
    win.showMaximized()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
