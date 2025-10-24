from PyQt5 import QtWidgets, QtCore

class RatioTableWidget(QtWidgets.QTableWidget):
    def __init__(self, rows=0, cols=0, parent=None):
        super().__init__(rows, cols, parent)
        self._column_ratios = []
        self._lock_resize = False

        header = self.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.Fixed)  # cố định width
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        # Khi người dùng kéo cột → cập nhật lại tỉ lệ
        header.sectionResized.connect(self._on_section_resized)

    # ⚙️ Thiết lập tỉ lệ cột (tổng ~1.0)
    def setColumnRatios(self, ratios):
        if isinstance(ratios, dict):
            ratios = list(ratios.values())
        self._column_ratios = ratios
        self.updateColumnWidths()

    # 📏 Khi resize → tự động chia lại theo tỉ lệ
    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.updateColumnWidths()

    # 🧩 Cập nhật chiều rộng cột theo tỉ lệ
    def updateColumnWidths(self):
        if not self._column_ratios or self._lock_resize:
            return
        total_width = self.viewport().width()
        for i, ratio in enumerate(self._column_ratios):
            if i < self.columnCount():
                self.setColumnWidth(i, int(total_width * ratio))

    # 📐 Cập nhật lại tỉ lệ nếu người dùng kéo tay
    def _on_section_resized(self, logicalIndex, oldSize, newSize):
        if self._lock_resize or not self._column_ratios or self.columnCount() == 0:
            return
        total_width = sum(self.columnWidth(i) for i in range(self.columnCount()))
        if total_width == 0:
            return
        self._column_ratios = [
            self.columnWidth(i) / total_width for i in range(self.columnCount())
        ]

    # 🧱 Khi set dữ liệu xong → ép lại tỉ lệ (tránh Qt tự resize)
    def lockAndRefresh(self):
        """Gọi sau khi set dữ liệu để đảm bảo cột giữ tỉ lệ"""
        self._lock_resize = True
        self._lock_resize = False
        self.updateColumnWidths()
        self.resizeRowsToContents()
