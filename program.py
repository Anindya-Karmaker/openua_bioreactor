import sys
import os
import configparser
import sqlite3
import datetime
import time
from functools import partial

# --- Determine the base directory of the script to ensure files are saved in the correct place ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# PySide6 components for the GUI
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QLabel, QLineEdit, QPushButton, QFileDialog, QMessageBox,
    QSpinBox, QColorDialog, QCheckBox, QGroupBox, QDialog, QDateTimeEdit,
    QDoubleSpinBox
)
from PySide6.QtCore import QThread, Signal, Qt, QTimer, QDateTime
from PySide6.QtGui import QColor, QFont

# Third-party libraries for plotting and OPC UA
import pyqtgraph as pg
import pyqtgraph.exporters
import pandas as pd
from opcua import Client


# #############################################################################
# UTILITY - GMP-INSPIRED LOGGER
# #############################################################################

def log_event(message):
    """Logs an event to a file with a timestamp for audit purposes."""
    try:
        log_path = os.path.join(BASE_DIR, "gmp_audit_log.txt")
        with open(log_path, "a") as log_file:
            log_file.write(f"{datetime.datetime.now().isoformat()} - {message}\n")
    except Exception as e:
        print(f"Failed to write to audit log: {e}")


# #############################################################################
# CORE LOGIC - CONFIGURATION and DATABASE (with bulk insert)
# #############################################################################

class ConfigManager:
    """Handles reading and writing the application's configuration file."""
    def __init__(self, filename="config.ini"):
        self.filename = os.path.join(BASE_DIR, filename)
        self.config = configparser.ConfigParser()
        if not os.path.exists(self.filename):
            self._create_default_config()
        else:
            self.config.read(self.filename)
            if self._validate_and_update_config():
                self.save_config()
                log_event("INFO: Configuration file was updated with new default values.")

    def _get_default_config(self):
        default_parser = configparser.ConfigParser()
        default_parser['OPC_SERVER'] = {'address': 'opc.tcp://localhost:4840/freeopcua/server/'}
        default_parser['SETTINGS'] = {'polling_interval_ms': '1000'}
        default_parser['TAGS'] = {'ph_name': 'pH', 'ph_nodeid': 'ns=2;i=2', 'ph_setpoint_nodeid': 'ns=2;i=10', 'do_name': 'DO', 'do_nodeid': 'ns=2;i=3', 'do_setpoint_nodeid': 'ns=2;i=11', 'temp_name': 'Temperature', 'temp_nodeid': 'ns=2;i=4', 'temp_setpoint_nodeid': 'ns=2;i=12', 'variable1_name': 'Variable 1', 'variable1_nodeid': 'ns=2;i=5', 'variable2_name': 'Variable 2', 'variable2_nodeid': 'ns=2;i=6', 'variable3_name': 'Variable 3', 'variable3_nodeid': 'ns=2;i=7', 'variable4_name': 'Variable 4', 'variable4_nodeid': 'ns=2;i=8', 'variable5_name': 'Variable 5', 'variable5_nodeid': 'ns=2;i=13', 'variable6_name': 'Variable 6', 'variable6_nodeid': 'ns=2;i=14', 'variable7_name': 'Variable 7', 'variable7_nodeid': 'ns=2;i=15', 'run_start_nodeid': 'ns=2;i=9'}
        default_parser['PLOT_COLORS'] = {'ph_color': '#1f77b4', 'ph_setpoint_color': '#aec7e8', 'do_color': '#ff7f0e', 'do_setpoint_color': '#ffbb78', 'temp_color': '#d62728', 'temp_setpoint_color': '#ff9896', 'variable1_color': '#2ca02c', 'variable2_color': '#98df8a', 'variable3_color': '#9467bd', 'variable4_color': '#c5b0d5', 'variable5_color': '#8c564b', 'variable6_color': '#c49c94', 'variable7_color': '#e377c2'}
        default_parser['AXIS_LIMITS'] = {'ph_ymin': '6', 'ph_ymax': '8', 'do_ymin': '0', 'do_ymax': '100', 'temp_ymin': '20', 'temp_ymax': '40', 'variable_ymin': '0', 'variable_ymax': '50'}
        default_parser['UI_STATE'] = {'variable1': 'false', 'variable2': 'false', 'variable3': 'false', 'variable4': 'false', 'variable5': 'false', 'variable6': 'false', 'variable7': 'false'}
        return default_parser

    def _create_default_config(self):
        self.config = self._get_default_config(); self.save_config()
    def _validate_and_update_config(self):
        default_config = self._get_default_config(); was_updated = False
        for section in default_config.sections():
            if not self.config.has_section(section): self.config.add_section(section); was_updated = True
            for key, value in default_config.items(section):
                if not self.config.has_option(section, key): self.config.set(section, key, value); was_updated = True
        return was_updated
    def get_config(self):
        self.config.read(self.filename); return self.config
    def save_config(self):
        with open(self.filename, 'w') as configfile: self.config.write(configfile); log_event("INFO: Configuration saved.")

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path; self.conn = sqlite3.connect(self.db_path, check_same_thread=False); self.conn.execute("PRAGMA journal_mode=WAL;"); self._create_table(); log_event(f"INFO: Database connection established for '{self.db_path}'.")
    def _create_table(self):
        cursor = self.conn.cursor(); cursor.execute("""CREATE TABLE IF NOT EXISTS sensordata (timestamp REAL PRIMARY KEY, ph REAL, ph_setpoint REAL, do REAL, do_setpoint REAL, temperature REAL, temp_setpoint REAL, variable1 REAL, variable2 REAL, variable3 REAL, variable4 REAL, variable5 REAL, variable6 REAL, variable7 REAL, bioreactor_status TEXT)"""); self.conn.commit()
    
    def insert_bulk_data(self, data_list):
        """ **NEW** Inserts a list of data dictionaries in a single transaction. """
        if not data_list:
            return
        try:
            cursor = self.conn.cursor()
            # Assume all dicts have the same keys, get them from the first item
            keys = sorted(data_list[0].keys())
            placeholders = f"({', '.join(['?'] * len(keys))})"
            
            # Convert list of dicts to list of tuples
            rows_to_insert = [[row.get(k) for k in keys] for row in data_list]
            
            sql = f"INSERT OR IGNORE INTO sensordata ({', '.join(keys)}) VALUES {placeholders}"
            cursor.executemany(sql, rows_to_insert)
            self.conn.commit()
            log_event(f"INFO: Flushed {len(rows_to_insert)} records to '{self.db_path}'.")
        except Exception as e:
            log_event(f"ERROR: Database bulk insert failed in '{self.db_path}': {e}")

    def get_all_data_as_dataframe(self):
        try:
            query = "SELECT * FROM sensordata ORDER BY timestamp ASC"; df = pd.read_sql_query(query, self.conn); return df
        except Exception as e: log_event(f"ERROR: Failed to read all data from '{self.db_path}': {e}"); return pd.DataFrame()
    def export_to_excel(self, output_path, start_ts, end_ts, interval_s, config):
        try:
            query = f"SELECT * FROM sensordata WHERE timestamp BETWEEN {start_ts} AND {end_ts} ORDER BY timestamp ASC"; df = pd.read_sql_query(query, self.conn)
            if df.empty: return False, "No data found in the selected time range."
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s'); df.set_index('datetime', inplace=True)
            if interval_s > 0: df = df.resample(f'{interval_s}S').mean().reset_index()
            start_event_df = pd.read_sql_query("SELECT timestamp FROM sensordata WHERE bioreactor_status = 'STARTED' ORDER BY timestamp ASC LIMIT 1", self.conn); start_timestamp = start_event_df['timestamp'].iloc[0] if not start_event_df.empty else None
            if start_timestamp: df['EFT_seconds'] = (df['datetime'] - pd.to_datetime(start_timestamp, unit='s')).dt.total_seconds()
            else: df['EFT_seconds'] = "N/A (Reactor not started)"
            tags_config = config['TAGS']; rename_map = {key.replace('_name',''): name for key, name in tags_config.items() if key.endswith('_name')}; rename_map.update({f"{key.replace('_name','')}_setpoint": f"{name} SP" for key, name in tags_config.items() if key.endswith('_name')}); df.rename(columns=rename_map, inplace=True)
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Data', index=False); notes_text = f"Data export from BIOne OPC Logger.\nExported Range: {datetime.datetime.fromtimestamp(start_ts)} to {datetime.datetime.fromtimestamp(end_ts)}.\nData Interval: {interval_s} seconds."; pd.DataFrame({'Notes': [notes_text]}).to_excel(writer, sheet_name='Notes', index=False)
            log_event(f"INFO: Data successfully exported to {output_path}"); return True, "Export successful."
        except Exception as e: log_event(f"ERROR: Failed to export data to Excel: {e}"); return False, f"An error occurred: {e}"

class OpcClientThread(QThread):
    data_received = Signal(dict); status_changed = Signal(str); reactor_started = Signal(float)
    def __init__(self, config, db_path):
        super().__init__(); self.config = config; self.db_path = db_path; self.running = False; self.reactor_start_time = None; self.data_cache = []
    
    def run(self):
        self.db_manager = DatabaseManager(self.db_path)
        self.running = True; is_connected = False; client = None
        
        # --- Caching Timer ---
        self.db_timer = QTimer()
        self.db_timer.setInterval(30000) # 30 seconds
        self.db_timer.timeout.connect(self._flush_cache_to_db)
        
        address = self.config['OPC_SERVER']['address']; tags_config = self.config['TAGS']
        try: poll_ms = self.config.getint('SETTINGS', 'polling_interval_ms')
        except (ValueError, configparser.NoOptionError): poll_ms = 1000; log_event(f"WARNING: Invalid 'polling_interval_ms' in config. Using default {poll_ms}ms.")
        poll_interval_s = poll_ms / 1000.0
        node_ids_to_poll = {name.replace('_nodeid', '').replace('_name', ''): nid for name, nid in tags_config.items() if nid and name.endswith('_nodeid')}
        
        try:
            client = Client(address); client.connect(); is_connected = True; self.status_changed.emit(f"Connected to {address}"); log_event(f"OPC-UA: Successfully connected to {address}."); nodes = {name: client.get_node(nid) for name, nid in node_ids_to_poll.items()}
            self.db_timer.start() # Start the cache flushing timer
            
            while self.running:
                data = {'timestamp': time.time()}; status_note = None
                for name, node in nodes.items():
                    try:
                        if name == 'run_start' and not self.reactor_start_time:
                            if node.get_value() == 1: self.reactor_start_time = data['timestamp']; self.reactor_started.emit(self.reactor_start_time); status_note = "STARTED"
                        else: data[name] = node.get_value()
                    except Exception: data[name] = None
                if status_note: data['bioreactor_status'] = status_note
                self.data_received.emit(data)
                self.data_cache.append(data) # Add to cache instead of DB
                time.sleep(poll_interval_s)

        except Exception as e: self.status_changed.emit(f"Connection Failed: {e}"); log_event(f"ERROR: OPC client thread failed. Details: {e}")
        finally:
            self.db_timer.stop()
            self._flush_cache_to_db() # Final flush to save any remaining data
            if is_connected and client: client.disconnect()
            self.status_changed.emit("Disconnected")

    def _flush_cache_to_db(self):
        """Thread-safe method to write the cache to the database."""
        if not self.data_cache:
            return
        
        # Make a copy and clear the original immediately to avoid race conditions
        cache_snapshot = self.data_cache[:]
        self.data_cache.clear()
        
        self.db_manager.insert_bulk_data(cache_snapshot)

    def stop(self): self.running = False


# #############################################################################
# GUI - CLASS DEFINITIONS FOR TABS AND DIALOGS
# #############################################################################

class SettingsTab(QWidget):
    """Settings configuration tab."""
    settings_saved = Signal()
    def __init__(self, config_manager):
        super().__init__(); self.config_manager = config_manager; self.tag_widgets = {}; self.color_buttons = {}; self.axis_widgets = {}; layout = QVBoxLayout(self); self._init_server_settings(layout); self._init_tag_settings(layout); self._init_axis_settings(layout); self._init_connection_controls(layout); layout.addStretch(1); self.load_settings()
    def _init_server_settings(self, p):
        gb = QGroupBox("Server & Polling"); l = QGridLayout(); l.addWidget(QLabel("<b>OPC UA Server Address:</b>"), 0, 0); self.opc_address_edit = QLineEdit(); l.addWidget(self.opc_address_edit, 0, 1); l.addWidget(QLabel("<b>Data Polling Interval (ms):</b>"), 1, 0); self.polling_edit = QSpinBox(); self.polling_edit.setRange(100, 60000); l.addWidget(self.polling_edit, 1, 1); gb.setLayout(l); p.addWidget(gb)
    def _init_tag_settings(self, p):
        gb = QGroupBox("Tag & Color Configuration"); l = QGridLayout(); l.addWidget(QLabel("<b>Parameter</b>"), 0, 0); l.addWidget(QLabel("<b>Display Name</b>"), 0, 1); l.addWidget(QLabel("<b>PV Node ID</b>"), 0, 2); l.addWidget(QLabel("<b>SP Node ID</b>"), 0, 3); l.addWidget(QLabel("<b>Color (PV / SP)</b>"), 0, 4); tags = ['ph', 'do', 'temp', 'variable1', 'variable2', 'variable3', 'variable4', 'variable5', 'variable6', 'variable7']
        for i, t in enumerate(tags): self._create_tag_row(l, t, i + 1)
        gb.setLayout(l); p.addWidget(gb)
    def _create_tag_row(self, l, k, r):
        l.addWidget(QLabel(f"{k.replace('_', ' ').title()}:"), r, 0); n, p, s = QLineEdit(), QLineEdit(), QLineEdit(); l.addWidget(n, r, 1); l.addWidget(p, r, 2); l.addWidget(s, r, 3); self.tag_widgets[k] = {'name': n, 'pv_node': p, 'sp_node': s}; cl = QHBoxLayout(); pv_b = QPushButton(); pv_b.setFixedSize(60, 25); pv_b.clicked.connect(lambda: self._pick_color(k)); cl.addWidget(pv_b); self.color_buttons[k] = pv_b
        if k in ['ph', 'do', 'temp']: sp_b = QPushButton(); sp_b.setFixedSize(60, 25); sp_b.clicked.connect(lambda: self._pick_color(f"{k}_setpoint")); cl.addWidget(sp_b); self.color_buttons[f"{k}_setpoint"] = sp_b
        else: s.setEnabled(False)
        l.addLayout(cl, r, 4)
    def _init_axis_settings(self, p):
        gb = QGroupBox("Plot Axis Limits"); l = QGridLayout(); l.addWidget(QLabel("<b>Axis Group</b>"), 0, 0); l.addWidget(QLabel("<b>Y-Min</b>"), 0, 1); l.addWidget(QLabel("<b>Y-Max</b>"), 0, 2); axes = ['ph', 'do', 'temp', 'variable']
        for i, a in enumerate(axes):
            l.addWidget(QLabel(f"{a.title()}:"), i+1, 0); mn, mx = QDoubleSpinBox(), QDoubleSpinBox(); mn.setRange(-10000, 10000); mx.setRange(-10000, 10000); mn.setDecimals(2); mx.setDecimals(2); l.addWidget(mn, i+1, 1); l.addWidget(mx, i+1, 2); self.axis_widgets[a] = {'min': mn, 'max': mx}
        gb.setLayout(l); p.addWidget(gb)
    def _init_connection_controls(self, p):
        gb = QGroupBox("Controls"); l = QGridLayout(); sb = QPushButton("Save All Settings"); sb.clicked.connect(self.save_settings); l.addWidget(sb, 0, 0, 1, 3); self.start_button = QPushButton("Start Client"); self.stop_button = QPushButton("Stop Client"); self.stop_button.setEnabled(False); self.status_label = QLabel("Status: Disconnected"); self.status_label.setStyleSheet("font-style: italic;"); l.addWidget(self.start_button, 1, 0); l.addWidget(self.stop_button, 1, 1); l.addWidget(self.status_label, 1, 2); gb.setLayout(l); p.addWidget(gb)
    def _pick_color(self, k):
        b = self.color_buttons[k]; s = b.styleSheet(); c = "#ffffff";
        if 'background-color' in s: c = s.split(':')[1].strip().rstrip(';')
        color = QColorDialog.getColor(QColor(c), self);
        if color.isValid(): b.setStyleSheet(f"background-color: {color.name()};")
    def load_settings(self):
        c = self.config_manager.get_config()
        self.opc_address_edit.setText(c.get('OPC_SERVER', 'address', fallback=''))
        try: self.polling_edit.setValue(c.getint('SETTINGS', 'polling_interval_ms'))
        except (ValueError, configparser.NoOptionError): self.polling_edit.setValue(1000)
        for k, w in self.tag_widgets.items(): w['name'].setText(c.get('TAGS', f'{k}_name', fallback='')); w['pv_node'].setText(c.get('TAGS', f'{k}_nodeid', fallback='')); w['sp_node'].setText(c.get('TAGS', f'{k}_setpoint_nodeid', fallback=''))
        for k, b in self.color_buttons.items(): b.setStyleSheet(f"background-color: {c.get('PLOT_COLORS', f'{k}_color', fallback='#ffffff')};")
        for k, w in self.axis_widgets.items():
            try: w['min'].setValue(c.getfloat('AXIS_LIMITS', f'{k}_ymin'))
            except (ValueError, configparser.NoOptionError): w['min'].setValue(0)
            try: w['max'].setValue(c.getfloat('AXIS_LIMITS', f'{k}_ymax'))
            except (ValueError, configparser.NoOptionError): w['max'].setValue(100)
    def save_settings(self):
        c = self.config_manager.get_config(); c['OPC_SERVER']['address'] = self.opc_address_edit.text(); c['SETTINGS']['polling_interval_ms'] = str(self.polling_edit.value())
        for k, w in self.tag_widgets.items(): c.set('TAGS', f'{k}_name', w['name'].text()); c.set('TAGS', f'{k}_nodeid', w['pv_node'].text());
        if w['sp_node'].isEnabled(): c.set('TAGS', f'{k}_setpoint_nodeid', w['sp_node'].text())
        for k, b in self.color_buttons.items():
            s = b.styleSheet(); n = "#ffffff";
            if 'background-color' in s: n = s.split(':')[1].strip().rstrip(';')
            c.set('PLOT_COLORS', f'{k}_color', n)
        for k, w in self.axis_widgets.items(): c.set('AXIS_LIMITS', f'{k}_ymin', str(w['min'].value())); c.set('AXIS_LIMITS', f'{k}_ymax', str(w['max'].value()))
        self.config_manager.save_config(); self.settings_saved.emit(); QMessageBox.information(self, "Success", "Settings saved.\nGo to the Dashboard and use 'Apply Display Names & Colors' to update the plot live.")
    def update_status_label(self, s): self.status_label.setText(f"Status: {s}")

class AboutTab(QWidget):
    def __init__(self):
        super().__init__(); l = QVBoxLayout(self); l.setAlignment(Qt.AlignCenter); f1 = QFont(); f1.setPointSize(24); f1.setBold(True); f2 = QFont(); f2.setFamily("monospace"); f2.setPointSize(10); tl = QLabel("Created by Anindya Karmaker"); tl.setFont(f1); l.addWidget(tl, 0, Qt.AlignCenter); gl = QLabel("<a href='https://github.com/your-repo'>GitHub: [Placeholder - https://github.com/your-repo]</a>"); gl.setFont(QFont("Arial", 12)); gl.setTextInteractionFlags(Qt.TextBrowserInteraction); gl.setOpenExternalLinks(True); l.addWidget(gl, 0, Qt.AlignCenter); txt = """<p><b>The MIT License (MIT)</b></p><p>Copyright (c) 2025 Anindya Karmaker</p><p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:</p><p>The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.</p><p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.</p>"""
        ml = QLabel(txt); ml.setFont(f2); ml.setWordWrap(True); ml.setTextFormat(Qt.RichText); gb = QGroupBox("License Information"); glay = QVBoxLayout(); glay.addWidget(ml); gb.setLayout(glay); l.addSpacing(20); l.addWidget(gb); l.addStretch(1)

class DashboardTab(QWidget):
    """Dashboard tab for plotting and data export."""
    def __init__(self, main_window):
        super().__init__(); self.main_window = main_window; self.config_manager = main_window.config_manager; self.fermentation_start_time = None; self.time_data = []; self.plot_data = {}; self.lines = {}; self.checkboxes = {}; self.view_boxes = {}; self.axes = {}; self.optional_axis_map = {}; self.axis_auto_range_state = {}
        layout = QHBoxLayout(self); self._init_plot(); self._init_controls()
        layout.addWidget(self.plot_widget, 4); layout.addWidget(self.controls_group_box, 1)
        self.update_timer = QTimer(self); self.update_timer.setInterval(500); self.update_timer.timeout.connect(self.redraw_plot); self.update_timer.start()
        self.apply_settings()
        self.load_ui_state()
    def _init_plot(self):
        pg.setConfigOptions(antialias=True); self.plot_widget = pg.PlotWidget(); self.legend = self.plot_widget.addLegend(offset=(10, 30)); self.p1 = self.plot_widget.getPlotItem(); self.p1.setLabels(left='pH', bottom='Elapsed Fermentation Time (hours)'); self.p1.showAxis('top'); self.p1.getAxis('top').setStyle(showValues=False); self.p1.getAxis('top').setHeight(150)
        self.p_temp = pg.ViewBox(); self.ax_temp = pg.AxisItem('right'); self.p1.layout.addItem(self.ax_temp, 2, 3); self.p1.scene().addItem(self.p_temp); self.ax_temp.linkToView(self.p_temp); self.p_temp.setXLink(self.p1)
        self.p_do = pg.ViewBox(); self.ax_do = pg.AxisItem('right'); self.p1.layout.addItem(self.ax_do, 2, 4); self.p1.scene().addItem(self.p_do); self.ax_do.linkToView(self.p_do); self.p_do.setXLink(self.p1)
        self.p_opt1 = pg.ViewBox(); self.ax_opt1 = pg.AxisItem('right'); self.p1.layout.addItem(self.ax_opt1, 2, 5); self.p1.scene().addItem(self.p_opt1); self.ax_opt1.linkToView(self.p_opt1); self.p_opt1.setXLink(self.p1)
        self.p_opt2 = pg.ViewBox(); self.ax_opt2 = pg.AxisItem('right'); self.p1.layout.addItem(self.ax_opt2, 2, 6); self.p1.scene().addItem(self.p_opt2); self.ax_opt2.linkToView(self.p_opt2); self.p_opt2.setXLink(self.p1)
        self.p_opt3 = pg.ViewBox(); self.ax_opt3 = pg.AxisItem('right'); self.p1.layout.addItem(self.ax_opt3, 2, 7); self.p1.scene().addItem(self.p_opt3); self.ax_opt3.linkToView(self.p_opt3); self.p_opt3.setXLink(self.p1)
        self.p1.getViewBox().sigResized.connect(self._update_views)
        self.view_boxes = {'ph': self.p1.getViewBox(), 'temp': self.p_temp, 'do': self.p_do, 'opt1': self.p_opt1, 'opt2': self.p_opt2, 'opt3': self.p_opt3}
        self.axes = {'ph': self.p1.getAxis('left'), 'temp': self.ax_temp, 'do': self.ax_do, 'opt1': self.ax_opt1, 'opt2': self.ax_opt2, 'opt3': self.ax_opt3}
        for key in self.view_boxes.keys(): self.axis_auto_range_state[key] = False
        for i in range(1, 4): self.axes[f'opt{i}'].hide()
    def _init_controls(self):
        self.controls_group_box = QGroupBox("Plot Controls & Export"); controls_layout = QVBoxLayout()
        checkbox_group = QGroupBox("Optional Variables (Max 3)"); checkbox_layout = QGridLayout()
        self.optional_variable_keys = [f'variable{i}' for i in range(1, 8)]
        for i, key in enumerate(self.optional_variable_keys):
            checkbox_layout.addWidget(QLabel(key.replace('_', ' ').title()), i, 0); pv_checkbox = QCheckBox(); pv_checkbox.stateChanged.connect(self._on_checkbox_state_changed); self.checkboxes[key] = pv_checkbox; checkbox_layout.addWidget(pv_checkbox, i, 1, Qt.AlignCenter)
        checkbox_group.setLayout(checkbox_layout); controls_layout.addWidget(checkbox_group); controls_layout.addStretch(1)
        apply_style_btn = QPushButton("Apply Display Names & Colors"); apply_style_btn.clicked.connect(self.apply_settings); apply_axes_btn = QPushButton("Apply Manual Axis Limits"); apply_axes_btn.clicked.connect(self._apply_axis_limits); reset_axes_btn = QPushButton("Enable Auto-Ranging"); reset_axes_btn.clicked.connect(self._enable_auto_range_all); load_db_btn = QPushButton("Load & Visualize Database"); load_db_btn.clicked.connect(self.main_window.load_and_visualize_db); save_img_btn = QPushButton("Save Graph as Image"); save_img_btn.clicked.connect(self.save_graph_image); export_btn = QPushButton("Export Data..."); export_btn.clicked.connect(self.show_export_dialog)
        controls_layout.addWidget(apply_style_btn); controls_layout.addWidget(apply_axes_btn); controls_layout.addWidget(reset_axes_btn); controls_layout.addWidget(load_db_btn); controls_layout.addWidget(save_img_btn); controls_layout.addWidget(export_btn); self.controls_group_box.setLayout(controls_layout)
    def _on_checkbox_state_changed(self):
        checked_optionals = [key for key in self.optional_variable_keys if self.checkboxes.get(key) and self.checkboxes[key].isChecked()]
        if len(checked_optionals) > 3:
            QMessageBox.warning(self, "Plot Limit Reached", "You can only display a maximum of 3 optional variables at a time."); sender = self.sender();
            if sender: sender.blockSignals(True); sender.setChecked(False); sender.blockSignals(False)
            return
        self.update_optional_plots()
    def _update_views(self):
        for vb_key in ['temp', 'do', 'opt1', 'opt2', 'opt3']: self.view_boxes[vb_key].setGeometry(self.p1.getViewBox().sceneBoundingRect())
    def apply_settings(self):
        log_event("GUI: Applying display names and colors to plot."); self._clear_plot_items(); config = self.config_manager.get_config(); colors = config['PLOT_COLORS']; tags = config['TAGS']
        self.axes['ph'].setLabel(tags.get('ph_name'), color=colors.get('ph_color')); self.axes['temp'].setLabel(tags.get('temp_name'), color=colors.get('temp_color')); self.axes['do'].setLabel(tags.get('do_name'), color=colors.get('do_color'))
        for key in ['ph', 'temp', 'do']: self._create_or_update_line(key, tags.get(f'{key}_name'), colors.get(f'{key}_color'), key, False); self._create_or_update_line(f'{key}_setpoint', f"{tags.get(f'{key}_name')} SP", colors.get(f'{key}_setpoint_color'), key, True)
        for key in self.optional_variable_keys: self._create_or_update_line(key, tags.get(f'{key}_name'), colors.get(f'{key}_color'), None, False)
        self.update_optional_plots()
        self._apply_axis_limits()
    def _clear_plot_items(self):
        for line in self.lines.values():
            for view_box in self.view_boxes.values():
                if line in view_box.addedItems: view_box.removeItem(line)
        self.legend.clear(); self.lines.clear()
    def _create_or_update_line(self, key, name, color, axis_key, is_setpoint):
        pen = pg.mkPen(color, width=2, style=Qt.DashLine if is_setpoint else Qt.SolidLine)
        self.plot_data.setdefault(key, []); new_line = pg.PlotDataItem(pen=pen, name=name); self.lines[key] = new_line
        if axis_key: self.view_boxes[axis_key].addItem(self.lines[key])
    def update_optional_plots(self):
        config = self.config_manager.get_config(); tags = config['TAGS']; colors = config['PLOT_COLORS']
        for key in self.optional_variable_keys:
            if key in self.lines:
                for vb in [self.view_boxes['opt1'], self.view_boxes['opt2'], self.view_boxes['opt3']]:
                    if self.lines[key] in vb.addedItems: vb.removeItem(self.lines[key])
                self.lines[key].hide()
        for i in range(1, 4): self.axes[f'opt{i}'].hide(); self.axes[f'opt{i}'].setLabel('')
        checked_optionals = [key for key in self.optional_variable_keys if self.checkboxes.get(key) and self.checkboxes[key].isChecked()]
        for i, key in enumerate(checked_optionals[:3]):
            axis_key = f'opt{i+1}'; view_box = self.view_boxes[axis_key]; axis_item = self.axes[axis_key]; line_item = self.lines[key]; color = colors.get(f'{key}_color', '#FFFFFF')
            view_box.addItem(line_item); line_item.show(); axis_item.setLabel(tags.get(f'{key}_name'), color=color); axis_item.show()
        self._update_axes_autoranges()
    def _apply_axis_limits(self):
        config = self.config_manager.get_config(); limits = config['AXIS_LIMITS']; log_event("GUI: Manual axis limits applied.")
        for key, axis in self.axes.items():
            self.axis_auto_range_state[key.rstrip('123')] = False
            try:
                if key.startswith('opt'): lim_key = 'variable'
                else: lim_key = key
                vmin = float(limits.get(f'{lim_key}_ymin')); vmax = float(limits.get(f'{lim_key}_ymax')); axis.setRange(vmin, vmax)
            except (ValueError, TypeError, AttributeError): log_event(f"WARNING: Invalid axis limits for '{key}' in config.")
    def _enable_auto_range_all(self):
        log_event("GUI: All axes set to auto-range mode.")
        for key, view_box in self.view_boxes.items(): self.axis_auto_range_state[key] = True
        self._update_axes_autoranges()
    def _update_axes_autoranges(self):
        for key, view_box in self.view_boxes.items():
            if any(isinstance(item, pg.PlotDataItem) and item.isVisible() for item in view_box.allChildren()): view_box.enableAutoRange()
        log_event("GUI: Axes autorange updated.")
    def clear_all_data(self):
        self.time_data.clear();
        for key in self.plot_data: self.plot_data[key].clear()
        self.fermentation_start_time = None
        if hasattr(self, 'start_line_item'): self.p1.removeItem(self.start_line_item); del self.start_line_item
        self.redraw_plot(); log_event("GUI: All plot data cleared.")
    def display_historical_data(self, df):
        self.clear_all_data(); start_events = df[df['bioreactor_status'] == 'STARTED']
        if not start_events.empty: self.set_fermentation_start(start_events['timestamp'].iloc[0])
        self.time_data = df['timestamp'].tolist()
        for key in self.plot_data:
            if key in df.columns: self.plot_data[key] = df[key].tolist()
        self.redraw_plot(); self._enable_auto_range_all()
    def set_fermentation_start(self, timestamp):
        if self.fermentation_start_time is None: self.fermentation_start_time = timestamp; self.start_line_item = pg.InfiniteLine(pos=0, angle=90, movable=False, pen=pg.mkPen('red', width=3, style=Qt.DotLine), label="EFT Start"); self.p1.addItem(self.start_line_item); self.redraw_plot()
    def update_plot_data(self, data):
        self.time_data.append(data.get('timestamp'))
        for key in self.plot_data: self.plot_data[key].append(self._sanitize_value(data.get(key, None)))
    def _sanitize_value(self, value):
        if isinstance(value, (int, float)): return value
        return None
    def redraw_plot(self):
        if not self.time_data:
            for line in self.lines.values(): line.clear()
            return
        start_time = self.fermentation_start_time or self.time_data[0]
        eft_data_hours = [(t - start_time) / 3600.0 for t in self.time_data]
        for key, line in self.lines.items():
            if line.isVisible():
                if len(self.plot_data.get(key, [])) == len(eft_data_hours):
                    try: line.setData(eft_data_hours, self.plot_data[key])
                    except Exception as e: log_event(f"ERROR: Failed to update plot for key '{key}'. Details: {e}")
        for axis_key, is_auto in self.axis_auto_range_state.items():
            if is_auto:
                view_box = self.view_boxes.get(axis_key)
                if view_box and any(isinstance(item, pg.PlotDataItem) and item.isVisible() for item in view_box.allChildren()): view_box.enableAutoRange()
    def save_graph_image(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save Graph", "", "PNG (*.png);;JPG (*.jpg)");
        if path: exporter = pg.exporters.ImageExporter(self.plot_widget.plotItem); exporter.export(path); log_event(f"GUI: Graph saved to {path}")
    def show_export_dialog(self):
        dialog = ExportDialog(self)
        if dialog.exec():
            start_ts, end_ts, interval = dialog.get_values()
            path, _ = QFileDialog.getSaveFileName(self, "Save Exported Data", "", "Excel Files (*.xlsx)")
            if path: db_manager = DatabaseManager(self.main_window.current_db_path); success, msg = db_manager.export_to_excel(path, start_ts, end_ts, interval, self.config_manager.get_config()); QMessageBox.information(self, "Export Status", msg)
    def save_ui_state(self):
        config = self.config_manager.get_config()
        if not config.has_section('UI_STATE'): config.add_section('UI_STATE')
        for key in self.optional_variable_keys:
            if key in self.checkboxes: config.set('UI_STATE', key, str(self.checkboxes[key].isChecked()).lower())
        self.config_manager.save_config()
    def load_ui_state(self):
        config = self.config_manager.get_config()
        if not config.has_section('UI_STATE'): return
        for key in self.optional_variable_keys:
            if key in self.checkboxes:
                is_checked = config.getboolean('UI_STATE', key, fallback=False)
                self.checkboxes[key].setChecked(is_checked)
        self.update_optional_plots()

class ExportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent); self.setWindowTitle("Export Data Options"); layout = QGridLayout(self); self.start_time_edit = QDateTimeEdit(QDateTime.currentDateTime().addDays(-1)); self.end_time_edit = QDateTimeEdit(QDateTime.currentDateTime()); self.interval_spinbox = QSpinBox(); self.interval_spinbox.setRange(1, 3600); self.interval_spinbox.setValue(60); self.interval_spinbox.setSuffix(" seconds"); layout.addWidget(QLabel("Start Time:"), 0, 0); layout.addWidget(self.start_time_edit, 0, 1); layout.addWidget(QLabel("End Time:"), 1, 0); layout.addWidget(self.end_time_edit, 1, 1); layout.addWidget(QLabel("Data Interval:"), 2, 0); layout.addWidget(self.interval_spinbox, 2, 1); ok_button = QPushButton("OK"); ok_button.clicked.connect(self.accept); cancel_button = QPushButton("Cancel"); cancel_button.clicked.connect(self.reject); layout.addWidget(ok_button, 3, 0); layout.addWidget(cancel_button, 3, 1)
    def get_values(self):
        return self.start_time_edit.dateTime().toSecsSinceEpoch(), self.end_time_edit.dateTime().toSecsSinceEpoch(), self.interval_spinbox.value()


# #############################################################################
# GUI - MAIN WINDOW
# #############################################################################

class MainWindow(QMainWindow):
    """The main application window containing the tabbed interface."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BIOne Advanced OPC Data Logger - v7.6 Final")
        self.setGeometry(100, 100, 1800, 950)
        log_event("INFO: Application started.")
        self.config_manager = ConfigManager()
        self.opc_thread = None
        self.initial_connection_notified = False
        self.current_db_path = os.path.join(BASE_DIR, f"bione_data_{datetime.date.today().isoformat()}.sqlite")
        DatabaseManager(self.current_db_path) 
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.dashboard_tab = DashboardTab(self)
        self.settings_tab = SettingsTab(self.config_manager)
        self.about_tab = AboutTab()
        self.tabs.addTab(self.dashboard_tab, "Dashboard"); self.tabs.addTab(self.settings_tab, "Settings"); self.tabs.addTab(self.about_tab, "About")
        self.settings_tab.start_button.clicked.connect(self.start_opc_client); self.settings_tab.stop_button.clicked.connect(self.stop_opc_client)
        self.settings_tab.settings_saved.connect(self.dashboard_tab.apply_settings)

    def start_opc_client(self):
        if self.opc_thread and self.opc_thread.isRunning(): return
        self.initial_connection_notified = False; self.dashboard_tab.clear_all_data(); self.config_manager.get_config(); self.dashboard_tab.apply_settings()
        self.opc_thread = OpcClientThread(self.config_manager.config, self.current_db_path)
        self.opc_thread.data_received.connect(self.dashboard_tab.update_plot_data); self.opc_thread.status_changed.connect(self.settings_tab.update_status_label); self.opc_thread.status_changed.connect(self.handle_connection_status); self.opc_thread.reactor_started.connect(self.dashboard_tab.set_fermentation_start)
        self.opc_thread.finished.connect(self.on_thread_finished)
        self.opc_thread.start()
        self.settings_tab.start_button.setEnabled(False); self.settings_tab.stop_button.setEnabled(True)

    def stop_opc_client(self):
        """Tells the thread to stop without blocking the GUI."""
        if self.opc_thread and self.opc_thread.isRunning():
            log_event("INFO: Stop client requested by user.")
            self.opc_thread.stop()

    def on_thread_finished(self):
        """**FAIL-SAFE UI RESET** - Called when thread terminates for any reason."""
        log_event("INFO: OPC client thread has finished.")
        self.settings_tab.start_button.setEnabled(True)
        self.settings_tab.stop_button.setEnabled(False)
        self.opc_thread = None

    def handle_connection_status(self, status):
        if not self.initial_connection_notified:
            if status.startswith("Connected"): QMessageBox.information(self, "Connection Success", "Successfully connected to the OPC UA server."); self.initial_connection_notified = True
            elif status.startswith("Connection Failed"): QMessageBox.critical(self, "Connection Failed", f"Could not connect to the OPC UA server.\n\nDetails: {status}"); self.initial_connection_notified = True

    def load_and_visualize_db(self):
        self.stop_opc_client()
        path, _ = QFileDialog.getOpenFileName(self, "Load & Visualize Database", BASE_DIR, "SQLite Database Files (*.sqlite)")
        if not path: return
        log_event(f"GUI: Loading historical data from '{path}'.")
        db_manager = DatabaseManager(path)
        historical_df = db_manager.get_all_data_as_dataframe()
        if historical_df.empty: QMessageBox.warning(self, "Empty Database", "The selected database contains no data to visualize."); return
        self.dashboard_tab.display_historical_data(historical_df)
        self.tabs.setCurrentWidget(self.dashboard_tab)

    def closeEvent(self, event):
        log_event("INFO: Application closing.")
        self.dashboard_tab.save_ui_state()
        self.stop_opc_client()
        event.accept()


# #############################################################################
# APPLICATION ENTRY POINT
# #############################################################################

if __name__ == '__main__':
    log_event("=================== APPLICATION LAUNCH v7.6 ===================")
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())