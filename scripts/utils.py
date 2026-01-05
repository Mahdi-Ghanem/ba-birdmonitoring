# scripts/utils.py
from datetime import datetime
from pathlib import Path
import sys
import yaml
import logging
import pandas as pd

# --- KONFIGURATION LADEN ---
def load_config(path_str:str):
    """
    Datei lädt die Konfigurationen aus dem übergebenen Pfad.yaml
    """
    path = Path(path_str)
    if not path.exists():
        sys.exit(f"CRITICAL: Config file not found at {path}") #improve
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# --- HELFER: BirdNET Week 48 ---
def compute_week48(dt: datetime) -> int:
    """
    Berechnet die BirdNET-Woche (1-48).
    Schema: Tage 1-7=W1, 8-14=W2, 15-21=W3, Rest=W4.
    """
    day = dt.day
    if day <= 7:
        week_in_month = 1
    elif day <= 14:
        week_in_month = 2
    elif day <= 21:
        week_in_month = 3
    else:
        week_in_month = 4
    return (dt.month - 1) * 4 + week_in_month

# --- HELFER: Session (Morning/Evening) ---
def get_session(dt, morning_range, evening_range):
    """
    Berechne ob die Uhrzeit in einem Morgen oder Abendintervall liegt
    """
    h = dt.hour
    if morning_range[0] <= h <= morning_range[1]:
        return "morning"
    if evening_range[0] <= h <= evening_range[1]:
        return "evening"
    return "other"


def calculate_slot_with_tolerance(file_start_dt, sun_event_dt, event_name, tolerance_min=15):
    """
    Berechnet den Slot (z.B. sunrise_-120) mit Toleranz.
    Robust gegen Zeitzonen und Datentypen.
    """
    # 1. Sicherheits-Check
    if pd.isna(sun_event_dt) or pd.isna(file_start_dt):
        return None, None

    # 2. Zeitzonen entfernen
    try:
        dt_file = pd.to_datetime(file_start_dt).replace(tzinfo=None)
        dt_sun = pd.to_datetime(sun_event_dt).replace(tzinfo=None)
    except Exception:
        return None, None

    # 3. Mathe (Diff in Minuten)
    delta = dt_file - dt_sun
    diff_minutes = delta.total_seconds() / 60.0
    
    # 4. Welcher vollen Stunde sind wir am nächsten?
    # Wir runden auf Stundenbasis, um die "Slots" (60, 120, 180...) zu finden
    hour_offset = round(diff_minutes / 60.0)
    
    # 5. Abweichungs-Check
    expected_minutes = hour_offset * 60.0
    deviation = abs(diff_minutes - expected_minutes)
    
    # 6. Toleranz-Check
    if deviation <= tolerance_min:
        # ÄNDERUNG: Wir rechnen die Stunden zurück in Minuten für das Label
        # z.B. -2 * 60 = -120
        slot_minutes = int(hour_offset * 60)
        return f"{event_name}_{slot_minutes}", diff_minutes
    else:
        return None, diff_minutes




def setup_logger(name, log_file):
    """
    Erstellt einen Logger, der in die Konsole UND in eine Datei schreibt.
    
    Args:
        name (str): Der Name des Loggers (z.B. 'Inventory', 'BirdNET').
        log_file (str): Pfad zur Log-Datei.
    """
    # 1. Logger holen
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # WICHTIG: Prüfen, ob schon Handler da sind (verhindert doppelte Logs)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # 2. Formatierung definieren (Zeit | Level | Nachricht)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", 
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # 3. Datei-Handler (schreibt in project/logs/pipeline.log)
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True) # Ordner erstellen, falls weg
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 4. Konsolen-Handler (schreibt in dein Terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger