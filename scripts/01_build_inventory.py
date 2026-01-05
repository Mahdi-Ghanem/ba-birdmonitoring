import sys
import yaml
import re
import logging
import pandas as pd
import soundfile as sf
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm

# Wir sagen Python: "Der Hauptordner ist eins weiter oben (project)"
sys.path.append(str(Path(__file__).parent.parent))

from scripts.utils import load_config, compute_week48, get_session, setup_logger, calculate_slot_with_tolerance

# --- HAUPTFUNKTION ---

def main():
    # 1. Config laden
    config_path = "config/pipeline.yaml"

    # 2. Hilfsfunktion nutzen. Jetzt haben wir die Konfigurationen geladen
    print(f"Lade Konfiguration: {config_path}")
    cfg = load_config(config_path)


    # 1. Logger initialisieren
    # Wir holen den Pfad aus der YAML (project/logs/pipeline.log)
    log_path = cfg["paths"]["pipeline_log"]
    logger = setup_logger("InventoryBuilder", log_path)
    
    logger.info("--- START Inventory Scan ---")
    logger.info(f"Konfiguration geladen. Scanne: {cfg['paths']['audio_dir']}")

    # Sonnenaufgang/-untergang Daten laden
    sun_ref_csv = Path("outputs/reference_sun.csv")
    if not sun_ref_csv.exists():
        logger.critical("Referenztabelle fehlt! Bitte erst '00_create_sun_reference.py' ausführen.")
        sys.exit(1)

    logger.info("Lade Sonnen-Referenztabelle...")
    df_sun = pd.read_csv(sun_ref_csv)

    # WICHTIG: Strings zurück in Datetime wandeln
    df_sun["date"] = pd.to_datetime(df_sun["date"]).dt.date
    df_sun["sunrise_naive"] = pd.to_datetime(df_sun["sunrise_naive"])
    df_sun["sunset_naive"] = pd.to_datetime(df_sun["sunset_naive"])


    # Pfade aus YAML holen (Relativ zu Projekt-Root oder Absolut)
    audio_dir = Path(cfg["paths"]["audio_dir"]) 
    output_csv = Path(cfg["paths"]["inventory_csv"])
    qc_csv = Path(cfg["paths"]["qc_inventory_csv"])

    # Regex aus YAML kompilieren
    filename_pattern = re.compile(cfg["scan"]["filename_regex"], re.IGNORECASE)
    morning = cfg["session_rules"]["morning_hours"]
    evening = cfg["session_rules"]["evening_hours"]

    logger.info(f"Scanne Ordner: {audio_dir}")
    if not audio_dir.exists():
        sys.exit(f"FEHLER: Audio-Ordner existiert nicht: {audio_dir}")

    # Alle WAVs finden (rekursiv mit rglob, falls Unterordner existieren)
    # Nutze set(), um Duplikate automatisch zu entfernen
    files = sorted(list(set(audio_dir.rglob("*.wav")) | set(audio_dir.rglob("*.WAV"))))    
    logger.info(f"{len(files)} Dateien gefunden.")

    inventory_rows = []
    anomalies = []

    # --- SCHLEIFE MIT PROGRESSBAR (tqdm) ---
    for p in tqdm(files, desc="Verarbeite Audio", unit="file"):
        row = {}

        # A) Dateigröße prüfen
        stat = p.stat()
        size_bytes = stat.st_size
        row["size_bytes"] = size_bytes
        row["filename"] = p.name
        row["filepath"] = str(p) # Absoluter Pfad für den Reader später

        # Default Werte für Fehlerfall
        row["is_empty"] = False
        row["wav_readable"] = False
        row["scan_status"] = "pending"

        # 1. Check: Leere Datei
        if size_bytes == 0:
            row["is_empty"] = True
            row["scan_status"] = "empty_file"
            row["last_error"] = "0 Byte File"
            logger.error(f"{p.name} :Kritischer Fehler: Datei leer")
            anomalies.append({"file": p.name, "issue": "empty_file"})
            inventory_rows.append(row)
            continue

        # B) Dateiname Parsen (Regex)
        match = filename_pattern.match(p.name)
        if not match:
            row["scan_status"] = "bad_filename"
            row["last_error"] = "Regex mismatch"
            logger.error(f"{p.name} Kritischer Fehler: Dateiname nicht im Schema")
            anomalies.append({"file": p.name, "issue": "bad_filename"})
            inventory_rows.append(row)
            continue

        info = match.groupdict()

        try:
            # Datum parsen: YYYYMMDD + HHMMSS
            dt_str = f"{info['date']}{info['time']}"
            start_dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
            row["recorder_id"] = info["rec"]
            row["start_dt"] = start_dt
            row["date"] = start_dt.date()
            row["month"] = start_dt.month

            # Abgeleitete Metadaten
            if cfg["birdnet_week48"]["enabled"]:
                row["birdnet_week48"] = compute_week48(start_dt)
            
            session = get_session(start_dt, morning, evening)
            row["session"] = session
            
            # File ID erstellen (WICHTIG für Datenbank/Parquet später)
            # Format: RECORDER_YYYYMMDD_HHMMSS - Eindeutige ID
            row["file_id"] = f"{info['rec']}_{start_dt.strftime('%Y%m%d_%H%M%S')}"

            #################
            # Wir suchen die passende Zeile in der Referenztabelle
            sun_row = df_sun[df_sun["date"] == row["date"]]

            if not sun_row.empty:
                # Zeiten holen
                sr = sun_row.iloc[0]["sunrise_naive"]
                ss = sun_row.iloc[0]["sunset_naive"]

                slot = None
                diff = None

                if session == "morning":
                    slot, diff = calculate_slot_with_tolerance(start_dt, sr, "sunrise", tolerance_min=15)
                    row["min_to_sunrise"] = round(diff, 1) if diff is not None else None
                    row["min_to_sunset"] = None
                
                elif session == "evening":
                    # Abends -> Sunset prüfen
                    slot, diff = calculate_slot_with_tolerance(start_dt, ss, "sunset", tolerance_min=15)
                    row["min_to_sunrise"] = None
                    row["min_to_sunset"] = round(diff, 1) if diff is not None else None

                else:
                    # Mittag/Nacht -> Kein Slot
                    slot = "other_time"
                    row["min_to_sunrise"] = None
                    row["min_to_sunset"] = None
                
                if slot is None:
                    if session == "morning": slot = "morning_no_slot"
                    elif session == "evening": slot = "evening_no_slot"

                row["solar_slot"] = slot
            
            else:
                row["solar_slot"] = "no_ref_data"
                anomalies.append({"file": p.name, "issue": "Missing Sun Data"})   

            ###############

        except ValueError as e:
            row["scan_status"] = "bad_timestamp"
            row["last_error"] = str(e)

            logger.error(f"{p.name} Kritischer Fehler: Datum falsch")

            anomalies.append({"file": p.name, "issue": "bad_timestamp"})
            inventory_rows.append(row)
            continue




        # ========== Audioheader lesen mit Soundfile (alternativ mit wave ) ======
        try:
            # sf.info liest nur den Header, sehr schnell!
            sf_info = sf.info(str(p))
            
            row["duration_s"] = sf_info.duration
            row["samplerate"] = sf_info.samplerate
            row["channels"] = sf_info.channels
            row["format"] = sf_info.format      # z.B. WAV
            row["subtype"] = sf_info.subtype    # z.B. PCM_16

            # Plausibilitäts-Check: Ist Datei extrem kurz? (< 1 Sekunde)
            if sf_info.duration < 300.0:
                logger.error(f"{p.name} Kritischer Fehler: Datei ist zu kurz")
                anomalies.append({"file": p.name, "issue": "too_short", "val": sf_info.duration})


            # Alles okay
            row["wav_readable"] = True
            row["scan_status"] = "scanned"
            
            # Endzeit berechnen
            row["end_dt"] = start_dt + timedelta(seconds=sf_info.duration)

        except Exception as e:
            row["wav_readable"] = False
            row["scan_status"] = "failed_read"
            row["last_error"] = str(e)
            logger.error(f"{p.name} Kritischer Fehler: Audio nicht lesbar")
            anomalies.append({"file": p.name, "issue": "corrupt_audio", "detail": str(e)})
    
        # Initialisiere Pipeline-Status Spalten (für spätere Skripte)
        row["birdnet_status"] = "pending" if row["wav_readable"] else "blocked"
        row["perch_status"] = "pending" if row["wav_readable"] else "blocked"
        row["updated_at"] = datetime.now()

        inventory_rows.append(row)

    # ======== SPEICHERN DER CSV =============
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    qc_csv.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(inventory_rows)

    # Speichern (Atomic-ish: erst schreiben, dann ist es da)
    logger.info(f"Speichere Inventory ({len(df)} Zeilen) nach: {output_csv}")
    df.to_csv(output_csv, index=False)

    # Anomalien speichern
    if anomalies:
        df_anom = pd.DataFrame(anomalies)
        logger.info(f"ACHTUNG: {len(anomalies)} Anomalien gefunden! Siehe: {qc_csv}")
        df_anom.to_csv(qc_csv, index=False)
    else:
        logger.info("Keine Anomalien gefunden. Saubere Daten!")

if __name__ == "__main__":
    main()










