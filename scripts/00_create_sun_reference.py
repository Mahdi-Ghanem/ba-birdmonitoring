import sys
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from astral import LocationInfo
from astral.sun import sun


# --- MODERNES ZEITZONEN-HANDLING ---
# zoneinfo ist seit Python 3.9 Standard.
# Auf Windows benötigt man zusätzlich 'pip install tzdata'
from zoneinfo import ZoneInfo 

# Pfad-Fix für utils Import
sys.path.append(str(Path(__file__).parent.parent))
from scripts.utils import load_config, setup_logger

def main():
    # 1. Setup
    config_path = "config/pipeline.yaml"
    cfg = load_config(config_path)
    
    log_path = cfg["paths"]["pipeline_log"]
    logger = setup_logger("SunReference", log_path)
    
    lat = cfg["location"]["latitude"]
    lon = cfg["location"]["longitude"]
    output_csv = Path("outputs/reference_sun.csv") #gefällt mir nicht
    
    logger.info(f"Erstelle Sonnen-Referenz (Europe/Berlin) für 2025...")

    # 2. Standort & Zeitzone definieren
    # Wir nutzen explizit die IANA Zeitzone für Deutschland
    berlin_tz = ZoneInfo("Europe/Berlin")
    city = LocationInfo("Biotop", "Germany", "Europe/Berlin", lat, lon)
    
    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    
    rows = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            # A) Berechnung (Astral liefert Ergebnisse passend zur LocationInfo)
            # Wir erzwingen aber sicherheitshalber die Ausgabe in unserer Zeitzone
            s = sun(city.observer, date=current_date, tzinfo=berlin_tz)
            
            # B) Werte extrahieren
            sr_aware = s["sunrise"]
            ss_aware = s["sunset"]
            
            # C) NAIVE Variante erstellen (für Dateinamen-Vergleich)
            # .replace(tzinfo=None) schneidet die Zeitzone ab, ÄNDERT ABER DIE UHRZEIT NICHT.
            # Beispiel: 
            #   Aware: 2025-06-01 05:20:00+02:00 (Sommerzeit)
            #   Naive: 2025-06-01 05:20:00       (So heißt deine Datei)
            sr_naive = sr_aware.replace(tzinfo=None)
            ss_naive = ss_aware.replace(tzinfo=None)

            rows.append({
                "date": current_date,
                # 1. Für die Wissenschaft (Aware ISO-String)
                "sunrise_aware": sr_aware.isoformat(),
                "sunset_aware": ss_aware.isoformat(),
                
                # 2. Für das Inventory-Matching (Naive Timestamp)
                "sunrise_naive": sr_naive,
                "sunset_naive": ss_naive,
                
                # 3. Metadaten
                "noon_naive": s["noon"].replace(tzinfo=None),
                "dst_active": bool(sr_aware.dst()) # True wenn Sommerzeit
            })

        except Exception as e:
            logger.error(f"Fehler bei Datum {current_date}: {e}")
            
        current_date += timedelta(days=1)

    # 3. Speichern
    df = pd.DataFrame(rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    
    # Plausibilitäts-Check im Log
    summer_row = df[df["date"] == date(2025, 6, 21)].iloc[0]
    winter_row = df[df["date"] == date(2025, 12, 21)].iloc[0]
    
    logger.info(f"Check Sommer (21.06.): {summer_row['sunrise_naive']} (Erwartet ca. 05:XX)")
    logger.info(f"Check Winter (21.12.): {winter_row['sunrise_naive']} (Erwartet ca. 08:XX)")
    logger.info(f"Referenztabelle erstellt: {output_csv}")

if __name__ == "__main__":
    main()