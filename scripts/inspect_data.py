import pandas as pd
from pathlib import Path

def inspect(filename):
    path = Path(f"outputs/{filename}")
    if not path.exists():
        print(f"Datei nicht gefunden: {path}")
        return

    # Pandas-Einstellungen für maximale Sichtbarkeit
    pd.set_option('display.max_rows', None)     # Zeige ALLE Zeilen
    pd.set_option('display.max_columns', None)  # Zeige ALLE Spalten
    pd.set_option('display.width', 1000)        # Nutze die volle Breite des Terminals

    print(f"--- Lade {filename} ---")
    df = pd.read_csv(path)
    
    # Info zur Struktur
    print("\nINFO:")
    print(df.info())
    
    # Die Daten
    print("\nDATEN:")
    print(df) # Oder df.head(50) für die ersten 50

if __name__ == "__main__":
    # Hier einfach ändern, welche Datei du sehen willst
    inspect("reference_sun.csv")
    # inspect("inventory.csv")