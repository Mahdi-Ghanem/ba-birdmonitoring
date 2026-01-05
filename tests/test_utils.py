# tests/test_utils.py
import pytest
from datetime import datetime
import sys
import os

# TRICK 17: Damit Python den 'scripts' Ordner findet, fügen wir ihn zum Pfad hinzu.
# Das ist nötig, weil 'tests' und 'scripts' Geschwister-Ordner sind.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.utils import compute_week48

# --- Hier beginnen die Tests ---

def test_week48_basics():
    """Prüft die Standardfälle (Anfang, Mitte, Ende des Monats)."""

    # Fall 1: 1. Januar -> Muss Woche 1 sein
    dt = datetime(2025, 1, 1)
    assert compute_week48(dt) == 1
    # 'assert' heißt: "Stelle sicher, dass das hier Wahr ist. Wenn nicht, schlag Alarm!"

    # Fall 2: 8. Januar -> Muss Woche 2 sein (da Tag > 7)
    dt = datetime(2025, 1, 8)
    assert compute_week48(dt) == 2

    # Fall 3: 15. Januar -> Muss Woche 3 sein
    dt = datetime(2025, 1, 15)
    assert compute_week48(dt) == 3

    # Fall 4: 22. Januar -> Muss Woche 4 sein
    dt = datetime(2025, 1, 22)
    assert compute_week48(dt) == 4


def test_week48_month_transition():
    """Prüft, ob der Monatswechsel korrekt gerechnet wird."""

    # 1. Februar -> (Monat 2 - 1) * 4 + 1 = 5
    dt = datetime(2025, 2, 1)
    assert compute_week48(dt) == 5


def test_week48_end_of_year():
    """Prüft den letzten Tag im Jahr."""

    # 31. Dezember -> (12 - 1) * 4 + 4 = 44 + 4 = 48
    dt = datetime(2025, 12, 31)
    assert compute_week48(dt) == 48


@pytest.mark.parametrize(
    "dt, expected",
    [
        # --- Januar (31 Tage) -> Wochen 1-4 ---
        (datetime(2025, 1, 1),  1),
        (datetime(2025, 1, 7),  1),
        (datetime(2025, 1, 8),  2),
        (datetime(2025, 1, 14), 2),
        (datetime(2025, 1, 15), 3),
        (datetime(2025, 1, 21), 3),
        (datetime(2025, 1, 22), 4),
        (datetime(2025, 1, 31), 4),

        # --- Februar (2025: 28 Tage) -> Wochen 5-8 ---
        (datetime(2025, 2, 1),  5),
        (datetime(2025, 2, 7),  5),
        (datetime(2025, 2, 8),  6),
        (datetime(2025, 2, 14), 6),
        (datetime(2025, 2, 15), 7),
        (datetime(2025, 2, 21), 7),
        (datetime(2025, 2, 22), 8),
        (datetime(2025, 2, 28), 8),

        # --- April (30 Tage) -> Wochen 13-16 ---
        (datetime(2025, 4, 1),  13),
        (datetime(2025, 4, 7),  13),
        (datetime(2025, 4, 8),  14),
        (datetime(2025, 4, 14), 14),
        (datetime(2025, 4, 15), 15),
        (datetime(2025, 4, 21), 15),
        (datetime(2025, 4, 22), 16),
        (datetime(2025, 4, 30), 16),

        # --- Dezember (31 Tage) -> Wochen 45-48 ---
        (datetime(2025, 12, 1),  45),
        (datetime(2025, 12, 7),  45),
        (datetime(2025, 12, 8),  46),
        (datetime(2025, 12, 14), 46),
        (datetime(2025, 12, 15), 47),
        (datetime(2025, 12, 21), 47),
    ],
)
def test_week48_30_scenarios(dt, expected):
    assert compute_week48(dt) == expected



# ... (Imports bleiben gleich) ...
# ... (deine Imports oben müssen datetime und pytest enthalten) ...
# Falls noch nicht importiert, hole get_session dazu:
from scripts.utils import get_session
# DEINE NEUEN REGELN:
# Morgens: 03:00 bis 12:00 (Stunde 3 bis inklusive 12)
# Abends: 15:00 bis 23:00 (Stunde 15 bis inklusive 23)
TEST_MORNING = [3, 12]
TEST_EVENING = [15, 23]

@pytest.mark.parametrize(
    "hour, expected_label",
    [
        # --- MORNING CASES (03-12) ---
        (3, "morning"),   # Neuer Start (ganz früh!)
        (5, "morning"),   # Mitten drin
        (12, "morning"),  # Neues Ende (Mittags)
        
        # --- EVENING CASES (15-23) ---
        (15, "evening"),  # Neuer Start (Nachmittags)
        (20, "evening"),  # Mitten drin
        (23, "evening"),  # Letzte Stunde des Tages (23:00-23:59)
        
        # --- OTHER CASES (Die Lücken) ---
        (2, "other"),     # Zu früh (vor 03:00)
        (13, "other"),    # Mittagspause (nach 12, vor 15)
        (14, "other"),    # Kurz vor Abend
        # (0 Uhr ist jetzt "other", da Abend bis 23 geht)
        (0, "other"),     
    ]
)
def test_session_logic(hour, expected_label):
    # Dummy Datum
    dummy_dt = datetime(2025, 5, 1, hour, 0, 0)
    
    # Aufruf mit deinen neuen Regeln
    result = get_session(dummy_dt, TEST_MORNING, TEST_EVENING)
    
    assert result == expected_label