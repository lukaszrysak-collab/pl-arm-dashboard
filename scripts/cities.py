"""
Bolt Food Poland — shared config for city IDs, AMs, targets, SQL helpers.
All scripts import from here.
"""

from datetime import date, timedelta

# ── City registry ─────────────────────────────────────────────────────────────

CITY_ID_MAP = {
    211: 'Krakow',
    350: 'Poznan',
    449: 'Gdansk',
    344: 'Wroclaw',
    351: 'Lodz',
    465: 'Lublin',
    352: 'Silesia',
    469: 'Bialystok',
    468: 'Torun',
}

CITY_IDS = list(CITY_ID_MAP.keys())
CITY_IDS_CSV = ','.join(str(cid) for cid in CITY_IDS)

# ── AM team ───────────────────────────────────────────────────────────────────

AM_NAMES = ['Gabriela Ziółek', 'Patrycja Konfederak', 'Weronika Słomska']

# Q2 2026 monthly KPI targets (Apr–Jun)
AM_TARGETS = {
    'Gabriela Ziółek':     {'churn': 0, 'reneg': 4,  'mktg': 25, 'smart': 30, 'ads': 22, 'boltplus': 173},
    'Patrycja Konfederak': {'churn': 0, 'reneg': 3,  'mktg': 25, 'smart': 60, 'ads': 10, 'boltplus': 127},
    'Weronika Słomska':    {'churn': 0, 'reneg': 1,  'mktg': 10, 'smart': 20, 'ads': 7,  'boltplus': 71},
}

KPI_WEIGHTS = {
    'churn':    0.20,
    'reneg':    0.20,
    'mktg':     0.10,
    'smart':    0.15,
    'ads':      0.20,
    'boltplus': 0.15,
}

# ── SQL fragments ─────────────────────────────────────────────────────────────

# Food-only filter for dim_provider joins
FOOD_FILTER_DIM = "d.is_bolt_market_provider = false AND d.is_grocery_segment = false"
FOOD_FILTER_V2  = "d.is_bolt_market_provider = false AND d.is_store_provider = false"

# AM filter (use am_managers for dim_provider, account_manager_name for dim_provider_v2)
AM_FILTER = "d.am_managers IN ('Gabriela Ziółek', 'Patrycja Konfederak', 'Weronika Słomska')"
AM_FILTER_V2 = "d.account_manager_name IN ('Gabriela Ziółek', 'Patrycja Konfederak', 'Weronika Słomska')"

# SMB filter
SMB_FILTER = "d.business_segment_v2 = 'SMB (AM Segment)'"

# ── Week helpers ──────────────────────────────────────────────────────────────

def complete_weeks(n=2):
    """Return list of (monday, sunday) for last n complete Mon–Sun weeks."""
    today = date.today()
    # Monday of current (possibly incomplete) week
    current_monday = today - timedelta(days=today.weekday())
    weeks = []
    for i in range(n, 0, -1):
        monday = current_monday - timedelta(weeks=i)
        sunday = monday + timedelta(days=6)
        weeks.append((monday, sunday))
    return weeks  # oldest first → [(prev_mon, prev_sun), (curr_mon, curr_sun)]

def week_filter_sql(date_col, n=2):
    """WHERE clause covering last n complete weeks."""
    weeks = complete_weeks(n)
    start = weeks[0][0].isoformat()
    end   = weeks[-1][1].isoformat()
    return f"{date_col} BETWEEN '{start}' AND '{end}'"

def current_month_start():
    today = date.today()
    return date(today.year, today.month, 1).isoformat()
