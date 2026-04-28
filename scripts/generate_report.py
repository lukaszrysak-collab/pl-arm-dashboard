#!/usr/bin/env python3
"""
Bolt Food Poland — Main Dashboard Generator
Produces index.html with city KPIs, WoW trends, and AM KPI tracking.
Data source: Databricks (fact_delivery_city_daily + fact_courier_daily_v2 + AM KPI tables).
"""

import json
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from dbx import DBX
from cities import (
    CITY_ID_MAP, CITY_IDS_CSV, AM_NAMES, AM_TARGETS, KPI_WEIGHTS,
    AM_FILTER, AM_FILTER_V2, FOOD_FILTER_DIM, FOOD_FILTER_V2,
    complete_weeks, week_filter_sql, current_month_start,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR   = os.path.dirname(SCRIPT_DIR)
DATA_DIR   = os.path.join(ROOT_DIR, 'data')
OUT_FILE   = os.path.join(ROOT_DIR, 'index.html')
os.makedirs(DATA_DIR, exist_ok=True)

# ── Date ranges ───────────────────────────────────────────────────────────────

weeks = complete_weeks(2)
prev_mon, prev_sun = weeks[0]
curr_mon, curr_sun = weeks[1]
month_start = current_month_start()

print(f'[Report] Prev week: {prev_mon} – {prev_sun}')
print(f'[Report] Curr week: {curr_mon} – {curr_sun}')
print(f'[Report] Month start: {month_start}')

# ── City-level P&L (fact_delivery_city_daily) ─────────────────────────────────

CITY_SQL = f"""
SELECT
    d.city_id,
    dc.city_name,
    CASE
        WHEN d.metric_timestamp_partition BETWEEN '{curr_mon}' AND '{curr_sun}' THEN 'curr'
        WHEN d.metric_timestamp_partition BETWEEN '{prev_mon}' AND '{prev_sun}' THEN 'prev'
    END                                                                          AS period,
    SUM(d.total_gmv_before_discounts_eur)                                        AS gmv,
    SUM(d.users_delivered_orders_count)                                          AS orders,
    SUM(d.total_contribution_profit_eur
        - d.total_invoiced_demand_incentives_eur
        + d.total_invoiced_bolt_plus_agency_fee_eur)                             AS cml2_num,
    SUM(d.total_gmv_before_discounts_eur)                                        AS cml2_den,
    SUM(d.total_campaign_spend_bolt_eur)                                         AS di_spend,
    SUM(d.total_invoiced_courier_costs_eur)                                      AS courier_costs,
    SUM(d.delivered_platform_orders_count)                                       AS platform_orders,
    MAX(CASE WHEN d.bad_order_rate_weight > 0 THEN d.bad_order_rate_value END)   AS bo_rate
FROM spark_catalog.ng_delivery_spark.fact_delivery_city_daily d
JOIN spark_catalog.ng_delivery_spark.dim_delivery_city dc
  ON d.city_id = dc.city_id
WHERE d.city_id IN ({CITY_IDS_CSV})
  AND d.metric_timestamp_partition BETWEEN '{prev_mon}' AND '{curr_sun}'
GROUP BY d.city_id, dc.city_name, period
HAVING period IS NOT NULL
ORDER BY d.city_id, period
"""

# ── Courier metrics (fact_courier_daily_v2) ───────────────────────────────────

COURIER_SQL = f"""
SELECT
    city_id,
    CASE
        WHEN metric_timestamp_partition BETWEEN '{curr_mon}' AND '{curr_sun}' THEN 'curr'
        WHEN metric_timestamp_partition BETWEEN '{prev_mon}' AND '{prev_sun}' THEN 'prev'
    END                                                    AS period,
    SUM(courier_total_utilised_hours)                      AS util_hours,
    SUM(courier_total_online_hours)                        AS online_hours,
    SUM(courier_delivered_orders_count)                    AS delivered,
    SUM(courier_proposed_orders_count)                     AS proposed
FROM spark_catalog.ng_delivery_spark.fact_courier_daily_v2
WHERE city_id IN ({CITY_IDS_CSV})
  AND metric_timestamp_partition BETWEEN '{prev_mon}' AND '{curr_sun}'
GROUP BY city_id, period
HAVING period IS NOT NULL
"""

# ── AM KPI queries ────────────────────────────────────────────────────────────
# All queries count MTD (month_start to today), matching central RAM tracker methodology.

SMART_PROMO_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT cp.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider cp
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON cp.provider_id = d.provider_id
WHERE cp.enrollment_source = 'portal_smart_promotions_blackbox_reenrollment'
  AND cp.created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

BOLTPLUS_SQL = f"""
SELECT v.account_manager_name AS am, COUNT(DISTINCT v.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.dim_provider_v2 v
WHERE v.is_bolt_plus_enrolled_provider = true
  AND v.bolt_plus_agency_fee_commission_rate > 0
  AND CAST(v.provider_bolt_plus_first_enrollment_start_ts AS DATE) >= '{month_start}'
  AND v.provider_bolt_plus_last_enrollment_churn_ts IS NULL
  AND v.account_manager_name IN ('Gabriela Ziółek', 'Patrycja Konfederak', 'Weronika Słomska')
  AND v.is_bolt_market_provider = false AND v.is_store_provider = false
GROUP BY v.account_manager_name
"""

RENEG_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT r.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.int_provider_commission_logs r
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON r.provider_id = d.provider_id
WHERE r.log_created_date >= '{month_start}'
  AND r.change_reason = 'Renegotiation'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

ADS_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT h.provider_id) AS cnt
FROM spark_catalog.mart_models_spark.mart_provider_sponsored_listing_performance_hourly h
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON h.provider_id = d.provider_id
WHERE h.sponsored_listing_hour >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

MKTG_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT cp.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider cp
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON cp.provider_id = d.provider_id
WHERE cp.enrollment_source IN ('smart_promotions', 'portal_regular_promotions')
  AND cp.enrollment_state = 'active'
  AND cp.created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

CHURN_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT pd.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.fact_provider_daily pd
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON pd.provider_id = d.provider_id
WHERE pd.observation_date >= '{month_start}'
  AND pd.churned_provider_28days = 1
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

SPARKLINE_SQL = f"""
SELECT
    d.city_id,
    d.metric_timestamp_partition AS day,
    SUM(d.users_delivered_orders_count) AS orders
FROM spark_catalog.ng_delivery_spark.fact_delivery_city_daily d
WHERE d.city_id IN ({CITY_IDS_CSV})
  AND d.metric_timestamp_partition >= '{(curr_sun - timedelta(days=13)).isoformat()}'
  AND d.metric_timestamp_partition <= '{curr_sun.isoformat()}'
GROUP BY d.city_id, d.metric_timestamp_partition
ORDER BY d.city_id, day
"""

# ── Fetch all data ────────────────────────────────────────────────────────────

def _run(dbx, sql, label):
    try:
        df = dbx.query(sql)
        print(f'[Report] {label}: {len(df)} rows')
        return df
    except Exception as e:
        print(f'[Report] {label} FAILED: {e}')
        return None

print('[Report] Querying Databricks...')
with DBX() as dbx:
    df_city    = _run(dbx, CITY_SQL,        'City P&L')
    df_courier = _run(dbx, COURIER_SQL,     'Courier')
    df_smart   = _run(dbx, SMART_PROMO_SQL, 'Smart Promos')
    df_bplus   = _run(dbx, BOLTPLUS_SQL,    'Bolt+ Enrolments')
    df_reneg   = _run(dbx, RENEG_SQL,       'Renegotiations')
    df_ads     = _run(dbx, ADS_SQL,         'Merchant Ads')
    df_mktg    = _run(dbx, MKTG_SQL,        'Marketing Campaigns')
    df_churn   = _run(dbx, CHURN_SQL,       'Churn')
    df_spark   = _run(dbx, SPARKLINE_SQL,   'Sparklines')

# ── Process city metrics ──────────────────────────────────────────────────────

import pandas as pd

def _f(v):
    try:
        return float(v) if v is not None and str(v).strip() not in ('', 'nan', 'None') else 0.0
    except (ValueError, TypeError):
        return 0.0

city_metrics = defaultdict(lambda: {'curr': {}, 'prev': {}})

if df_city is not None:
    for _, r in df_city.iterrows():
        cid    = int(_f(r['city_id']))
        name   = CITY_ID_MAP.get(cid, str(r.get('city_name', cid)))
        period = str(r['period'])

        gmv    = _f(r['gmv'])
        orders = _f(r['orders'])
        cml2_n = _f(r['cml2_num'])
        cml2_d = _f(r['cml2_den'])
        di     = _f(r['di_spend'])
        cc     = _f(r['courier_costs'])
        po     = _f(r['platform_orders'])
        bo     = _f(r['bo_rate'])

        city_metrics[name][period] = {
            'gmv':      gmv,
            'orders':   orders,
            'cml2_pct': cml2_n / cml2_d * 100 if cml2_d else 0,
            'di_pct':   di / gmv * 100 if gmv else 0,
            'cpo':      cc / po if po else 0,
            'bo_pct':   bo * 100 if bo else 0,
        }

if df_courier is not None:
    for _, r in df_courier.iterrows():
        cid    = int(_f(r['city_id']))
        name   = CITY_ID_MAP.get(cid)
        period = str(r['period'])
        if not name:
            continue
        util_h   = _f(r['util_hours'])
        online_h = _f(r['online_hours'])
        deliv    = _f(r['delivered'])
        proposed = _f(r['proposed'])

        existing = city_metrics[name].get(period, {})
        existing['util_pct'] = util_h / online_h * 100 if online_h else 0
        existing['ar_pct']   = deliv / proposed * 100 if proposed else 0
        city_metrics[name][period] = existing

def _wow(curr, prev, is_pct=False):
    if not curr or not prev:
        return None
    delta = curr - prev
    if is_pct:
        return round(delta, 2)
    return round(delta / prev * 100, 1) if prev else None

def _fmt_wow(v, is_pp=False):
    if v is None:
        return '—'
    sign = '+' if v >= 0 else ''
    suffix = 'pp' if is_pp else '%'
    return f'{sign}{v:.1f}{suffix}'

# Build enriched city list with WoW and absolute deltas
cities_out = []
for name, periods in city_metrics.items():
    curr = periods.get('curr', {})
    prev = periods.get('prev', {})

    gmv    = curr.get('gmv', 0)
    orders = curr.get('orders', 0)
    cml2   = curr.get('cml2_pct', 0)
    di     = curr.get('di_pct', 0)
    cpo    = curr.get('cpo', 0)
    util   = curr.get('util_pct', 0)
    ar     = curr.get('ar_pct', 0)
    bo     = curr.get('bo_pct', 0)

    prev_orders = prev.get('orders', 0)
    prev_gmv    = prev.get('gmv', 0)

    cities_out.append({
        'name':       name,
        'gmv':        round(gmv, 0),
        'orders':     round(orders, 0),
        'cml2_pct':   round(cml2, 2),
        'di_pct':     round(di, 2),
        'cpo':        round(cpo, 2),
        'util_pct':   round(util, 1),
        'ar_pct':     round(ar, 1),
        'bo_pct':     round(bo, 2),
        'gmv_wow':    _wow(gmv, prev_gmv),
        'gmv_abs':    round(gmv - prev_gmv, 0),
        'orders_wow': _wow(orders, prev_orders),
        'orders_abs': round(orders - prev_orders, 0),
        'cml2_wow':   _wow(cml2, prev.get('cml2_pct', 0), is_pct=True),
        'di_wow':     _wow(di, prev.get('di_pct', 0), is_pct=True),
        'cpo_wow':    _wow(cpo, prev.get('cpo', 0), is_pct=True),
        'util_wow':   _wow(util, prev.get('util_pct', 0), is_pct=True),
        'ar_wow':     _wow(ar, prev.get('ar_pct', 0), is_pct=True),
    })

cities_out.sort(key=lambda c: c['gmv'], reverse=True)

# Sparkline data per city (14-day daily orders)
city_sparklines = defaultdict(list)
if df_spark is not None:
    for _, r in df_spark.sort_values('day').iterrows():
        cid = int(_f(r['city_id']))
        name = CITY_ID_MAP.get(cid)
        if name:
            city_sparklines[name].append(round(_f(r['orders']), 0))

for c in cities_out:
    c['spark'] = city_sparklines.get(c['name'], [])

CITY_COLORS = {
    'Krakow':    '#34D186',
    'Poznan':    '#3b82f6',
    'Gdansk':    '#14b8a6',
    'Wroclaw':   '#8b5cf6',
    'Lublin':    '#f59e0b',
    'Bialystok': '#ec4899',
    'Lodz':      '#06b6d4',
    'Silesia':   '#f97316',
    'Torun':     '#84cc16',
}

# Aggregate totals
def _sum(key):
    return sum(c[key] for c in cities_out)

def _wavg(key, weight='gmv'):
    total_w = _sum(weight)
    if not total_w:
        return 0
    return sum(c[key] * c[weight] for c in cities_out) / total_w

totals = {
    'gmv':      _sum('gmv'),
    'orders':   _sum('orders'),
    'cml2_pct': _wavg('cml2_pct'),
    'di_pct':   _wavg('di_pct'),
    'cpo':      _wavg('cpo', weight='orders'),
    'util_pct': _wavg('util_pct', weight='orders'),
    'ar_pct':   _wavg('ar_pct', weight='orders'),
    'bo_pct':   _wavg('bo_pct', weight='orders'),
}

# Previous week weighted averages for total WoW
def _prev_wavg(key, weight='gmv'):
    total_w = sum(p.get('prev', {}).get(weight, 0) for p in city_metrics.values())
    if not total_w:
        return 0
    return sum(
        p.get('prev', {}).get(key, 0) * p.get('prev', {}).get(weight, 0)
        for p in city_metrics.values()
    ) / total_w

prev_gmv_total    = sum(p.get('prev', {}).get('gmv', 0) for p in city_metrics.values())
prev_orders_total = sum(p.get('prev', {}).get('orders', 0) for p in city_metrics.values())

total_wows = {
    'gmv_wow':    _wow(totals['gmv'], prev_gmv_total),
    'gmv_abs':    round(totals['gmv'] - prev_gmv_total, 0),
    'orders_wow': _wow(totals['orders'], prev_orders_total),
    'orders_abs': round(totals['orders'] - prev_orders_total, 0),
    'cml2_wow':   round(totals['cml2_pct'] - _prev_wavg('cml2_pct'), 2),
    'di_wow':     round(totals['di_pct']   - _prev_wavg('di_pct'), 2),
    'cpo_wow':    round(totals['cpo']      - _prev_wavg('cpo', 'orders'), 2),
    'util_wow':   round(totals['util_pct'] - _prev_wavg('util_pct', 'orders'), 2),
    'ar_wow':     round(totals['ar_pct']   - _prev_wavg('ar_pct', 'orders'), 2),
}

# ── Process AM KPIs ───────────────────────────────────────────────────────────

def _am_counts(df, am_col='am', cnt_col='cnt'):
    result = {am: 0 for am in AM_NAMES}
    if df is None:
        return result
    for _, r in df.iterrows():
        am = str(r[am_col]).strip()
        if am in result:
            result[am] = int(_f(r[cnt_col]))
    return result

am_actuals = {
    'smart':    _am_counts(df_smart),
    'boltplus': _am_counts(df_bplus),
    'reneg':    _am_counts(df_reneg),
    'ads':      _am_counts(df_ads),
    'mktg':     _am_counts(df_mktg),
    'churn':    _am_counts(df_churn),
}

def _kpi_score(am):
    targets = AM_TARGETS[am]
    score = 0.0
    for kpi, weight in KPI_WEIGHTS.items():
        actual  = am_actuals[kpi][am]
        target  = targets[kpi]
        if kpi == 'churn':
            achievement = 1.0 if actual == 0 else 0.0
        elif target == 0:
            achievement = 1.0
        else:
            achievement = min(actual / target, 1.3)
        score += weight * achievement
    return round(score * 100, 1)

am_kpis = []
for am in AM_NAMES:
    kpis = {}
    for kpi in ['churn', 'reneg', 'mktg', 'smart', 'ads', 'boltplus']:
        actual = am_actuals[kpi][am]
        target = AM_TARGETS[am][kpi]
        pct    = (actual / target * 100) if target else (100 if actual == 0 else 0)
        kpis[kpi] = {'actual': actual, 'target': target, 'pct': round(pct, 1)}
    am_kpis.append({
        'name':  am,
        'score': _kpi_score(am),
        'kpis':  kpis,
    })

# ── Serialize data for JS ─────────────────────────────────────────────────────

cities_json      = json.dumps(cities_out,  ensure_ascii=False)
totals_json      = json.dumps({**totals, **total_wows}, ensure_ascii=False)
am_kpis_json     = json.dumps(am_kpis, ensure_ascii=False)
city_colors_json = json.dumps(CITY_COLORS, ensure_ascii=False)

week_label = f"W{curr_mon.isocalendar()[1]} ({curr_mon.strftime('%-d %b')}–{curr_sun.strftime('%-d %b %Y')})"

# ── Generate HTML ─────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Poland ARM Dashboard — Bolt Food</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<script src="https://unpkg.com/lucide@latest"></script>
<script src="theme.js"></script>
<style>
:root {{
  --bg:           #0e0e1a;
  --bg-card:      #161626;
  --bg-sidebar:   #111120;
  --border:       #252538;
  --border-hover: #3a3a58;
  --accent:       #34D186;
  --accent-light: rgba(52,209,134,0.10);
  --accent2:      #3b82f6;
  --accent3:      #f59e0b;
  --danger:       #ef4444;
  --purple:       #8b5cf6;
  --text:         #f0f0f5;
  --text-secondary: #b0b0cc;
  --text-muted:   #6b6b88;
  --shadow-sm:    0 1px 4px rgba(0,0,0,0.25);
  --shadow-md:    0 4px 16px rgba(0,0,0,0.35);
  --radius:       12px;
  --radius-sm:    8px;
}}
[data-theme="light"] {{
  --bg:           #f1f2f6;
  --bg-card:      #ffffff;
  --bg-sidebar:   #ffffff;
  --border:       #e2e3ec;
  --border-hover: #c0c0d8;
  --accent-light: rgba(52,209,134,0.08);
  --text:         #1a1a2e;
  --text-secondary: #4a4a6a;
  --text-muted:   #8a8aaa;
  --shadow-sm:    0 1px 4px rgba(0,0,0,0.08);
  --shadow-md:    0 4px 16px rgba(0,0,0,0.12);
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--text); line-height:1.5; overflow-x:hidden; }}

/* ── App layout ── */
.app-layout {{ display:flex; min-height:100vh; }}

/* ── Sidebar ── */
.sidebar {{ width:225px; position:fixed; top:0; left:0; bottom:0; background:var(--bg-sidebar); border-right:1px solid var(--border); display:flex; flex-direction:column; padding:20px 12px; z-index:200; overflow-y:auto; scrollbar-width:none; }}
.sidebar::-webkit-scrollbar {{ display:none; }}
.sidebar-logo {{ display:flex; align-items:center; gap:10px; padding:0 6px; margin-bottom:28px; }}
.sidebar-logo-icon {{ width:34px; height:34px; background:var(--accent); border-radius:9px; display:flex; align-items:center; justify-content:center; color:#fff; font-weight:800; font-size:1.05em; flex-shrink:0; }}
.sidebar-brand {{ font-weight:700; font-size:0.85em; color:var(--text); }}
.sidebar-brand-sub {{ font-size:0.67em; color:var(--text-muted); font-weight:400; display:block; margin-top:1px; }}
.sidebar-section {{ font-size:0.63em; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.8px; padding:18px 6px 8px; }}
.sidebar-nav {{ display:flex; flex-direction:column; gap:2px; flex:1; }}
.sidebar-item {{ display:flex; align-items:center; gap:10px; padding:9px 12px; border-radius:var(--radius-sm); color:var(--text-secondary); font-size:0.81em; font-weight:500; cursor:pointer; transition:all 0.15s ease; border:none; background:none; font-family:inherit; text-align:left; width:100%; }}
.sidebar-item:hover {{ background:var(--accent-light); color:var(--text); }}
.sidebar-item.active {{ background:var(--accent-light); color:var(--accent); font-weight:600; }}
.sidebar-item i {{ width:17px; height:17px; flex-shrink:0; }}
.sidebar-footer {{ margin-top:auto; padding:14px 6px 0; border-top:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; }}
.sidebar-footer-week {{ font-size:0.65em; color:var(--text-muted); }}

/* ── Main content ── */
.main-content {{ margin-left:225px; flex:1; display:flex; flex-direction:column; min-width:0; }}
.top-bar {{ display:flex; align-items:center; gap:10px; padding:10px 24px; border-bottom:1px solid var(--border); background:var(--bg-card); position:sticky; top:0; z-index:100; }}
.top-bar-title {{ font-size:0.82em; font-weight:700; color:var(--text); }}
.top-bar-sub {{ font-size:0.7em; color:var(--text-muted); margin-left:4px; }}
.top-bar-spacer {{ flex:1; }}
.content-area {{ padding:20px 24px; flex:1; overflow-y:auto; scrollbar-width:none; }}
.content-area::-webkit-scrollbar {{ display:none; }}

/* ── Tab panels ── */
.tab-panel {{ display:none; }}
.tab-panel.active {{ display:block; }}

/* ── Section title ── */
.section-title {{ font-size:0.88em; font-weight:700; color:var(--text); display:flex; align-items:center; gap:8px; margin:0 0 12px 0; }}
.section-title .accent-bar {{ width:3px; height:18px; background:var(--accent); border-radius:2px; flex-shrink:0; }}

/* ── Executive Summary header ── */
.exec-header {{ display:flex; align-items:center; gap:14px; margin-bottom:16px; }}
.exec-title {{ font-size:1.25em; font-weight:800; color:var(--text); letter-spacing:-0.02em; display:flex; align-items:center; gap:10px; }}
.exec-title::before {{ content:''; display:inline-block; width:4px; height:22px; background:var(--accent); border-radius:2px; }}
.exec-week {{ font-size:0.72em; color:var(--text-muted); margin-left:4px; }}

/* ── KPI row (6 cards, each with own color) ── */
.kpi-row-6 {{ display:grid; grid-template-columns:repeat(6,1fr); gap:10px; margin-bottom:16px; }}
@media (max-width:1200px) {{ .kpi-row-6 {{ grid-template-columns:repeat(3,1fr); }} }}
@media (max-width:800px)  {{ .kpi-row-6 {{ grid-template-columns:repeat(2,1fr); }} }}
.kpi-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:16px 18px; transition:all 0.2s ease; }}
.kpi-card:hover {{ border-color:var(--border-hover); box-shadow:var(--shadow-md); }}
.kpi-label {{ font-size:0.6em; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }}
.kpi-value {{ font-size:1.6em; font-weight:800; letter-spacing:-0.03em; line-height:1.05; margin-bottom:6px; }}
.kpi-delta {{ font-size:0.72em; font-weight:600; color:var(--text-muted); }}
.kpi-delta .pct {{ font-weight:700; }}
.kpi-delta .abs {{ opacity:0.75; margin-left:3px; }}
.delta-up   {{ color:var(--accent) !important; }}
.delta-down {{ color:var(--danger) !important; }}
.delta-neut {{ color:var(--text-muted) !important; }}

/* ── Overview lower split ── */
.exec-lower {{ display:grid; grid-template-columns:1fr 380px; gap:14px; }}
@media (max-width:1100px) {{ .exec-lower {{ grid-template-columns:1fr; }} }}
.dash-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:18px; box-shadow:var(--shadow-sm); transition:all 0.2s ease; }}
.dash-card:hover {{ box-shadow:var(--shadow-md); border-color:var(--border-hover); }}
.card-category {{ font-size:0.62em; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.6px; margin-bottom:4px; display:flex; align-items:center; gap:6px; }}
.card-category i {{ width:13px; height:13px; opacity:0.6; }}
.card-title {{ font-size:1.05em; font-weight:700; color:var(--text); margin-bottom:14px; letter-spacing:-0.01em; }}

/* ── City big grid (3×3) ── */
.city-big-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; }}
.city-big-card {{ background:var(--bg); border:1px solid var(--border); border-radius:var(--radius-sm); padding:14px 12px 10px; cursor:pointer; transition:all 0.2s; display:flex; flex-direction:column; align-items:center; text-align:center; position:relative; overflow:hidden; }}
.city-big-card:hover {{ border-color:var(--border-hover); box-shadow:var(--shadow-sm); transform:translateY(-1px); }}
.city-big-name {{ font-size:0.8em; font-weight:700; margin-bottom:4px; }}
.city-big-orders {{ font-size:1.5em; font-weight:800; color:var(--text); letter-spacing:-0.03em; line-height:1.1; }}
.city-big-sub {{ font-size:0.62em; color:var(--text-muted); margin-top:3px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:100%; }}
.city-big-spark {{ height:36px; width:100%; margin-top:8px; }}
.city-big-spark svg {{ width:100%; height:100%; }}

/* ── Movers ── */
.movers-section {{ margin-bottom:12px; }}
.movers-label {{ font-size:0.68em; font-weight:700; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:8px; }}
.movers-label.gainers  {{ color:var(--accent); }}
.movers-label.decliners {{ color:var(--danger); }}
.mover-row {{ display:flex; align-items:center; gap:8px; padding:6px 0; border-bottom:1px solid var(--border); font-size:0.8em; }}
.mover-row:last-child {{ border-bottom:none; }}
.mover-rank {{ width:16px; font-weight:700; font-size:0.7em; color:var(--text-muted); text-align:center; flex-shrink:0; }}
.mover-city {{ font-weight:600; flex:1; color:var(--text); font-size:0.9em; }}
.mover-bar {{ flex:0 0 44px; height:4px; border-radius:3px; background:var(--border); overflow:hidden; }}
.mover-bar-fill {{ height:100%; border-radius:3px; }}
.mover-delta {{ font-weight:700; font-size:0.82em; white-space:nowrap; text-align:right; }}
.mover-delta .mover-abs {{ display:inline; }}
.mover-delta .mover-pct {{ opacity:0.7; font-size:0.88em; margin-left:3px; }}

/* ── City table ── */
.city-table-wrap {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); overflow:auto; margin-bottom:20px; }}
.city-table {{ width:100%; border-collapse:collapse; font-size:0.78em; }}
.city-table th {{ padding:11px 13px; text-align:right; font-size:0.68em; font-weight:700; text-transform:uppercase; letter-spacing:0.05em; color:var(--text-muted); border-bottom:1px solid var(--border); white-space:nowrap; cursor:pointer; user-select:none; background:var(--bg-card); position:sticky; top:0; z-index:1; }}
.city-table th:first-child {{ text-align:left; }}
.city-table th:hover {{ color:var(--accent); }}
.city-table th.sorted-asc::after  {{ content:' ↑'; color:var(--accent); }}
.city-table th.sorted-desc::after {{ content:' ↓'; color:var(--accent); }}
.city-table td {{ padding:10px 13px; text-align:right; border-bottom:1px solid var(--border); color:var(--text); }}
.city-table td:first-child {{ text-align:left; font-weight:700; }}
.city-table tr:last-child td {{ border-bottom:none; }}
.city-table tr:hover td {{ background:rgba(255,255,255,0.015); }}
.city-table .total-row td {{ font-weight:700; color:var(--accent); border-top:2px solid var(--border); }}

/* ── AM KPI cards ── */
.am-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:16px; margin-bottom:20px; }}
.am-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:20px 22px; }}
.am-card-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }}
.am-name {{ font-size:0.9em; font-weight:700; color:var(--text); }}
.am-score-badge {{ padding:4px 10px; border-radius:20px; font-size:0.72em; font-weight:700; }}
.score-green {{ background:rgba(52,209,134,0.15); color:var(--accent); }}
.score-amber {{ background:rgba(245,158,11,0.15); color:var(--accent3); }}
.score-red   {{ background:rgba(239,68,68,0.15);  color:var(--danger); }}
.kpi-row-item {{ margin-bottom:11px; }}
.kpi-row-header {{ display:flex; justify-content:space-between; font-size:0.72em; margin-bottom:4px; }}
.kpi-row-name {{ font-weight:600; color:var(--text-secondary); }}
.kpi-row-val  {{ color:var(--text-muted); }}
.progress-bg   {{ height:5px; background:var(--border); border-radius:3px; overflow:hidden; }}
.progress-fill {{ height:100%; border-radius:3px; transition:width 0.8s cubic-bezier(0.25,0.46,0.45,0.94); }}
.fill-green  {{ background:var(--accent); }}
.fill-amber  {{ background:var(--accent3); }}
.fill-red    {{ background:var(--danger); }}
.fill-purple {{ background:var(--purple); }}

/* ── Weight pills ── */
.weights-row {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:4px; }}
.weight-pill {{ background:var(--bg-card); border:1px solid var(--border); border-radius:7px; padding:7px 13px; font-size:0.72em; }}
.weight-name {{ color:var(--text-muted); }}
.weight-val  {{ font-weight:700; color:var(--accent); margin-left:5px; }}

@media (max-width:900px) {{ .sidebar {{ display:none; }} .main-content {{ margin-left:0; }} .city-big-grid {{ grid-template-columns:1fr 1fr; }} }}
</style>
</head>
<body>
<div class="app-layout">

<!-- ── Sidebar ── -->
<aside class="sidebar">
  <div class="sidebar-logo">
    <div class="sidebar-logo-icon">B</div>
    <div>
      <div class="sidebar-brand">PL ARM</div>
      <span class="sidebar-brand-sub">Bolt Food Poland</span>
    </div>
  </div>

  <div class="sidebar-section">Analytics</div>
  <nav class="sidebar-nav">
    <button class="sidebar-item active" onclick="showTab('overview',this)">
      <i data-lucide="layout-dashboard"></i> Overview
    </button>
    <button class="sidebar-item" onclick="showTab('cities',this)">
      <i data-lucide="map-pin"></i> Cities
    </button>
    <button class="sidebar-item" onclick="showTab('am_kpis',this)">
      <i data-lucide="target"></i> AM KPIs
    </button>
  </nav>

  <div class="sidebar-footer">
    <span class="sidebar-footer-week">{week_label}</span>
    <div id="themeToggleWrap"></div>
  </div>
</aside>

<!-- ── Main ── -->
<div class="main-content">
  <div class="top-bar">
    <span class="top-bar-title">Poland ARM Dashboard</span>
    <span class="top-bar-sub">{week_label}</span>
    <div class="top-bar-spacer"></div>
    <div id="themeToggleWrapTopBar"></div>
  </div>

  <div class="content-area">

    <!-- ── Overview tab ── -->
    <div id="tab-overview" class="tab-panel active">

      <div class="exec-header">
        <div class="exec-title">Executive Summary</div>
        <span class="exec-week">{week_label}</span>
      </div>

      <!-- 6 KPI cards -->
      <div class="kpi-row-6" id="kpiRow"></div>

      <!-- City grid + Movers -->
      <div class="exec-lower">
        <div class="dash-card">
          <div class="card-category"><i data-lucide="layers"></i>PERFORMANCE</div>
          <div class="card-title">City Overview</div>
          <div class="city-big-grid" id="cityBigGrid"></div>
        </div>
        <div class="dash-card">
          <div class="card-category"><i data-lucide="trending-up"></i>WEEKLY CHANGE</div>
          <div class="card-title">Top Movers (WoW Orders)</div>
          <div class="movers-section">
            <div class="movers-label gainers">GAINERS</div>
            <div id="gainersRows"></div>
          </div>
          <div class="movers-section">
            <div class="movers-label decliners">DECLINERS</div>
            <div id="declinersRows"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- ── Cities tab ── -->
    <div id="tab-cities" class="tab-panel">
      <div class="section-title"><div class="accent-bar"></div>City Breakdown — click headers to sort</div>
      <div class="city-table-wrap">
        <table class="city-table" id="cityTable">
          <thead>
            <tr>
              <th data-col="name"      data-type="str">City</th>
              <th data-col="gmv"       data-type="num">GMV</th>
              <th data-col="gmv_wow"   data-type="num">GMV WoW</th>
              <th data-col="orders"    data-type="num">Orders</th>
              <th data-col="orders_wow" data-type="num">Ord WoW</th>
              <th data-col="cml2_pct"  data-type="num">CML2%</th>
              <th data-col="cml2_wow"  data-type="num">CML2 Δ</th>
              <th data-col="di_pct"    data-type="num">DI%</th>
              <th data-col="di_wow"    data-type="num">DI Δ</th>
              <th data-col="cpo"       data-type="num">CPO</th>
              <th data-col="cpo_wow"   data-type="num">CPO Δ</th>
              <th data-col="util_pct"  data-type="num">Util%</th>
              <th data-col="util_wow"  data-type="num">Util Δ</th>
              <th data-col="ar_pct"    data-type="num">AR%</th>
              <th data-col="ar_wow"    data-type="num">AR Δ</th>
              <th data-col="bo_pct"    data-type="num">BO%</th>
            </tr>
          </thead>
          <tbody id="cityTableBody"></tbody>
        </table>
      </div>
    </div>

    <!-- ── AM KPIs tab ── -->
    <div id="tab-am_kpis" class="tab-panel">
      <div class="section-title"><div class="accent-bar"></div>AM KPIs — Q2 2026 · Month-to-date vs monthly targets</div>
      <div class="am-grid" id="amGrid"></div>
      <div class="section-title" style="margin-top:8px"><div class="accent-bar"></div>KPI Weights</div>
      <div class="weights-row" id="weightsRow"></div>
    </div>

  </div>
</div>
</div>

<script>
const CITIES      = {cities_json};
const TOTALS      = {totals_json};
const AM_KPIS     = {am_kpis_json};
const AM_TARGETS  = {json.dumps(AM_TARGETS, ensure_ascii=False)};
const KPI_WEIGHTS = {json.dumps(KPI_WEIGHTS, ensure_ascii=False)};
const CITY_COLORS = {city_colors_json};

const KPI_LABELS = {{
  churn:'Regrettable Churn', reneg:'Renegotiations', mktg:'Mktg Campaigns',
  smart:'Smart Promos', ads:'Merchant Ads', boltplus:'Bolt+ Enrolments',
}};

// ── Tab switching ──────────────────────────────────────────────────────────
function showTab(id, btn) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-item').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  if (btn) btn.classList.add('active');
}}

// ── Formatters ─────────────────────────────────────────────────────────────
function fmtEur(v) {{
  if (v === null || v === undefined || isNaN(v)) return '—';
  if (Math.abs(v) >= 1e6) return '€' + (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return '€' + (v/1e3).toFixed(1) + 'k';
  return '€' + v.toFixed(0);
}}
function fmtEurAbs(v) {{
  if (v === null || v === undefined || isNaN(v)) return '';
  const sign = v >= 0 ? '+' : '';
  if (Math.abs(v) >= 1e6) return sign + '€' + (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return sign + '€' + (v/1e3).toFixed(1) + 'k';
  return sign + '€' + v.toFixed(0);
}}
function fmtNum(v) {{
  if (v === null || v === undefined || isNaN(v)) return '—';
  if (Math.abs(v) >= 1e6) return (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return (v/1e3).toFixed(1) + 'k';
  return String(Math.round(v));
}}
function fmtNumAbs(v) {{
  if (v === null || v === undefined || isNaN(v)) return '';
  const sign = v >= 0 ? '+' : '';
  if (Math.abs(v) >= 1e3) return sign + (v/1e3).toFixed(1) + 'k';
  return sign + Math.round(v);
}}
function fmtPct(v, d) {{
  if (v === null || v === undefined) return '—';
  return v.toFixed(d !== undefined ? d : 1) + '%';
}}
function deltaClass(v, inv) {{
  if (v === null || v === undefined) return 'delta-neut';
  const good = inv ? v < 0 : v > 0;
  return good ? 'delta-up' : v === 0 ? 'delta-neut' : 'delta-down';
}}
function fmtDelta(v, pp) {{
  if (v === null || v === undefined) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(1) + (pp ? 'pp' : '%');
}}

// ── Overview KPI cards ─────────────────────────────────────────────────────
function renderKpiRow() {{
  const T = TOTALS;
  const cards = [
    {{ label:'TOTAL GMV (WEEK)', val:fmtEur(T.gmv),        color:'#34D186', wow:T.gmv_wow,    abs:fmtEurAbs(T.gmv_abs),    pp:false, inv:false }},
    {{ label:'TOTAL ORDERS',     val:fmtNum(T.orders),     color:'#3b82f6', wow:T.orders_wow, abs:fmtNumAbs(T.orders_abs), pp:false, inv:false }},
    {{ label:'AVG CML2%',        val:fmtPct(T.cml2_pct,1), color:'#14b8a6', wow:T.cml2_wow,   abs:null,                    pp:true,  inv:false }},
    {{ label:'AVG DI%',          val:fmtPct(T.di_pct,1),   color:'#f59e0b', wow:T.di_wow,     abs:null,                    pp:true,  inv:true  }},
    {{ label:'AVG CPO',          val:fmtEur(T.cpo),        color:'#a78bfa', wow:T.cpo_wow,    abs:null,                    pp:true,  inv:true  }},
    {{ label:'AVG BO%',          val:fmtPct(T.bo_pct,1),   color:'#f87171', wow:null,          abs:null,                    pp:true,  inv:true  }},
  ];

  const g = document.getElementById('kpiRow');
  g.innerHTML = cards.map(c => {{
    const cls = c.wow !== null ? deltaClass(c.wow, c.inv) : '';
    const sign = c.wow !== null ? (c.wow >= 0 ? '+' : '') : '';
    const wowStr = c.wow !== null ? `${{sign}}${{c.wow.toFixed(1)}}${{c.pp ? 'pp' : '%'}}` : '';
    const absStr = c.abs ? `(${{c.abs}})` : (c.wow !== null && c.pp ? '' : '');
    const deltaHtml = c.wow !== null
      ? `<div class="kpi-delta"><span class="pct ${{cls}}">${{wowStr}}</span>${{absStr ? `<span class="abs ${{cls}}">${{absStr}}</span>` : ''}}</div>`
      : '';
    return `<div class="kpi-card">
      <div class="kpi-label">${{c.label}}</div>
      <div class="kpi-value" style="color:${{c.color}}">${{c.val}}</div>
      ${{deltaHtml}}
    </div>`;
  }}).join('');
}}

// ── Sparkline SVG ──────────────────────────────────────────────────────────
function buildSparkline(values, color) {{
  if (!values || values.length < 2) return '';
  const W = 100, H = 36, pad = 2;
  const mn = Math.min(...values), mx = Math.max(...values);
  const range = mx - mn || 1;
  const pts = values.map((v, i) => {{
    const x = pad + i * (W - pad * 2) / (values.length - 1);
    const y = H - pad - (v - mn) / range * (H - pad * 2);
    return x.toFixed(1) + ',' + y.toFixed(1);
  }}).join(' ');
  return `<svg viewBox="0 0 ${{W}} ${{H}}" preserveAspectRatio="none">
    <polyline points="${{pts}}" fill="none" stroke="${{color}}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity="0.9"/>
  </svg>`;
}}

// ── City big grid ──────────────────────────────────────────────────────────
function renderCityBigGrid() {{
  const g = document.getElementById('cityBigGrid');
  const citiesBtn = document.querySelectorAll('.sidebar-item')[1];
  g.innerHTML = CITIES.map(c => {{
    const color  = CITY_COLORS[c.name] || '#34D186';
    const spark  = buildSparkline(c.spark, color);
    const sign   = (c.orders_wow !== null && c.orders_wow >= 0) ? '+' : '';
    const wowStr = c.orders_wow !== null ? `${{sign}}${{c.orders_wow.toFixed(1)}}%` : '';
    const absStr = c.orders_abs !== undefined ? fmtNumAbs(c.orders_abs) : '';
    return `<div class="city-big-card" style="border-top:3px solid ${{color}}"
             onclick="showTab('cities', document.querySelectorAll('.sidebar-item')[1])">
      <div class="city-big-name" style="color:${{color}}">${{c.name}}</div>
      <div class="city-big-orders">${{fmtNum(c.orders)}}</div>
      <div class="city-big-sub">orders · ${{fmtEur(c.gmv)}} GMV${{wowStr ? ' · ' + wowStr : ''}}</div>
      <div class="city-big-spark">${{spark}}</div>
    </div>`;
  }}).join('');
}}

// ── Movers ─────────────────────────────────────────────────────────────────
function renderMovers() {{
  const valid    = CITIES.filter(c => c.orders_wow !== null);
  const gainers  = [...valid].sort((a,b) => b.orders_wow - a.orders_wow).slice(0, 5);
  const decliners= [...valid].sort((a,b) => a.orders_wow - b.orders_wow).slice(0, 5);
  const maxAbs   = Math.max(...valid.map(c => Math.abs(c.orders_wow || 0)), 1);

  function moverRow(c, rank, inv) {{
    const pct   = Math.min(Math.abs(c.orders_wow) / maxAbs * 100, 100);
    const fill  = inv ? 'var(--danger)' : 'var(--accent)';
    const dCls  = deltaClass(c.orders_wow, inv);
    const arrow = inv ? '↓' : '↑';
    const absStr = c.orders_abs !== undefined ? fmtNumAbs(c.orders_abs) : '';
    const sign  = c.orders_wow >= 0 ? '+' : '';
    return `<div class="mover-row">
      <span class="mover-rank">${{rank}}</span>
      <span class="mover-city">${{c.name}}</span>
      <div class="mover-bar"><div class="mover-bar-fill" style="width:${{pct.toFixed(1)}}%;background:${{fill}}"></div></div>
      <span class="mover-delta ${{dCls}}">${{arrow}} <span class="mover-abs">${{absStr}}</span><span class="mover-pct">(${{sign}}${{c.orders_wow.toFixed(1)}}%)</span></span>
    </div>`;
  }}

  document.getElementById('gainersRows').innerHTML   = gainers.map((c,i)   => moverRow(c, i+1, false)).join('');
  document.getElementById('declinersRows').innerHTML = decliners.map((c,i) => moverRow(c, i+1, true)).join('');
}}

// ── City table ─────────────────────────────────────────────────────────────
let sortCol = 'gmv', sortAsc = false;

function renderCityTable() {{
  const tbody = document.getElementById('cityTableBody');
  const data  = [...CITIES].sort((a, b) => {{
    const av = a[sortCol], bv = b[sortCol];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  }});

  const T = TOTALS;
  function dc(v, inv) {{ return v !== null && v !== undefined ? ` class="${{deltaClass(v, inv)}}"` : ''; }}

  tbody.innerHTML = data.map(c => `<tr>
    <td><span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${{CITY_COLORS[c.name]||'#34D186'}};margin-right:7px;vertical-align:middle"></span>${{c.name}}</td>
    <td>${{fmtEur(c.gmv)}}</td>
    <td${{dc(c.gmv_wow,false)}}>${{fmtDelta(c.gmv_wow)}}</td>
    <td>${{fmtNum(c.orders)}}</td>
    <td${{dc(c.orders_wow,false)}}>${{fmtDelta(c.orders_wow)}}</td>
    <td>${{fmtPct(c.cml2_pct,2)}}</td>
    <td${{dc(c.cml2_wow,false)}}>${{fmtDelta(c.cml2_wow,true)}}</td>
    <td>${{fmtPct(c.di_pct,2)}}</td>
    <td${{dc(c.di_wow,true)}}>${{fmtDelta(c.di_wow,true)}}</td>
    <td>${{fmtEur(c.cpo)}}</td>
    <td${{dc(c.cpo_wow,true)}}>${{fmtDelta(c.cpo_wow,true)}}</td>
    <td>${{fmtPct(c.util_pct)}}</td>
    <td${{dc(c.util_wow,false)}}>${{fmtDelta(c.util_wow,true)}}</td>
    <td>${{fmtPct(c.ar_pct)}}</td>
    <td${{dc(c.ar_wow,false)}}>${{fmtDelta(c.ar_wow,true)}}</td>
    <td${{c.bo_pct>10?' class="delta-down"':''}}>${{fmtPct(c.bo_pct,2)}}</td>
  </tr>`).join('') + `<tr class="total-row">
    <td>All Cities</td>
    <td>${{fmtEur(T.gmv)}}</td>
    <td${{dc(T.gmv_wow,false)}}>${{fmtDelta(T.gmv_wow)}}</td>
    <td>${{fmtNum(T.orders)}}</td>
    <td${{dc(T.orders_wow,false)}}>${{fmtDelta(T.orders_wow)}}</td>
    <td>${{fmtPct(T.cml2_pct,2)}}</td>
    <td${{dc(T.cml2_wow,false)}}>${{fmtDelta(T.cml2_wow,true)}}</td>
    <td>${{fmtPct(T.di_pct,2)}}</td>
    <td${{dc(T.di_wow,true)}}>${{fmtDelta(T.di_wow,true)}}</td>
    <td>${{fmtEur(T.cpo)}}</td>
    <td${{dc(T.cpo_wow,true)}}>${{fmtDelta(T.cpo_wow,true)}}</td>
    <td>${{fmtPct(T.util_pct)}}</td>
    <td${{dc(T.util_wow,false)}}>${{fmtDelta(T.util_wow,true)}}</td>
    <td>${{fmtPct(T.ar_pct)}}</td>
    <td${{dc(T.ar_wow,false)}}>${{fmtDelta(T.ar_wow,true)}}</td>
    <td>${{fmtPct(T.bo_pct,2)}}</td>
  </tr>`;

  document.querySelectorAll('.city-table th').forEach(th => {{
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.col === sortCol) th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');
  }});
}}

document.querySelectorAll('.city-table th').forEach(th => {{
  th.addEventListener('click', () => {{
    if (sortCol === th.dataset.col) sortAsc = !sortAsc;
    else {{ sortCol = th.dataset.col; sortAsc = false; }}
    renderCityTable();
  }});
}});

// ── AM KPI cards ────────────────────────────────────────────────────────────
function renderAmGrid() {{
  const g = document.getElementById('amGrid');
  g.innerHTML = AM_KPIS.map(am => {{
    const sc = am.score;
    const badgeCls = sc >= 90 ? 'score-green' : sc >= 70 ? 'score-amber' : 'score-red';
    const kpiRows = Object.entries(am.kpis).map(([key, k]) => {{
      const pct = Math.min(k.pct, 130);
      const fillCls = key === 'churn'
        ? (k.actual === 0 ? 'fill-green' : 'fill-red')
        : pct >= 90 ? 'fill-green' : pct >= 60 ? 'fill-amber' : 'fill-red';
      return `<div class="kpi-row-item">
        <div class="kpi-row-header">
          <span class="kpi-row-name">${{KPI_LABELS[key] || key}}</span>
          <span class="kpi-row-val">${{k.actual}} / ${{k.target}} (${{k.pct.toFixed(0)}}%)</span>
        </div>
        <div class="progress-bg"><div class="progress-fill ${{fillCls}}" style="width:${{Math.min(pct,100).toFixed(1)}}%"></div></div>
      </div>`;
    }}).join('');
    return `<div class="am-card">
      <div class="am-card-header">
        <div class="am-name">${{am.name}}</div>
        <div class="am-score-badge ${{badgeCls}}">${{sc}}%</div>
      </div>
      ${{kpiRows}}
    </div>`;
  }}).join('');

  const wr = document.getElementById('weightsRow');
  wr.innerHTML = Object.entries(KPI_WEIGHTS).map(([k, w]) => `
    <div class="weight-pill">
      <span class="weight-name">${{KPI_LABELS[k]}}</span>
      <span class="weight-val">${{(w * 100).toFixed(0)}}%</span>
    </div>`).join('');
}}

// ── Init ───────────────────────────────────────────────────────────────────
renderKpiRow();
renderCityBigGrid();
renderMovers();
renderCityTable();
renderAmGrid();
lucide.createIcons();
</script>
</body>
</html>"""

with open(OUT_FILE, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'[Report] Written → {OUT_FILE}')
