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

# Smart Promos enrolled MTD
SMART_PROMO_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT cp.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider cp
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON cp.provider_id = d.provider_id
WHERE cp.enrollment_state = 'active'
  AND cp.enrollment_source IN ('smart_promotions',
                                'portal_smart_promotions_blackbox_reenrollment')
  AND cp.created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

# Bolt+ enrolments MTD
BOLTPLUS_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT cp.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider cp
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON cp.provider_id = d.provider_id
WHERE cp.enrollment_source = 'bolt_plus'
  AND cp.enrollment_state = 'active'
  AND cp.created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

# Renegotiations MTD (commission log entries)
RENEG_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT r.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.int_provider_commission_logs r
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON r.provider_id = d.provider_id
WHERE r.log_created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

# Merchant Ads active MTD
ADS_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT h.provider_id) AS cnt
FROM spark_catalog.mart_models_spark.mart_provider_sponsored_listing_performance_hourly h
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON h.provider_id = d.provider_id
WHERE h.sponsored_listing_created_date >= '{month_start}'
  AND h.impressions > 0
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

# Marketing Campaigns MTD
MKTG_SQL = f"""
SELECT d.am_managers AS am, COUNT(DISTINCT cp.provider_id) AS cnt
FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider cp
JOIN spark_catalog.ng_delivery_spark.dim_provider d ON cp.provider_id = d.provider_id
WHERE cp.enrollment_state = 'active'
  AND cp.enrollment_source = 'portal_marketing_promotions'
  AND cp.created_date >= '{month_start}'
  AND {AM_FILTER}
  AND {FOOD_FILTER_DIM}
GROUP BY d.am_managers
"""

# Churn MTD — fact_provider_non_additive_monthly has account_manager_name directly
CHURN_SQL = f"""
SELECT account_manager_name AS am,
       SUM(CASE WHEN is_provider_churned THEN 1 ELSE 0 END) AS cnt
FROM spark_catalog.mart_models_spark.fact_provider_non_additive_monthly
WHERE timeframe_date = '{month_start}'
  AND account_manager_name IN ('Gabriela Ziółek', 'Patrycja Konfederak', 'Weronika Słomska')
GROUP BY account_manager_name
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

# ── Process city metrics ──────────────────────────────────────────────────────

import pandas as pd

def _f(v):
    try:
        return float(v) if v is not None and str(v).strip() not in ('', 'nan', 'None') else 0.0
    except (ValueError, TypeError):
        return 0.0

# Build per-city, per-period dict
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

# Build enriched city list with WoW
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
        'gmv_wow':    _wow(gmv, prev.get('gmv', 0)),
        'orders_wow': _wow(orders, prev.get('orders', 0)),
        'cml2_wow':   _wow(cml2, prev.get('cml2_pct', 0), is_pct=True),
        'di_wow':     _wow(di, prev.get('di_pct', 0), is_pct=True),
        'cpo_wow':    _wow(cpo, prev.get('cpo', 0), is_pct=True),
        'util_wow':   _wow(util, prev.get('util_pct', 0), is_pct=True),
        'ar_wow':     _wow(ar, prev.get('ar_pct', 0), is_pct=True),
    })

cities_out.sort(key=lambda c: c['gmv'], reverse=True)

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

# Previous week totals for WoW
prev_totals = defaultdict(float)
for name, periods in city_metrics.items():
    prev = periods.get('prev', {})
    for k, v in prev.items():
        prev_totals[k] += v

total_wows = {
    'gmv_wow':    _wow(totals['gmv'], prev_totals.get('gmv', 0)),
    'orders_wow': _wow(totals['orders'], prev_totals.get('orders', 0)),
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
            # Lower = better; 0 churn = 100%, any churn = 0% for now
            achievement = 1.0 if actual == 0 else 0.0
        elif target == 0:
            achievement = 1.0
        else:
            achievement = min(actual / target, 1.3)  # cap at 130%
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

cities_json  = json.dumps(cities_out,  ensure_ascii=False)
totals_json  = json.dumps({**totals, **total_wows}, ensure_ascii=False)
am_kpis_json = json.dumps(am_kpis, ensure_ascii=False)

week_label = f"W{curr_mon.isocalendar()[1]} ({curr_mon.strftime('%-d %b')}–{curr_sun.strftime('%-d %b %Y')})"

# ── Generate HTML ─────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Bolt Food Poland — Area Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/apexcharts@3.44.0/dist/apexcharts.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/gsap@3.12.2/dist/gsap.min.js"></script>
<style>
:root {{
  --bg:         #0f0f1a;
  --surface:    #16162a;
  --surface2:   #1e1e35;
  --border:     #2a2a45;
  --text:       #f0f0f5;
  --text2:      #a0a0be;
  --text3:      #6b6b88;
  --accent:     #34D186;
  --accent2:    #3b82f6;
  --accent3:    #f59e0b;
  --danger:     #ef4444;
  --purple:     #8b5cf6;
  --radius:     12px;
}}
[data-theme="light"] {{
  --bg:       #f2f3f7;
  --surface:  #ffffff;
  --surface2: #f0f0f8;
  --border:   #dfe1e8;
  --text:     #1a1a2e;
  --text2:    #4a4a60;
  --text3:    #71717a;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg);
  color: var(--text);
  display: flex;
  height: 100vh;
  overflow: hidden;
}}

/* ── Sidebar ── */
#sidebar {{
  width: 220px;
  flex-shrink: 0;
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  padding: 20px 0;
  overflow-y: auto;
}}
.sidebar-logo {{
  padding: 0 20px 24px;
  font-size: 13px;
  font-weight: 700;
  color: var(--text);
  letter-spacing: 0.05em;
  text-transform: uppercase;
  display: flex;
  align-items: center;
  gap: 10px;
}}
.sidebar-logo span {{ color: var(--accent); }}
.nav-section-label {{
  padding: 0 20px 6px;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text3);
}}
.nav-btn {{
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 20px;
  cursor: pointer;
  border: none;
  background: none;
  color: var(--text2);
  font-size: 13.5px;
  width: 100%;
  text-align: left;
  transition: all .15s;
  border-left: 3px solid transparent;
}}
.nav-btn:hover {{ background: rgba(255,255,255,0.04); color: var(--text); }}
.nav-btn.active {{
  background: rgba(52,209,134,0.08);
  color: var(--accent);
  border-left-color: var(--accent);
  font-weight: 600;
}}
.sidebar-footer {{
  margin-top: auto;
  padding: 16px 20px 4px;
  border-top: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
}}
.week-badge {{
  font-size: 11px;
  color: var(--text3);
}}
#themeToggleWrap {{ display: flex; }}

/* ── Main panel ── */
#main {{
  flex: 1;
  overflow-y: auto;
  padding: 28px 32px;
}}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}

/* ── Page header ── */
.page-header {{
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 28px;
}}
.page-title {{
  font-size: 22px;
  font-weight: 700;
  color: var(--text);
}}
.page-sub {{
  font-size: 13px;
  color: var(--text3);
}}

/* ── KPI cards ── */
.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 16px;
  margin-bottom: 28px;
}}
.kpi-card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px 22px;
  transition: border-color .2s;
}}
.kpi-card:hover {{ border-color: var(--accent); }}
.kpi-label {{
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text3);
  margin-bottom: 8px;
}}
.kpi-value {{
  font-size: 28px;
  font-weight: 700;
  color: var(--text);
  line-height: 1;
  margin-bottom: 8px;
}}
.kpi-wow {{
  font-size: 12px;
  font-weight: 600;
}}
.wow-up   {{ color: var(--accent); }}
.wow-down {{ color: var(--danger); }}
.wow-neut {{ color: var(--text3); }}

/* ── Section header ── */
.section-header {{
  font-size: 13px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text3);
  margin-bottom: 14px;
  margin-top: 8px;
}}

/* ── City table ── */
.city-table-wrap {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  overflow: auto;
  margin-bottom: 28px;
}}
.city-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}}
.city-table th {{
  padding: 12px 14px;
  text-align: right;
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text3);
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
  cursor: pointer;
  user-select: none;
  background: var(--surface);
  position: sticky;
  top: 0;
  z-index: 1;
}}
.city-table th:first-child {{ text-align: left; }}
.city-table th:hover {{ color: var(--accent); }}
.city-table th .sort-icon {{ margin-left: 4px; opacity: 0.4; }}
.city-table th.sorted-asc .sort-icon::after  {{ content: ' ↑'; opacity: 1; color: var(--accent); }}
.city-table th.sorted-desc .sort-icon::after {{ content: ' ↓'; opacity: 1; color: var(--accent); }}
.city-table td {{
  padding: 11px 14px;
  text-align: right;
  border-bottom: 1px solid var(--border);
  color: var(--text);
}}
.city-table td:first-child {{ text-align: left; font-weight: 600; }}
.city-table tr:last-child td {{ border-bottom: none; }}
.city-table tr:hover td {{ background: rgba(255,255,255,0.02); }}
.city-table .total-row td {{
  font-weight: 700;
  color: var(--accent);
  border-top: 2px solid var(--border);
}}
.cell-wow {{ font-size: 11px; display: block; margin-top: 2px; }}

/* ── AM KPI cards ── */
.am-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 20px;
  margin-bottom: 28px;
}}
.am-card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 22px 24px;
}}
.am-name {{
  font-size: 15px;
  font-weight: 700;
  color: var(--text);
  margin-bottom: 4px;
}}
.am-score {{
  font-size: 12px;
  color: var(--text3);
  margin-bottom: 18px;
}}
.am-score span {{ font-weight: 700; color: var(--accent); font-size: 18px; }}
.kpi-row {{
  margin-bottom: 12px;
}}
.kpi-row-header {{
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  margin-bottom: 5px;
  color: var(--text2);
}}
.kpi-row-header .kpi-name {{ font-weight: 600; }}
.kpi-row-header .kpi-val {{ color: var(--text3); }}
.progress-bar-bg {{
  height: 6px;
  background: var(--surface2);
  border-radius: 4px;
  overflow: hidden;
}}
.progress-bar-fill {{
  height: 100%;
  border-radius: 4px;
  transition: width 0.8s cubic-bezier(0.25,0.46,0.45,0.94);
}}
.fill-green  {{ background: var(--accent); }}
.fill-amber  {{ background: var(--accent3); }}
.fill-red    {{ background: var(--danger); }}
.fill-purple {{ background: var(--purple); }}

/* ── Movers ── */
.movers-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  margin-bottom: 28px;
}}
.movers-card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 18px 20px;
}}
.movers-card h3 {{
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text3);
  margin-bottom: 14px;
}}
.mover-row {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 7px 0;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
}}
.mover-row:last-child {{ border-bottom: none; }}
.mover-city {{ font-weight: 600; }}
.mover-delta {{ font-weight: 700; }}

/* ── Responsive ── */
@media (max-width: 900px) {{
  #sidebar {{ display: none; }}
  #main {{ padding: 16px; }}
}}
</style>
<script src="theme.js" defer></script>
</head>
<body>

<!-- Sidebar -->
<nav id="sidebar">
  <div class="sidebar-logo">
    <span>⚡</span> PL ARM
  </div>

  <div class="nav-section-label">Analytics</div>
  <button class="nav-btn active" onclick="showTab('overview',this)">
    📊 Overview
  </button>
  <button class="nav-btn" onclick="showTab('cities',this)">
    🏙️ Cities
  </button>
  <button class="nav-btn" onclick="showTab('am_kpis',this)">
    🎯 AM KPIs
  </button>

  <div class="sidebar-footer">
    <span class="week-badge">{week_label}</span>
    <div id="themeToggleWrap"></div>
  </div>
</nav>

<!-- Main -->
<main id="main">

  <!-- Overview tab -->
  <div id="tab-overview" class="tab-panel active">
    <div class="page-header">
      <span class="page-title">Poland Overview</span>
      <span class="page-sub" id="overviewWeek">{week_label}</span>
    </div>

    <div class="kpi-grid" id="kpiGrid"></div>

    <div class="section-header">City Movers (Orders WoW)</div>
    <div class="movers-grid" id="moversGrid"></div>
  </div>

  <!-- Cities tab -->
  <div id="tab-cities" class="tab-panel">
    <div class="page-header">
      <span class="page-title">City Breakdown</span>
      <span class="page-sub">Click column headers to sort</span>
    </div>
    <div class="city-table-wrap">
      <table class="city-table" id="cityTable">
        <thead>
          <tr>
            <th data-col="name"    data-type="str">City <span class="sort-icon"></span></th>
            <th data-col="gmv"     data-type="num">GMV <span class="sort-icon"></span></th>
            <th data-col="gmv_wow" data-type="num">GMV WoW <span class="sort-icon"></span></th>
            <th data-col="orders"  data-type="num">Orders <span class="sort-icon"></span></th>
            <th data-col="orders_wow" data-type="num">Ord WoW <span class="sort-icon"></span></th>
            <th data-col="cml2_pct" data-type="num">CML2% <span class="sort-icon"></span></th>
            <th data-col="cml2_wow" data-type="num">CML2 Δ <span class="sort-icon"></span></th>
            <th data-col="di_pct"  data-type="num">DI% <span class="sort-icon"></span></th>
            <th data-col="di_wow"  data-type="num">DI Δ <span class="sort-icon"></span></th>
            <th data-col="cpo"     data-type="num">CPO <span class="sort-icon"></span></th>
            <th data-col="cpo_wow" data-type="num">CPO Δ <span class="sort-icon"></span></th>
            <th data-col="util_pct" data-type="num">Util% <span class="sort-icon"></span></th>
            <th data-col="ar_pct"  data-type="num">AR% <span class="sort-icon"></span></th>
            <th data-col="bo_pct"  data-type="num">BO% <span class="sort-icon"></span></th>
            <th data-col="merchants" data-type="num">Merchants <span class="sort-icon"></span></th>
          </tr>
        </thead>
        <tbody id="cityTableBody"></tbody>
      </table>
    </div>
  </div>

  <!-- AM KPIs tab -->
  <div id="tab-am_kpis" class="tab-panel">
    <div class="page-header">
      <span class="page-title">AM KPIs — Q2 2026</span>
      <span class="page-sub">Month-to-date vs monthly targets (Apr–Jun). Bonus: 90%–130% achievement.</span>
    </div>
    <div class="am-grid" id="amGrid"></div>

    <div class="section-header">KPI Weights</div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px" id="weightsRow"></div>
  </div>

</main>

<script>
const CITIES  = {cities_json};
const TOTALS  = {totals_json};
const AM_KPIS = {am_kpis_json};
const AM_TARGETS = {json.dumps(AM_TARGETS, ensure_ascii=False)};
const KPI_WEIGHTS = {json.dumps(KPI_WEIGHTS, ensure_ascii=False)};

const KPI_LABELS = {{
  churn:    'Regrettable Churn',
  reneg:    'Renegotiations',
  mktg:     'Mktg Campaigns',
  smart:    'Smart Promos',
  ads:      'Merchant Ads',
  boltplus: 'Bolt+ Enrolments',
}};

// ── Tab switching ──────────────────────────────────────────────────────────
function showTab(id, btn) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  if (btn) btn.classList.add('active');
}}

// ── Formatters ─────────────────────────────────────────────────────────────
function fmtEur(v) {{
  if (!v && v !== 0) return '—';
  if (v >= 1e6) return '€' + (v/1e6).toFixed(2) + 'M';
  if (v >= 1e3) return '€' + (v/1e3).toFixed(1) + 'k';
  return '€' + v.toFixed(2);
}}
function fmtNum(v) {{
  if (!v && v !== 0) return '—';
  if (v >= 1e6) return (v/1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v/1e3).toFixed(1) + 'k';
  return v.toFixed(0);
}}
function fmtPct(v, decimals=1) {{
  if (v === null || v === undefined) return '—';
  return v.toFixed(decimals) + '%';
}}
function wowClass(v, invert=false) {{
  if (v === null || v === undefined) return 'wow-neut';
  const good = invert ? v < 0 : v > 0;
  return good ? 'wow-up' : v === 0 ? 'wow-neut' : 'wow-down';
}}
function fmtWow(v, suffix='%', is_pp=false) {{
  if (v === null || v === undefined) return '—';
  const sign = v >= 0 ? '+' : '';
  const s = is_pp ? 'pp' : suffix;
  return sign + v.toFixed(1) + s;
}}

// ── Overview KPI cards ─────────────────────────────────────────────────────
function renderKpiGrid() {{
  const g = document.getElementById('kpiGrid');
  const kpis = [
    {{ label:'GMV',          val: fmtEur(TOTALS.gmv),        wow: TOTALS.gmv_wow,    wowFmt: fmtWow(TOTALS.gmv_wow),    inv:false }},
    {{ label:'Orders',       val: fmtNum(TOTALS.orders),     wow: TOTALS.orders_wow, wowFmt: fmtWow(TOTALS.orders_wow), inv:false }},
    {{ label:'CML2%',        val: fmtPct(TOTALS.cml2_pct,2), wow: null,              wowFmt:'',                         inv:false }},
    {{ label:'DI%',          val: fmtPct(TOTALS.di_pct,2),   wow: null,              wowFmt:'',                         inv:true  }},
    {{ label:'CPO',          val: fmtEur(TOTALS.cpo),        wow: null,              wowFmt:'',                         inv:true  }},
    {{ label:'Courier Util', val: fmtPct(TOTALS.util_pct),   wow: null,              wowFmt:'',                         inv:false }},
    {{ label:'Courier AR%',  val: fmtPct(TOTALS.ar_pct),     wow: null,              wowFmt:'',                         inv:false }},
    {{ label:'BO%',          val: fmtPct(TOTALS.bo_pct,2),   wow: null,              wowFmt:'',                         inv:true  }},
  ];
  g.innerHTML = kpis.map(k => `
    <div class="kpi-card">
      <div class="kpi-label">${{k.label}}</div>
      <div class="kpi-value" data-counter="${{k.val}}">${{k.val}}</div>
      ${{k.wow !== null ? `<div class="kpi-wow ${{wowClass(k.wow, k.inv)}}">${{k.wowFmt}} WoW</div>` : ''}}
    </div>`).join('');
}}

// ── City movers ────────────────────────────────────────────────────────────
function renderMovers() {{
  const g = document.getElementById('moversGrid');
  const sorted = [...CITIES].filter(c => c.orders_wow !== null);
  const gainers  = [...sorted].sort((a,b) => b.orders_wow - a.orders_wow).slice(0,4);
  const decliners= [...sorted].sort((a,b) => a.orders_wow - b.orders_wow).slice(0,4);

  function rows(list, inv) {{
    return list.map(c => `
      <div class="mover-row">
        <span class="mover-city">${{c.name}}</span>
        <span class="mover-delta ${{wowClass(c.orders_wow, inv)}}">${{fmtWow(c.orders_wow)}}</span>
      </div>`).join('');
  }}

  g.innerHTML = `
    <div class="movers-card">
      <h3>📈 Gainers</h3>
      ${{rows(gainers, false)}}
    </div>
    <div class="movers-card">
      <h3>📉 Decliners</h3>
      ${{rows(decliners, true)}}
    </div>`;
}}

// ── City table ─────────────────────────────────────────────────────────────
let sortCol = 'gmv', sortAsc = false;

function renderCityTable() {{
  const tbody = document.getElementById('cityTableBody');
  const data  = [...CITIES];
  data.sort((a,b) => {{
    const av = a[sortCol], bv = b[sortCol];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  }});

  // Totals row
  const T = TOTALS;
  function wowCell(v, inv=false, is_pp=false) {{
    if (v === null || v === undefined) return '';
    const cls = wowClass(v, inv);
    return `<span class="cell-wow ${{cls}}">${{fmtWow(v, '%', is_pp)}}</span>`;
  }}

  tbody.innerHTML = data.map(c => `
    <tr>
      <td>${{c.name}}</td>
      <td>${{fmtEur(c.gmv)}}  ${{wowCell(c.gmv_wow)}}</td>
      <td class="${{wowClass(c.gmv_wow)}}">${{c.gmv_wow!==null?fmtWow(c.gmv_wow):'—'}}</td>
      <td>${{fmtNum(c.orders)}} ${{wowCell(c.orders_wow)}}</td>
      <td class="${{wowClass(c.orders_wow)}}">${{c.orders_wow!==null?fmtWow(c.orders_wow):'—'}}</td>
      <td>${{fmtPct(c.cml2_pct,2)}}</td>
      <td class="${{wowClass(c.cml2_wow)}}">${{c.cml2_wow!==null?fmtWow(c.cml2_wow,'%',true):'—'}}</td>
      <td>${{fmtPct(c.di_pct,2)}}</td>
      <td class="${{wowClass(c.di_wow,true)}}">${{c.di_wow!==null?fmtWow(c.di_wow,'%',true):'—'}}</td>
      <td>${{fmtEur(c.cpo)}}</td>
      <td class="${{wowClass(c.cpo_wow,true)}}">${{c.cpo_wow!==null?fmtWow(c.cpo_wow,'%',true):'—'}}</td>
      <td>${{fmtPct(c.util_pct)}}</td>
      <td>${{fmtPct(c.ar_pct)}}</td>
      <td class="${{c.bo_pct>10?'wow-down':''}}">${{fmtPct(c.bo_pct,2)}}</td>
      <td>${{fmtNum(c.merchants)}}</td>
    </tr>`).join('') + `
    <tr class="total-row">
      <td>All Cities</td>
      <td>${{fmtEur(T.gmv)}}</td>
      <td class="${{wowClass(T.gmv_wow)}}">${{T.gmv_wow!==null?fmtWow(T.gmv_wow):'—'}}</td>
      <td>${{fmtNum(T.orders)}}</td>
      <td class="${{wowClass(T.orders_wow)}}">${{T.orders_wow!==null?fmtWow(T.orders_wow):'—'}}</td>
      <td>${{fmtPct(T.cml2_pct,2)}}</td>
      <td>—</td>
      <td>${{fmtPct(T.di_pct,2)}}</td>
      <td>—</td>
      <td>${{fmtEur(T.cpo)}}</td>
      <td>—</td>
      <td>${{fmtPct(T.util_pct)}}</td>
      <td>${{fmtPct(T.ar_pct)}}</td>
      <td>${{fmtPct(T.bo_pct,2)}}</td>
      <td>${{fmtNum(T.merchants)}}</td>
    </tr>`;

  document.querySelectorAll('.city-table th').forEach(th => {{
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.col === sortCol) {{
      th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');
    }}
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
    const scoreColor = am.score >= 90 ? 'fill-green' : am.score >= 70 ? 'fill-amber' : 'fill-red';
    const kpiRows = Object.entries(am.kpis).map(([key, k]) => {{
      const pct = Math.min(k.pct, 130);
      const fillClass = key === 'churn'
        ? (k.actual === 0 ? 'fill-green' : 'fill-red')
        : pct >= 90 ? 'fill-green' : pct >= 60 ? 'fill-amber' : 'fill-red';
      const label = KPI_LABELS[key] || key;
      return `
        <div class="kpi-row">
          <div class="kpi-row-header">
            <span class="kpi-name">${{label}}</span>
            <span class="kpi-val">${{k.actual}} / ${{k.target}} (${{k.pct.toFixed(0)}}%)</span>
          </div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill ${{fillClass}}" style="width:${{Math.min(pct, 100).toFixed(1)}}%"></div>
          </div>
        </div>`;
    }}).join('');
    return `
      <div class="am-card">
        <div class="am-name">${{am.name}}</div>
        <div class="am-score">Overall score: <span>${{am.score}}%</span></div>
        ${{kpiRows}}
      </div>`;
  }}).join('');

  // Weight badges
  const wr = document.getElementById('weightsRow');
  wr.innerHTML = Object.entries(KPI_WEIGHTS).map(([k,w]) => `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;font-size:12px;">
      <span style="color:var(--text3)">${{KPI_LABELS[k]}}</span>
      <span style="font-weight:700;color:var(--accent);margin-left:6px">${{(w*100).toFixed(0)}}%</span>
    </div>`).join('');
}}

// ── Init ───────────────────────────────────────────────────────────────────
renderKpiGrid();
renderMovers();
renderCityTable();
renderAmGrid();
</script>
</body>
</html>"""

with open(OUT_FILE, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'[Report] Written → {OUT_FILE}')
