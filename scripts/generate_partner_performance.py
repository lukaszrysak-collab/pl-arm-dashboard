#!/usr/bin/env python3
"""
Bolt Food Poland — Partner & AM Performance Tracker
Generates partner_performance.html with 4-week brand-level metrics per city.
Data source: fact_provider_daily + dim_provider + dim_provider_v2
"""

import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from dbx import DBX
from cities import (
    CITY_ID_MAP, CITY_IDS_CSV, AM_NAMES,
    FOOD_FILTER_DIM, AM_FILTER,
    complete_weeks,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR   = os.path.dirname(SCRIPT_DIR)
DATA_DIR   = os.path.join(ROOT_DIR, 'data')
OUT_FILE   = os.path.join(ROOT_DIR, 'partner_performance.html')
CACHE_FILE = os.path.join(DATA_DIR, 'partner_perf_cache.csv')
os.makedirs(DATA_DIR, exist_ok=True)

# ── Date ranges: 4 complete Mon–Sun weeks ────────────────────────────────────

weeks = complete_weeks(4)
oldest_mon = weeks[0][0]
latest_sun = weeks[-1][1]

print(f'[PartnerPerf] Weeks: {oldest_mon} – {latest_sun}')

# Build week labels e.g. "W15"
def week_label(monday):
    return f"W{monday.isocalendar()[1]} ({monday.strftime('%-d %b')})"

week_labels = [week_label(mon) for mon, _ in weeks]

# ── SQL ───────────────────────────────────────────────────────────────────────

SQL = f"""
WITH brand_info AS (
    SELECT
        p.provider_id,
        COALESCE(p.brand_name, p.provider_name)    AS brand,
        p.provider_city_id                          AS city_id,
        p.am_managers                               AS am,
        COALESCE(p.business_segment_v2, 'Other')    AS segment
    FROM spark_catalog.ng_delivery_spark.dim_provider p
    WHERE p.provider_city_id IN ({CITY_IDS_CSV})
      AND p.provider_status = 'active'
      AND p.is_bolt_market_provider = false
      AND p.is_grocery_segment = false
),
promo_active AS (
    SELECT DISTINCT provider_id
    FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider
    WHERE enrollment_state = 'active'
      AND expires_at > CURRENT_TIMESTAMP()
      AND enrollment_source IN ('portal_regular_promotions',
                                'portal_smart_promotions_blackbox_reenrollment',
                                'smart_promotions')
),
bplus_active AS (
    SELECT DISTINCT provider_id
    FROM spark_catalog.ng_delivery_spark.delivery_campaign_provider
    WHERE enrollment_state = 'active'
      AND expires_at > CURRENT_TIMESTAMP()
      AND enrollment_source = 'bolt_plus'
)
SELECT
    bi.city_id,
    bi.brand,
    bi.am,
    bi.segment,
    DATE_TRUNC('WEEK', pd.observation_date)                                      AS week_start,
    SUM(pd.delivered_orders)                                                     AS orders,
    SUM(pd.gmv_orders_eur)                                                       AS gmv,
    SUM(pd.gmv_orders_eur)
        / NULLIF(SUM(pd.delivered_orders), 0)                                    AS aov,
    SUM(pd.commission_eur)
        / NULLIF(SUM(pd.gmv_orders_eur), 0)                                      AS cm_pct,
    SUM(pd.honey_order_rate_value * pd.honey_order_rate_weight)
        / NULLIF(SUM(pd.honey_order_rate_weight), 0)                             AS bd_pct,
    SUM(pd.commission_eur)
        / NULLIF(SUM(pd.gmv_orders_eur), 0)                                      AS commission_pct,
    SUM(pd.provider_acceptance_rate_value * pd.provider_acceptance_rate_weight)
        / NULLIF(SUM(pd.provider_acceptance_rate_weight), 0)                     AS avail_rate,
    SUM(pd.food_activated_users)                                                 AS new_users,
    MAX(CASE WHEN pa.provider_id IS NOT NULL THEN 1 ELSE 0 END)                  AS has_promo,
    MAX(CASE WHEN ba.provider_id IS NOT NULL THEN 1 ELSE 0 END)                  AS has_bplus
FROM spark_catalog.ng_delivery_spark.fact_provider_daily pd
JOIN brand_info bi
    ON pd.provider_id = bi.provider_id
LEFT JOIN promo_active pa ON pd.provider_id = pa.provider_id
LEFT JOIN bplus_active ba ON pd.provider_id = ba.provider_id
WHERE pd.observation_date BETWEEN '{oldest_mon}' AND '{latest_sun}'
  AND bi.city_id IN ({CITY_IDS_CSV})
GROUP BY bi.city_id, bi.brand, bi.am, bi.segment, week_start
HAVING orders > 0
ORDER BY bi.city_id, week_start, orders DESC
"""

print('[PartnerPerf] Querying Databricks...')
try:
    with DBX() as dbx:
        df = dbx.query(SQL)
    df.to_csv(CACHE_FILE, index=False)
    print(f'[PartnerPerf] {len(df)} rows fetched, cached → {CACHE_FILE}')
except Exception as e:
    print(f'[PartnerPerf] DBX failed ({e}), trying cache...')
    import pandas as pd
    if os.path.exists(CACHE_FILE):
        df = pd.read_csv(CACHE_FILE)
        print(f'[PartnerPerf] Loaded {len(df)} rows from cache')
    else:
        print('[PartnerPerf] No cache available, exiting')
        sys.exit(1)

# ── Process ───────────────────────────────────────────────────────────────────

def _f(v):
    try:
        return float(v) if v is not None and str(v).strip() not in ('', 'nan', 'None') else 0.0
    except (ValueError, TypeError):
        return 0.0

def _norm_seg(s):
    s = str(s).strip().upper() if s else ''
    if s.startswith('ENT'): return 'ENT'
    if s == 'MM': return 'MM'
    if 'SMB' in s: return 'SMB'
    return 'Other'

# week_start strings from the query
raw_weeks = sorted(df['week_start'].dropna().unique())
week_keys  = [str(w)[:10] for w in raw_weeks]
print(f'[PartnerPerf] Week keys: {week_keys}')

# Map each week_start → label index
week_idx = {k: i for i, k in enumerate(week_keys)}
n_weeks  = len(week_keys)

# city → brand → {am, seg, weeks: {idx: metrics}}
city_data = defaultdict(lambda: defaultdict(lambda: {'am': '', 'seg': '', 'weeks': {}}))

for _, r in df.iterrows():
    cid   = int(_f(r['city_id']))
    city  = CITY_ID_MAP.get(cid)
    if not city:
        continue
    brand = str(r['brand']).strip()
    wkey  = str(r['week_start'])[:10]
    widx  = week_idx.get(wkey, -1)
    if widx < 0:
        continue

    am  = str(r['am']).strip() if r['am'] and str(r['am']).strip() not in ('', 'None', 'nan') else ''
    seg = _norm_seg(r['segment'])

    entry = city_data[city][brand]
    if am:
        entry['am'] = am
    if seg and seg != 'Other':
        entry['seg'] = seg

    entry['weeks'][widx] = {
        'orders':  _f(r['orders']),
        'gmv':     _f(r['gmv']),
        'aov':     _f(r['aov']),
        'cm_pct':  _f(r['cm_pct']),
        'bd_pct':  _f(r['bd_pct']),
        'comm_pct':_f(r['commission_pct']),
        'avail':   _f(r['avail_rate']),
        'new_users':_f(r['new_users']),
        'promo':   int(_f(r['has_promo'])),
        'bplus':   int(_f(r['has_bplus'])),
    }

# Build AM full-portfolio KPIs (across all cities)
am_full = defaultdict(lambda: {'orders':0, 'gmv':0, 'cm_sum':0, 'cm_w':0})
for city, brands in city_data.items():
    for brand, data in brands.items():
        am = data['am']
        if not am:
            continue
        latest = data['weeks'].get(n_weeks - 1, data['weeks'].get(max(data['weeks'].keys(), default=-1), {}))
        if not latest:
            continue
        am_full[am]['orders'] += latest.get('orders', 0)
        am_full[am]['gmv']    += latest.get('gmv', 0)
        cm  = latest.get('cm_pct', 0)
        ord_ = latest.get('orders', 0)
        am_full[am]['cm_sum'] += cm * ord_
        am_full[am]['cm_w']   += ord_

# Top-N partners per city for display
TOP_N = 15

cities_out = {}
for city in sorted(CITY_ID_MAP.values()):
    brands = city_data.get(city, {})
    # Sort by latest week orders descending
    def latest_orders(item):
        weeks_d = item[1]['weeks']
        if not weeks_d:
            return 0
        return weeks_d.get(max(weeks_d.keys()), {}).get('orders', 0)
    sorted_brands = sorted(brands.items(), key=latest_orders, reverse=True)[:TOP_N]
    cities_out[city] = [
        {
            'brand': brand,
            'am':    data['am'],
            'seg':   data['seg'],
            'weeks': {str(i): data['weeks'].get(i, None) for i in range(n_weeks)},
        }
        for brand, data in sorted_brands
    ]

am_summary = {}
for am in AM_NAMES:
    d = am_full.get(am, {})
    am_summary[am] = {
        'orders': round(d.get('orders', 0), 0),
        'gmv':    round(d.get('gmv', 0), 2),
        'cm_pct': round(d['cm_sum'] / d['cm_w'] * 100, 2) if d.get('cm_w', 0) else 0,
    }

cities_json   = json.dumps(cities_out,   ensure_ascii=False)
am_sum_json   = json.dumps(am_summary,   ensure_ascii=False)
week_lbls_json = json.dumps(week_labels[:n_weeks], ensure_ascii=False)
week_keys_json = json.dumps(week_keys,   ensure_ascii=False)

# ── Generate HTML ─────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PL Partner Performance</title>
<style>
:root {{
  --bg:#0f0f1a; --surface:#16162a; --surface2:#1e1e35;
  --border:#2a2a45; --text:#f0f0f5; --text2:#a0a0be; --text3:#6b6b88;
  --accent:#34D186; --accent2:#3b82f6; --danger:#ef4444; --amber:#f59e0b;
  --purple:#8b5cf6; --radius:10px;
}}
[data-theme="light"] {{
  --bg:#f2f3f7; --surface:#fff; --surface2:#f0f0f8; --border:#dfe1e8;
  --text:#1a1a2e; --text2:#4a4a60; --text3:#71717a;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:var(--bg);color:var(--text);padding:24px 28px}}
h1{{font-size:20px;font-weight:700;margin-bottom:4px}}
.subtitle{{font-size:13px;color:var(--text3);margin-bottom:24px}}

/* Controls */
.controls{{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin-bottom:20px}}
.filter-group{{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text2)}}
select,input[type=text]{{
  background:var(--surface);border:1px solid var(--border);border-radius:8px;
  color:var(--text);padding:7px 12px;font-size:13px;outline:none;
}}
select:focus,input:focus{{border-color:var(--accent)}}
.home-link{{
  margin-left:auto;font-size:13px;color:var(--accent);text-decoration:none;
  border:1px solid var(--border);border-radius:8px;padding:7px 14px;
}}
.home-link:hover{{background:rgba(52,209,134,0.08)}}

/* AM summary cards */
.am-summary{{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:24px}}
.am-chip{{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
  padding:12px 18px;cursor:pointer;transition:all .15s;font-size:13px;
}}
.am-chip.active{{border-color:var(--accent);background:rgba(52,209,134,0.08)}}
.am-chip:hover{{border-color:var(--accent)}}
.am-chip-name{{font-weight:700;margin-bottom:4px}}
.am-chip-stats{{font-size:11px;color:var(--text3)}}

/* City sections */
.city-section{{margin-bottom:32px}}
.city-header{{
  font-size:14px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;
  color:var(--text3);margin-bottom:10px;padding:8px 0;
  border-bottom:1px solid var(--border);
}}

/* Partner table */
.table-wrap{{overflow-x:auto;border-radius:var(--radius);
  border:1px solid var(--border);background:var(--surface)}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th{{
  padding:9px 11px;text-align:right;font-size:10.5px;font-weight:700;
  text-transform:uppercase;letter-spacing:0.05em;color:var(--text3);
  border-bottom:1px solid var(--border);white-space:nowrap;
  position:sticky;top:0;background:var(--surface);cursor:pointer;
}}
th:first-child,th:nth-child(2),th:nth-child(3){{text-align:left}}
th:hover{{color:var(--accent)}}
td{{
  padding:9px 11px;text-align:right;border-bottom:1px solid var(--border);
  color:var(--text);white-space:nowrap;
}}
td:first-child,td:nth-child(2),td:nth-child(3){{text-align:left}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:rgba(255,255,255,0.02)}}

.badge{{
  display:inline-block;padding:2px 7px;border-radius:12px;
  font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.04em;
}}
.badge-ent{{background:rgba(139,92,246,0.15);color:#a78bfa}}
.badge-mm {{background:rgba(59,130,246,0.15);color:#60a5fa}}
.badge-smb{{background:rgba(52,209,134,0.15);color:var(--accent)}}

.wow-up{{color:var(--accent)}}
.wow-dn{{color:var(--danger)}}

.tag-promo{{
  display:inline-block;padding:2px 6px;border-radius:6px;font-size:10px;
  background:rgba(245,158,11,0.15);color:var(--amber);font-weight:700;
}}
.tag-bplus{{
  display:inline-block;padding:2px 6px;border-radius:6px;font-size:10px;
  background:rgba(139,92,246,0.15);color:#a78bfa;font-weight:700;margin-left:3px;
}}

.hidden{{display:none!important}}
</style>
<script src="theme.js" defer></script>
</head>
<body>

<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px">
  <div>
    <h1>Partner Performance — Poland</h1>
    <div class="subtitle">Top {TOP_N} partners per city · 4 complete weeks · Click AM chip to filter</div>
  </div>
  <div style="display:flex;gap:10px;align-items:center">
    <div id="themeToggleWrap"></div>
    <a href="index.html" class="home-link">← Dashboard</a>
  </div>
</div>

<!-- AM summary chips -->
<div class="am-summary" id="amSummary"></div>

<!-- Filters -->
<div class="controls">
  <div class="filter-group">
    Segment:
    <select id="segFilter">
      <option value="">All</option>
      <option value="ENT">ENT</option>
      <option value="MM">MM</option>
      <option value="SMB">SMB</option>
    </select>
  </div>
  <div class="filter-group">
    Brand:
    <input type="text" id="brandSearch" placeholder="Search brand…" style="width:180px">
  </div>
  <div class="filter-group">
    City:
    <select id="cityFilter"><option value="">All cities</option></select>
  </div>
</div>

<div id="content"></div>

<script>
const CITIES_DATA  = {cities_json};
const AM_SUMMARY   = {am_sum_json};
const WEEK_LABELS  = {week_lbls_json};
const WEEK_KEYS    = {week_keys_json};
const N_WEEKS      = WEEK_LABELS.length;

let activeAM = null;
let activeSeg = '';
let brandQ = '';
let cityFilter = '';

const fmtEur = v => v ? (v >= 1000 ? '€'+(v/1000).toFixed(1)+'k' : '€'+v.toFixed(0)) : '—';
const fmtPct = v => v ? v.toFixed(1)+'%' : '—';
const fmtNum = v => v ? (v >= 1000 ? (v/1000).toFixed(1)+'k' : v.toFixed(0)) : '—';

function wow(curr, prev) {{
  if (!curr || !prev) return '';
  const d = (curr - prev) / prev * 100;
  const cls = d > 0 ? 'wow-up' : 'wow-dn';
  const sign = d > 0 ? '▲' : '▼';
  return `<span class="${{cls}}" style="font-size:10px;margin-left:3px">${{sign}}${{Math.abs(d).toFixed(0)}}%</span>`;
}}

// AM chips
function renderAmChips() {{
  const el = document.getElementById('amSummary');
  const ams = Object.keys(AM_SUMMARY);
  el.innerHTML = `<div class="am-chip ${{activeAM===null?'active':''}}" onclick="setAM(null,this)">
    <div class="am-chip-name">All AMs</div>
    <div class="am-chip-stats">All cities</div>
  </div>` + ams.map(am => {{
    const s = AM_SUMMARY[am];
    return `<div class="am-chip ${{activeAM===am?'active':''}}" onclick="setAM('${{am}}',this)">
      <div class="am-chip-name">${{am.split(' ')[0]}}</div>
      <div class="am-chip-stats">${{fmtNum(s.orders)}} ord · ${{fmtPct(s.cm_pct)}} CM</div>
    </div>`;
  }}).join('');
}}

function setAM(am, el) {{
  activeAM = am;
  document.querySelectorAll('.am-chip').forEach(c=>c.classList.remove('active'));
  if (el) el.classList.add('active');
  renderContent();
}}

// City filter
function populateCityFilter() {{
  const sel = document.getElementById('cityFilter');
  Object.keys(CITIES_DATA).forEach(c => {{
    const o = document.createElement('option');
    o.value = c; o.textContent = c;
    sel.appendChild(o);
  }});
}}

// Main render
function renderContent() {{
  activeSeg  = document.getElementById('segFilter').value;
  brandQ     = document.getElementById('brandSearch').value.toLowerCase();
  cityFilter = document.getElementById('cityFilter').value;

  const content = document.getElementById('content');
  let html = '';

  for (const [city, partners] of Object.entries(CITIES_DATA)) {{
    if (cityFilter && city !== cityFilter) continue;

    const filtered = partners.filter(p => {{
      if (activeAM && p.am !== activeAM) return false;
      if (activeSeg && p.seg !== activeSeg) return false;
      if (brandQ && !p.brand.toLowerCase().includes(brandQ)) return false;
      return true;
    }});
    if (!filtered.length) continue;

    // Table header: columns = Brand, AM, Seg, then N weeks × [Orders, GMV, CM%]
    let thead = `<tr>
      <th>Brand</th><th>AM</th><th>Seg</th>`;
    for (let i = N_WEEKS - 1; i >= 0; i--) {{
      const lbl = WEEK_LABELS[i] || 'W?';
      thead += `<th colspan="4" style="text-align:center;border-left:1px solid var(--border)">${{lbl}}</th>`;
    }}
    thead += '</tr><tr><th></th><th></th><th></th>';
    for (let i = 0; i < N_WEEKS; i++) {{
      thead += `<th style="border-left:1px solid var(--border)">Orders</th>
                <th>GMV</th><th>CM%</th><th>Avail</th>`;
    }}
    thead += '</tr>';

    let tbody = filtered.map(p => {{
      const segBadge = {{ENT:'badge-ent',MM:'badge-mm',SMB:'badge-smb'}}[p.seg] || '';
      const tags = (p.weeks[N_WEEKS-1]?.promo ? '<span class="tag-promo">Promo</span>' : '')
                 + (p.weeks[N_WEEKS-1]?.bplus ? '<span class="tag-bplus">B+</span>'  : '');

      let cells = '';
      for (let i = N_WEEKS - 1; i >= 0; i--) {{
        const w  = p.weeks[String(i)];
        const wp = p.weeks[String(i-1)];
        if (!w) {{
          cells += `<td style="border-left:1px solid var(--border)">—</td><td>—</td><td>—</td><td>—</td>`;
          continue;
        }}
        cells += `
          <td style="border-left:1px solid var(--border)">${{fmtNum(w.orders)}}${{wow(w.orders, wp?.orders)}}</td>
          <td>${{fmtEur(w.gmv)}}</td>
          <td>${{fmtPct(w.cm_pct * 100)}}</td>
          <td>${{fmtPct(w.avail * 100)}}</td>`;
      }}
      return `<tr>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">
          <span style="font-weight:600">${{p.brand}}</span> ${{tags}}
        </td>
        <td style="color:var(--text3);font-size:11px">${{p.am ? p.am.split(' ')[0] : '—'}}</td>
        <td>${{p.seg ? `<span class="badge ${{segBadge}}">${{p.seg}}</span>` : ''}}</td>
        ${{cells}}
      </tr>`;
    }}).join('');

    html += `<div class="city-section">
      <div class="city-header">${{city}}</div>
      <div class="table-wrap"><table><thead>${{thead}}</thead><tbody>${{tbody}}</tbody></table></div>
    </div>`;
  }}

  content.innerHTML = html || '<p style="color:var(--text3);padding:20px">No partners match current filters.</p>';
}}

// Init
populateCityFilter();
renderAmChips();
renderContent();

document.getElementById('segFilter').addEventListener('change', renderContent);
document.getElementById('brandSearch').addEventListener('input', renderContent);
document.getElementById('cityFilter').addEventListener('change', renderContent);
</script>
</body>
</html>"""

with open(OUT_FILE, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'[PartnerPerf] Written → {OUT_FILE}')
