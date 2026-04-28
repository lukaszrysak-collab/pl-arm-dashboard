#!/usr/bin/env python3
"""
Bolt Food Poland — Pipeline Orchestrator
Run from the scripts/ directory: python run_pipeline.py
Optional flags:
  --skip-dbx   skip Databricks steps (use cached data)
  --only-perf  only regenerate partner_performance.html
"""

import os
import sys
import subprocess
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SKIP_DBX   = '--skip-dbx'   in sys.argv
ONLY_PERF  = '--only-perf'  in sys.argv

def run(script, label):
    path = os.path.join(SCRIPT_DIR, script)
    args = [sys.executable, path]
    if SKIP_DBX:
        args.append('--skip-dbx')
    print(f'\n{"─"*55}')
    print(f'▶  {label}')
    print(f'{"─"*55}')
    t0 = time.time()
    result = subprocess.run(args, cwd=SCRIPT_DIR)
    elapsed = time.time() - t0
    if result.returncode == 0:
        print(f'✓  {label} ({elapsed:.1f}s)')
    else:
        print(f'✗  {label} FAILED (exit {result.returncode})')
    return result.returncode == 0

steps = []

if not ONLY_PERF:
    steps += [
        ('generate_report.py',              'Main dashboard → index.html'),
    ]

steps += [
    ('generate_partner_performance.py',     'Partner performance → partner_performance.html'),
]

print('=' * 55)
print('  Bolt Food Poland — Dashboard Pipeline')
print('=' * 55)

ok = all(run(s, l) for s, l in steps)

print('\n' + '=' * 55)
print('  Pipeline complete' if ok else '  Pipeline finished with errors')
print('=' * 55)
sys.exit(0 if ok else 1)
