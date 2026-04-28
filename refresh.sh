#!/bin/bash
# Local refresh: regenerate dashboard + push to GitHub
# Usage: ./refresh.sh
# Runs from any directory.

set -e
cd "$(dirname "$0")"

echo "▶ Running pipeline..."
python scripts/run_pipeline.py

echo "▶ Committing..."
git add index.html partner_performance.html
git diff --cached --quiet && echo "Nothing new to commit." && exit 0

git commit -m "refresh: dashboard update $(date +'%Y-%m-%d %H:%M')"

echo "▶ Pushing to GitHub..."
git push

echo "✓ Done. GitHub Pages will update in ~1 min."
