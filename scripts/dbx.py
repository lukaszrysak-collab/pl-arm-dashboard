"""
Databricks Connection Class
Authenticates with a Personal Access Token loaded from .env.

Usage:
    from dbx import DBX

    dbx = DBX()
    df = dbx.query("SELECT * FROM table LIMIT 10")
    dbx.close()

Or as context manager:
    with DBX() as dbx:
        df = dbx.query("SELECT * FROM table")
"""

from databricks import sql
from pathlib import Path
import pandas as pd

SERVER_HOSTNAME = "bolt-common.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/b39957853740b21d"

def _load_token():
    import os
    # 1. Environment variable (GitHub Actions / CI)
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if token:
        return token
    # 2. Local .env file
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("DATABRICKS_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABRICKS_TOKEN not found in env or .env")


class DBX:
    """Databricks connection wrapper that returns pandas DataFrames."""

    def __init__(self, http_path=None):
        self.conn = sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=http_path or HTTP_PATH,
            access_token=_load_token(),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, q, params=None):
        """Execute SQL query and return pandas DataFrame."""
        with self.conn.cursor() as cur:
            cur.execute(q, params or None)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)

    def query_to_csv(self, q, filepath, params=None):
        """Execute query and save directly to CSV."""
        df = self.query(q, params)
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} rows to {filepath}")
        return df

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    print("Testing connection...")
    with DBX() as dbx:
        df = dbx.query("SELECT 1 AS test")
        print("Connected successfully!" if len(df) else "Something went wrong")
    print("Done.")
