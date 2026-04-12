# db_utils.py
# Database connection and helper functions (T-SQL / Azure SQL with MFA)

import re
import struct
import logging
from contextlib import contextmanager

import pyodbc
import pandas as pd

from config import DB_CONFIG, TARGET_SCHEMA

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def build_connection_string() -> str:
    """Build an ODBC connection string from DB_CONFIG."""
    cfg = DB_CONFIG
    parts = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={cfg['server']}",
        f"DATABASE={cfg['database']}",
        f"Authentication={cfg['authentication']}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
        "Connection Timeout=30",
    ]
    # Add username/password only when explicitly set (service principal flow)
    if cfg.get("username"):
        parts.append(f"UID={cfg['username']}")
    if cfg.get("password"):
        parts.append(f"PWD={cfg['password']}")
    return ";".join(parts)


@contextmanager
def get_connection():
    """
    Context manager that yields an open pyodbc connection.
    ActiveDirectoryInteractive will open a browser/authenticator MFA prompt
    the first time (or when the token has expired).
    """
    conn_str = build_connection_string()
    logger.info("Opening database connection (MFA prompt may appear)...")
    conn = pyodbc.connect(conn_str, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Database connection closed.")


# ---------------------------------------------------------------------------
# Schema / table management
# ---------------------------------------------------------------------------

def sanitize_identifier(name: str) -> str:
    """Strip characters that are unsafe in T-SQL identifiers."""
    return re.sub(r"[^\w]", "_", name)


def pandas_dtype_to_tsql(dtype) -> str:
    """Map a pandas dtype to a safe T-SQL column type."""
    kind = dtype.kind
    if kind in ("i", "u"):
        return "BIGINT"
    if kind == "f":
        return "FLOAT"
    if kind == "b":
        return "BIT"
    if kind == "M":
        return "DATETIME2"
    return "NVARCHAR(MAX)"


def table_exists(cursor: pyodbc.Cursor, schema: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        schema,
        table,
    )
    return cursor.fetchone() is not None


def create_table_from_df(
    cursor: pyodbc.Cursor,
    df: pd.DataFrame,
    schema: str,
    table: str,
    drop_if_exists: bool = False,
) -> None:
    """Create a T-SQL table whose columns match the DataFrame."""
    full_name = f"[{schema}].[{table}]"

    if drop_if_exists:
        cursor.execute(f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NOT NULL DROP TABLE {full_name};")
        logger.info("Dropped existing table %s", full_name)

    col_defs = ",\n    ".join(
        f"[{sanitize_identifier(col)}] {pandas_dtype_to_tsql(df[col].dtype)}"
        for col in df.columns
    )
    ddl = f"""
    CREATE TABLE {full_name} (
        [_ingestion_id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [_source_file]  NVARCHAR(500),
        [_loaded_at]    DATETIME2 DEFAULT SYSUTCDATETIME(),
        {col_defs}
    );
    """
    cursor.execute(ddl)
    logger.info("Created table %s", full_name)


def ensure_table(
    cursor: pyodbc.Cursor,
    df: pd.DataFrame,
    schema: str,
    table: str,
    truncate: bool = False,
) -> None:
    """Create table if it doesn't exist; optionally truncate for a full reload."""
    full_name = f"[{schema}].[{table}]"
    if not table_exists(cursor, schema, table):
        create_table_from_df(cursor, df, schema, table)
    elif truncate:
        cursor.execute(f"TRUNCATE TABLE {full_name};")
        logger.info("Truncated table %s", full_name)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def add_columns_if_missing(
    cursor: pyodbc.Cursor,
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """
    Add any DataFrame columns not yet present in the target table.
    Useful when new source files introduce new fields.
    """
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        schema,
        table,
    )
    existing = {row[0].lower() for row in cursor.fetchall()}
    for col in df.columns:
        safe = sanitize_identifier(col)
        if safe.lower() not in existing:
            tsql_type = pandas_dtype_to_tsql(df[col].dtype)
            cursor.execute(
                f"ALTER TABLE [{schema}].[{table}] ADD [{safe}] {tsql_type};"
            )
            logger.info("Added column [%s] to [%s].[%s]", safe, schema, table)


def insert_dataframe(
    conn: pyodbc.Connection,
    df: pd.DataFrame,
    schema: str,
    table: str,
    source_file: str,
    batch_size: int = 500,
) -> int:
    """
    Bulk-insert a DataFrame into the target table in batches.
    Returns the total number of rows inserted.
    """
    safe_cols = [sanitize_identifier(c) for c in df.columns]
    col_list  = ", ".join(f"[{c}]" for c in safe_cols)
    placeholders = ", ".join("?" * (len(safe_cols) + 2))  # +2 for metadata cols
    sql = (
        f"INSERT INTO [{schema}].[{table}] "
        f"([_source_file], [_loaded_at], {col_list}) "
        f"VALUES ({placeholders})"
    )

    # Convert DataFrame to list of tuples; replace pd.NA / NaN with None
    rows = []
    for row in df.itertuples(index=False, name=None):
        cleaned = tuple(None if pd.isna(v) else v for v in row)
        rows.append((source_file, None, *cleaned))  # None → DEFAULT for _loaded_at

    cursor = conn.cursor()
    cursor.fast_executemany = True  # pyodbc performance flag

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(sql, batch)
        total += len(batch)
        logger.debug("Inserted batch %d rows (cumulative: %d)", len(batch), total)

    cursor.close()
    return total
