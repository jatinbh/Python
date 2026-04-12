# ingest.py
# Main entry point — discovers Excel/CSV files and loads them into SQL Server.
#
# Usage:
#   python ingest.py                        # uses FILE_PATTERNS from config.py
#   python ingest.py data/sales.xlsx        # load a single file
#   python ingest.py data/q1.csv data/q2.csv
#
# Dependencies (install with pip):
#   pip install pyodbc pandas openpyxl

import sys
import glob
import logging
import pathlib
import re

import pandas as pd

from config import FILE_PATTERNS, TARGET_SCHEMA, TRUNCATE_ON_LOAD, BATCH_SIZE
from db_utils import (
    get_connection,
    ensure_table,
    add_columns_if_missing,
    insert_dataframe,
    sanitize_identifier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File reading
# ---------------------------------------------------------------------------

def read_file(path: pathlib.Path) -> dict[str, pd.DataFrame]:
    """
    Read an Excel or CSV file and return a dict of {table_name: DataFrame}.
    Excel files produce one entry per sheet; CSVs produce one entry named
    after the file stem.
    """
    suffix = path.suffix.lower()
    results: dict[str, pd.DataFrame] = {}

    if suffix in (".xlsx", ".xls", ".xlsm"):
        xl = pd.ExcelFile(path, engine="openpyxl")
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            if df.empty:
                logger.warning("Sheet '%s' in '%s' is empty — skipping.", sheet, path.name)
                continue
            df = clean_dataframe(df)
            table_name = derive_table_name(path.stem, sheet)
            results[table_name] = df
            logger.info("Read %d rows from sheet '%s' in '%s'", len(df), sheet, path.name)

    elif suffix == ".csv":
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            logger.warning("'%s' is empty — skipping.", path.name)
            return results
        df = clean_dataframe(df)
        table_name = derive_table_name(path.stem)
        results[table_name] = df
        logger.info("Read %d rows from '%s'", len(df), path.name)

    else:
        logger.warning("Unsupported file type '%s' — skipping.", path.name)

    return results


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names and strip leading/trailing whitespace from strings."""
    df.columns = [sanitize_identifier(str(c)).strip("_") for c in df.columns]
    # Remove entirely blank rows and columns
    df = df.dropna(how="all").dropna(axis=1, how="all")
    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    return df.reset_index(drop=True)


def derive_table_name(file_stem: str, sheet_name: str | None = None) -> str:
    """Produce a safe T-SQL table name from file + optional sheet name."""
    parts = [re.sub(r"[^\w]", "_", file_stem)]
    if sheet_name and sheet_name.lower() not in ("sheet1", "sheet2", "sheet3", file_stem.lower()):
        parts.append(re.sub(r"[^\w]", "_", sheet_name))
    return "_".join(parts)[:128]  # SQL Server max identifier length


# ---------------------------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------------------------

def discover_files(patterns: list[str]) -> list[pathlib.Path]:
    """Expand glob patterns and return unique, sorted file paths."""
    found: set[pathlib.Path] = set()
    for pattern in patterns:
        for match in glob.glob(pattern, recursive=True):
            found.add(pathlib.Path(match).resolve())
    return sorted(found)


def ingest_file(conn, path: pathlib.Path) -> None:
    """Read one file and insert all its sheets/tables into the database."""
    tables = read_file(path)
    if not tables:
        return

    cursor = conn.cursor()
    for table_name, df in tables.items():
        logger.info("Loading table [%s].[%s] from '%s'…", TARGET_SCHEMA, table_name, path.name)
        ensure_table(cursor, df, TARGET_SCHEMA, table_name, truncate=TRUNCATE_ON_LOAD)
        add_columns_if_missing(cursor, df, TARGET_SCHEMA, table_name)
        conn.commit()  # commit DDL before DML

        n = insert_dataframe(conn, df, TARGET_SCHEMA, table_name, str(path), BATCH_SIZE)
        conn.commit()
        logger.info("  -> %d rows inserted into [%s].[%s]", n, TARGET_SCHEMA, table_name)
    cursor.close()


def main(file_args: list[str]) -> None:
    if file_args:
        files = [pathlib.Path(f).resolve() for f in file_args]
    else:
        files = discover_files(FILE_PATTERNS)

    if not files:
        logger.error("No files found. Check FILE_PATTERNS in config.py or pass file paths as arguments.")
        sys.exit(1)

    logger.info("Files to ingest: %d", len(files))
    for f in files:
        logger.info("  %s", f)

    # A single connection is reused for all files so MFA is prompted only once.
    with get_connection() as conn:
        for path in files:
            try:
                ingest_file(conn, path)
            except Exception as exc:
                logger.error("Failed to ingest '%s': %s", path, exc, exc_info=True)
                # Continue with remaining files rather than aborting entirely.

    logger.info("Ingestion complete.")


if __name__ == "__main__":
    main(sys.argv[1:])
