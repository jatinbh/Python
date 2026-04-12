# config.py
# Database and ingestion configuration

DB_CONFIG = {
    "server": "your-server.database.windows.net",   # Azure SQL / SQL Server hostname
    "database": "your-database-name",
    "driver": "ODBC Driver 18 for SQL Server",
    "authentication": "ActiveDirectoryInteractive",  # Triggers MFA prompt
    # For service principal (non-interactive MFA), use:
    # "authentication": "ActiveDirectoryServicePrincipal",
    # "username": "your-app-id@tenant-id",
    # "password": "your-client-secret",
}

# Schema to load tables into (change to "dbo" or your target schema)
TARGET_SCHEMA = "dbo"

# If True, drops and recreates the table on each run (full reload).
# If False, appends new rows only.
TRUNCATE_ON_LOAD = False

# Batch size for bulk inserts (rows per executemany call)
BATCH_SIZE = 500

# File discovery — glob patterns relative to the script's working directory
FILE_PATTERNS = [
    "data/**/*.xlsx",
    "data/**/*.xls",
    "data/**/*.csv",
]
