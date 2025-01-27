# DataLoader

DataLoader is a program that loads data from a database and stores it into object storage.

## Installation

**Insert uv**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Install dependencies**

```bash
uv sync
```

## Usage

.dlt/config.toml

```toml

[runtime]
log_level="WARNING"  # the system log level of dlt
# use the dlthub_telemetry setting to enable/disable anonymous usage data reporting, see https://dlthub.com/docs/reference/telemetry
dlthub_telemetry = true

[transfer_table]
schema = "demo"
table="clients"
primary_key="id"
incremental="id"
table_format="delta"
included_columns=["id", "name", "code", "created_by", "updated_at"]
```

```bash

# Run DataLoader
uv run main.py

# Display info via streamlit
uv run dlt pipeline demo-clients show

```
## Query the data using duckdb

**Install duckdb**

Run `duckdb` in a terminal

```bash
duckdb
```
**Query the data**

```sql

SELECT *
    FROM delta_scan('file:///data/bucket_url/clients');

```
