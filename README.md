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
.dlt/secrets.toml

```toml
[sources.sql_table.credentials]
drivername = "pymysql" # please set me up!
database = "ddd" # please set me up!
password = "pdd" # please set me up!
username = "root" # please set me up!
host = "host" # please set me up!
port = 0 # please set me up!

[destination.filesystem]
# bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,
bucket_url = "file:///data/transfer_table"  # three / for an absolute path

# Parquet-like layout (note: it is not compatible with the internal datetime of the parquet file)
# layout = "{table_name}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"


[destination.filesystem.credentials]
aws_access_key_id = "aws_access_key_id" # please set me up!
aws_secret_access_key = "aws_secret_access_key" # please set me up!
endpoint_url = "https://<account_id>.r2.cloudflarestorage.com" # copy your endpoint URL here

[destination.filesystem.kwargs]
use_ssl=true
auto_mkdir=true
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
    FROM delta_scan('file:///data/transfer_table/clients');

```
