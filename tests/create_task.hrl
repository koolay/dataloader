POST http://localhost:8000/upsert-tasks
Content-Type: application/json

{
    "schema": "demo",
    "table": "clients",
    "primary_key": "id",
    "incremental": "id",
    "included_columns": ["id", "name", "code", "created_by", "updated_at"],
    "cron_schedule": "* * * * *"
}

