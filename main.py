import logging
import sys
from os import getenv
from typing import List

import dlt
from dlt.common import pendulum
from dlt.common.schema.typing import (TColumnNames, TTableFormat,
                                      TWriteDispositionConfig)
from dlt.destinations import filesystem
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import TableBackend, sql_table
from loguru import logger


class InterceptHandler(logging.Handler):
    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())


class TransferTable:
    def __init__(
        self,
        conn_string_credentials: ConnectionStringCredentials,
        table: str,
        schema: str,
        primary_key: TColumnNames,
        incremental: str,
        table_format: TTableFormat = "delta",
        layout: str = "{table_name}/{load_id}.{file_id}.{ext}",
        included_columns: List[str] = [],
        excluded_columns: List[str] = [],
        chunk_size=1000,
        backend: TableBackend = "pyarrow",
    ):
        """
        TransferTable class to transfer data from a database table to a file storage.

        Args:
            db_conn_string (str): Database connection string.
            table (str): Table name.
            schema (str): Schema name.
            primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
            layout (str): A layout of the files holding table data in the destination bucket/filesystem. Uses a set of pre-defined
                and user-defined (extra) placeholders. Please refer to https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#files-layout
            incremental (str): Incremental column name.
            table_format (str): Table format. Default is 'delta'.
            included_columns (List[str]): List of columns to include. Default is [].
            excluded_columns (List[str]): List of columns to exclude. Default is [].
            chunk_size (int): Chunk size. Default is 1000.
            backend (str): Backend to use. Default is 'pyarrow'.
        """
        self.conn_string_credentials: ConnectionStringCredentials = (
            conn_string_credentials
        )
        self.table = table
        self.schema = schema
        self.table_format: TTableFormat = table_format
        self.layout = layout
        self.chunk_size = chunk_size
        self.backend: TableBackend = backend
        self.included_columns = included_columns
        self.primary_key: TColumnNames = primary_key
        self.incremental = incremental

    def _init_pipeline(self):
        # Create a pipeline
        dataset_name = "{schema}-{table}".format(schema=self.schema, table=self.table)
        pipeline = dlt.pipeline(
            pipeline_name=dataset_name,
            dataset_name=dataset_name,
            destination=filesystem(
                layout=self.layout,
                current_datetime=pendulum.now(),
            ),
        )
        return pipeline

    def _init_table(self):
        return sql_table(
            self.conn_string_credentials,
            self.table,
            self.schema,
            chunk_size=self.chunk_size,
            backend=self.backend,
            included_columns=self.included_columns,
        ).apply_hints(incremental=dlt.sources.incremental(self.incremental))

    def run(
        self,
        write_disposition: TWriteDispositionConfig = "merge",
    ):
        # Create a pipeline
        pipeline = self._init_pipeline()
        tb = self._init_table()

        info = pipeline.run(
            [tb],
            write_disposition=write_disposition,
            primary_key=self.primary_key,
            table_format=self.table_format,
        )
        return info


if __name__ == "__main__":
    db_conn_string = getenv(
        "SOURCE_DB_CONN_STRING", "mysql+pymysql://root:dev@localhost:3307/demo"
    )
    schema = dlt.config["transfer_table.schema"]
    table = dlt.config["transfer_table.table"]
    primary_key = dlt.config["transfer_table.primary_key"]
    incremental = dlt.config["transfer_table.incremental"]
    included_columns = dlt.config["transfer_table.included_columns"]
    try:
        client_ds = TransferTable(
            conn_string_credentials=ConnectionStringCredentials(db_conn_string),
            table=table,
            schema=schema,
            primary_key=primary_key,
            incremental=incremental,
            table_format="delta",
            included_columns=included_columns,
        )
        print(client_ds.run())
    except Exception as e:
        logger.exception(e)
