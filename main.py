import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from os import getenv
from typing import List

import dlt
import uvicorn
from dlt.common import pendulum
from dlt.common.schema.typing import (TColumnNames, TTableFormat,
                                      TWriteDispositionConfig)
from dlt.destinations import filesystem
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import TableBackend, sql_table
from fastapi import Body, FastAPI, Query
from loguru import logger
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.worker import Worker

interrupt_event = asyncio.Event()
weblog = logging.getLogger("web")
dlt_log = logging.getLogger("dlt")


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


dlt_log.addHandler(InterceptHandler())
weblog.addHandler(InterceptHandler())


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


@dataclass
class TableTransferInput:
    # dbname
    schema: str
    table: str
    primary_key: str
    # incremental column
    incremental: str
    included_columns: List[str]
    cron_schedule: str


@dataclass
class WorkerOptions:
    max_concurrent_workflow_tasks: int
    queue_name: str


@activity.defn
async def runPipeline(input: TableTransferInput):
    db_conn_string = getenv(
        "SOURCE_DB_CONN_STRING", "mysql+pymysql://root:dev@localhost:3307/demo"
    )
    client_ds = TransferTable(
        conn_string_credentials=ConnectionStringCredentials(db_conn_string),
        table=input.table,
        schema=input.schema,
        primary_key=input.primary_key,
        incremental=input.incremental,
        table_format="delta",
        included_columns=input.included_columns,
    )
    await asyncio.to_thread(client_ds.run)


# skip sandbox for: fix ImportError: PyO3 modules may only be initialized once per interpreter process
@workflow.defn(sandboxed=False)
class TableTransferWorkflow:
    @workflow.run
    async def run(self, input: TableTransferInput) -> None:
        active_timeout = getenv("TEMPORAL_ACTIVE_TABLETRANSFER_TIMEOUT", "120")
        result = await workflow.execute_activity(
            runPipeline,
            input,
            start_to_close_timeout=timedelta(seconds=int(active_timeout)),
        )
        return result


async def run_webserver(
    tclient: Client, wk_options: WorkerOptions, logger: logging.Logger
):
    logger.info("Initializing Webserver...")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Setup context manager for HTTP server to have Activity callback method in its context"""
        app.state.logger = logger
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def health():
        return "ok"

    @app.post("/upsert-tasks")
    async def upsert_task(payload: TableTransferInput = Body(...)):
        await tclient.start_workflow(
            TableTransferWorkflow.run,
            payload,
            id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
            id="table-transfer-{schema}-{table}".format(
                schema=payload.schema, table=payload.table
            ).lower(),
            task_queue=wk_options.queue_name,
            cron_schedule=payload.cron_schedule,
        )

        return {"errcode": 0, "errmsg": "ok"}

    http_port = getenv("HTTP_PORT", "8000")
    config = uvicorn.Config(app, "0.0.0.0", int(http_port))
    server = uvicorn.Server(config)
    await server.serve()


async def run_worker(tclient: Client, wk_options: WorkerOptions):
    queue_name = wk_options.queue_name
    max_concurrent_workflow_tasks = wk_options.max_concurrent_workflow_tasks
    # Run a worker for the workflow
    wk = Worker(
        tclient,
        task_queue=queue_name,
        workflows=[TableTransferWorkflow],
        activities=[runPipeline],
        max_concurrent_workflow_tasks=int(max_concurrent_workflow_tasks),
    )

    await wk.run()


def on_signal_received():
    print("\nInterrupt received, shutting down...\n")
    interrupt_event.set()


async def main(loop: asyncio.AbstractEventLoop):
    # Setup signal handler for graceful shutdown on SIGINT
    loop.add_signal_handler(signal.SIGINT, on_signal_received)

    temporal_hostport = getenv("TEMPORAL_HOSTPORT", "localhost:7233")
    temporal_namespace = getenv("TEMPORAL_NAMESPACE", "default")
    wk_options = WorkerOptions(
        max_concurrent_workflow_tasks=int(
            getenv("TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS", "4")
        ),
        queue_name=getenv("TEMPORAL_QUEUE_NAME", "table-transfer"),
    )
    tclient = await Client.connect(temporal_hostport, namespace=temporal_namespace)

    logger.info("Starting worker...")
    loop.create_task(run_worker(tclient, wk_options))
    logger.info("Starting webserver...")
    loop.create_task(run_webserver(tclient, wk_options, logger=weblog))
    try:
        # Wait indefinitely until the interrupt event is set
        await interrupt_event.wait()
    finally:
        # The worker will be shutdown gracefully due to the async context manager
        logger.info("Shutting down the worker")
        await loop.shutdown_asyncgens()  # Shutdown asynchronous generators


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        print("\nInterrupt received, shutting down...\n")
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
