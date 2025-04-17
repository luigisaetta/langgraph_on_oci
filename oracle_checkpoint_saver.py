"""
oracle_checkpoint_saver.py

This module provides the OracleCheckpointSaver class, which implements
LangGraph's BaseCheckpointSaver interface to persist workflow state
in an Oracle Database using the oracledb driver.
"""

import json
import uuid
from typing import Any, Dict, Iterator, Optional, AsyncIterator, Sequence, Tuple
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    RunnableConfig,
    ChannelVersions,
)
import oracledb
from utils import get_console_logger

# for connection to DB
from config_private import CONNECT_ARGS

logger = get_console_logger()

# the name of the Oracle DB table
TABLE_NAME = "OCI_LANGGRAPH_CHECKPOINTS"


class OracleCheckpointSaver(BaseCheckpointSaver):
    """
    A checkpoint saver that persists LangGraph workflow state to an
    Oracle Database.

    This class manages the insertion, updating, retrieval, and listing
    of checkpoints identified by thread_id and checkpoint_id.

    Attributes:
        connection (oracledb.Connection): Active connection to the Oracle Database.
        cursor (oracledb.Cursor): Cursor object for executing SQL statements.
    """

    def __init__(self):
        """
        Initializes the OracleCheckpointSaver
        """
        super().__init__()

    def get_oracle_connection(self):
        """
        Get the connection to the DB
        """
        try:
            conn = oracledb.connect(
                **CONNECT_ARGS,
            )

            return conn
        except oracledb.DatabaseError as e:
            logger.error("Error connecting to Oracle DB: %s", e)
            # maybe we should re-raise here
            return None

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Inserts a new checkpoint or updates an existing one in the database.
        """
        logger.info("OracleCheckpointSaver, called put...")

        thread_id = config["configurable"]["thread_id"]
        # normally we don't set it in the config
        checkpoint_id = config["configurable"].get("checkpoint_id")

        # If checkpoint_id is missing, generate a new UUID
        if not checkpoint_id:
            checkpoint_id = str(uuid.uuid4())
            config.setdefault("configurable", {})["checkpoint_id"] = checkpoint_id
            logger.info("Generated new checkpoint_id: %s", checkpoint_id)

        state_json = json.dumps(checkpoint)
        metadata_json = json.dumps(metadata)

        update_sql = f"""
            UPDATE {TABLE_NAME}
            SET state = :state, metadata = :metadata
            WHERE thread_id = :thread_id AND checkpoint_id = :checkpoint_id
        """

        with self.get_oracle_connection() as conn:
            with conn.cursor() as cursor:
                cursor.setinputsizes(
                    state=oracledb.DB_TYPE_JSON, metadata=oracledb.DB_TYPE_JSON
                )
                cursor.execute(
                    update_sql,
                    {
                        "state": state_json,
                        "metadata": metadata_json,
                        "thread_id": thread_id,
                        "checkpoint_id": checkpoint_id,
                    },
                )

                # If no rows were updated, it is the first checkpoint for this thread, insert a new record
                if cursor.rowcount == 0:
                    insert_sql = f"""
                        INSERT INTO {TABLE_NAME} (thread_id, checkpoint_id, state, metadata)
                        VALUES (:thread_id, :checkpoint_id, :state, :metadata)
                    """
                    cursor.execute(
                        insert_sql,
                        {
                            "thread_id": thread_id,
                            "checkpoint_id": checkpoint_id,
                            "state": state_json,
                            "metadata": metadata_json,
                        },
                    )

            conn.commit()

        return config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Stores intermediate writes linked to a checkpoint.

        Args:
            config (RunnableConfig): Configuration specifying thread_id and checkpoint_id.
            writes (Sequence[Tuple[str, Any]]): List of writes to store.
            task_id (str): Identifier for the task creating the writes.
            task_path (str, optional): Path of the task creating the writes.
        """
        # TODO Implement logic to store intermediate writes if necessary
        logger.info("OracleCheckpointSaver, called put_writes...")

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """
        Retrieves a checkpoint tuple (state and metadata) from the database.
        Requires a valid thread_id and checkpoint_id in the configuration.
        """
        logger.info("OracleCheckpointSaver: called get_tuple...")

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = config["configurable"].get("checkpoint_id")

        # If checkpoint_id is missing, we can't retrieve the checkpoint
        if not checkpoint_id:
            logger.warning("Missing checkpoint_id in get_tuple(); returning None.")
            return None

        select_sql = f"""
            SELECT state, metadata
            FROM {TABLE_NAME}
            WHERE thread_id = :thread_id AND checkpoint_id = :checkpoint_id
        """

        with self.get_oracle_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    select_sql, {"thread_id": thread_id, "checkpoint_id": checkpoint_id}
                )
                row = cursor.fetchone()
                if row:
                    state = json.loads(row[0])
                    metadata = json.loads(row[1])
                    return CheckpointTuple(checkpoint=state, metadata=metadata)

        return None

    def list(
        self,
        config: Optional[RunnableConfig] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """
        Lists checkpoints that match the given criteria.

        Args:
            config (Optional[RunnableConfig], optional):
                Base configuration for filtering checkpoints.
            filter (Optional[Dict[str, Any]], optional):
                Additional filtering criteria.
            before (Optional[RunnableConfig], optional):
                List checkpoints created before this configuration.
            limit (Optional[int], optional): Maximum number of checkpoints to return.

        Returns:
            Iterator[CheckpointTuple]: Iterator of matching checkpoint tuples.
        """
        thread_id = config["configurable"]["thread_id"] if config else None
        select_sql = f"""
            SELECT state, metadata
            FROM {TABLE_NAME}
            WHERE thread_id = :thread_id
        """
        with self.get_oracle_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_sql, {"thread_id": thread_id})
                for row in cursor.fetchall():
                    state = json.loads(row[0])
                    metadata = json.loads(row[1])
                    yield CheckpointTuple(checkpoint=state, metadata=metadata)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Asynchronously inserts a new checkpoint or updates an existing one in the database.

        Args:
            config (RunnableConfig): Configuration specifying thread_id and checkpoint_id.
            checkpoint (Checkpoint): The checkpoint data to store.
            metadata (CheckpointMetadata): Additional metadata for the checkpoint.
            new_versions (ChannelVersions): New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.
        """
        # Implement asynchronous version if needed
        return self.put(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Asynchronously stores intermediate writes linked to a checkpoint.

        Args:
            config (RunnableConfig): Configuration specifying thread_id and checkpoint_id.
            writes (Sequence[Tuple[str, Any]]): List of writes to store.
            task_id (str): Identifier for the task creating the writes.
            task_path (str, optional): Path of the task creating the writes.
        """
        # Implement asynchronous version if needed
        self.put_writes(config, writes, task_id, task_path)

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """
        Asynchronously retrieves a checkpoint tuple from the database.

        Args:
            config (RunnableConfig): Configuration specifying thread_id and checkpoint_id.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if not found.
        """
        # Implement asynchronous version if needed
        return self.get_tuple(config)

    async def alist(
        self,
        config: Optional[RunnableConfig] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """
        Asynchronously lists checkpoints that match the given criteria.

        Args:
            config (Optional[RunnableConfig], optional): Base configuration
            for filtering checkpoints.
            filter (Optional[Dict[str, Any]], optional): Additional filtering criteria.
            before (Optional[RunnableConfig], optional): List checkpoints
            created before this configuration.
            limit (Optional[int], optional): Maximum number of checkpoints to return.

        Yields:
            AsyncIterator[CheckpointTuple]: Asynchronous iterator of matching checkpoint tuples.
        """
        # Implement asynchronous version if needed
        for checkpoint in self.list(config, filter=filter, before=before, limit=limit):
            yield checkpoint
