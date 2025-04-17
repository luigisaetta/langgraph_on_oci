CREATE TABLE OCI_LANGGRAPH_CHECKPOINTS (
    thread_id     VARCHAR2(64) NOT NULL,
    checkpoint_id VARCHAR2(64) NOT NULL,
    state         JSON,
    metadata      JSON,
    PRIMARY KEY (thread_id, checkpoint_id)
);