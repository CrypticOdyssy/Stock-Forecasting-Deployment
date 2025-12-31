-- enable TimescaleDB extension (needed before create_hypertable)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    series_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,  -- 'running', 'completed', 'failed'
    stage VARCHAR(50),  -- 'ingestion', 'preprocessing', 'forecasting', 'anomaly'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    metadata JSONB
);

CREATE INDEX idx_pipeline_jobs_series ON pipeline_jobs(series_id);
CREATE INDEX idx_pipeline_jobs_status ON pipeline_jobs(status);
CREATE INDEX idx_pipeline_jobs_created ON pipeline_jobs(created_at DESC);

-- then create tables
CREATE TABLE IF NOT EXISTS time_series_raw (
    series_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    features JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY(series_id, timestamp)
);
 
CREATE TABLE IF NOT EXISTS time_series_preprocessed (
    series_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    features JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY(series_id, timestamp)
);
 
-- now create hypertables
SELECT create_hypertable('time_series_raw', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('time_series_preprocessed', 'timestamp', if_not_exists => TRUE);