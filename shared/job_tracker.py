"""
Lightweight job tracker for pipeline status updates.
No containers needed - just import and use!
"""
import logging
import psycopg2
from psycopg2 import pool
import json
from typing import Optional, Dict, Any
import os


logger = logging.getLogger(__name__)


class SimpleJobTracker:
    """Standalone job tracker - works without dependency injection"""
    
    _pool = None
    
    @classmethod
    def get_pool(cls):
        """Get or create database connection pool"""
        if cls._pool is None:
            cls._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                host=os.getenv("DATABASE_HOST", "timescaledb"),
                port=int(os.getenv("DATABASE_PORT", "5432")),
                database=os.getenv("DATABASE_NAME", "timeseries"),
                user=os.getenv("DATABASE_USER", "tsuser"),
                password=os.getenv("DATABASE_PASSWORD", "ts_password"),
                connect_timeout=10
            )
            logger.info("Job tracker database pool initialized")
        return cls._pool
    
    @classmethod
    def update_status(
        cls,
        job_id: str,
        series_id: str,
        status: str,  # 'running', 'completed', 'failed'
        stage: str,   # 'ingestion', 'preprocessing', 'forecasting', 'anomaly'
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Update pipeline job status in database.
        
        Usage:
            SimpleJobTracker.update_status(
                job_id='job123',
                series_id='AAPL',
                status='running',
                stage='ingestion'
            )
        """
        conn = None
        try:
            pool = cls.get_pool()
            conn = pool.getconn()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO pipeline_jobs 
                    (job_id, series_id, status, stage, error_message, metadata, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id) 
                DO UPDATE SET
                    status = EXCLUDED.status,
                    stage = EXCLUDED.stage,
                    error_message = EXCLUDED.error_message,
                    metadata = COALESCE(
                        pipeline_jobs.metadata || EXCLUDED.metadata,
                        pipeline_jobs.metadata,
                        EXCLUDED.metadata
                    ),
                    updated_at = NOW()
            """, (
                job_id,
                series_id,
                status,
                stage,
                error_message,
                json.dumps(metadata) if metadata else None
            ))
            
            conn.commit()
            cursor.close()
            logger.info(f"✅ Job {job_id}: {status} at {stage}")
            
        except Exception as e:
            logger.error(f"❌ Failed to update job status: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                pool.putconn(conn)
