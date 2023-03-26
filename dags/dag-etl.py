from datetime import datetime, timedelta
import os
from airflow.operators.dummy_operator import DummyOperator
from sparkify.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from sparkify.helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
)
def sparkify_etl():
    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songplay",
        s3_bucket="sparkify-final-ud",
        s3_key="log-data",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="sparkify-final-ud",
        s3_key="song-data",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
        append_mode=True,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="user",
        sql=SqlQueries.user_table_insert,
        append_mode=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="song",
        sql=SqlQueries.user_table_insert,
        append_mode=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artist",
        sql=SqlQueries.artist_table_insert,
        append_mode=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
        append_mode=False,
    )

    null_check_query = "SELECT COUNT(*) FROM {} WHERE {} IS NULL"
    data_quality_tests = [
        (null_check_query.format(table, column), 0)
        for table, column in [
            ("song", "song_id"),
            ("songplay", "songplay_id"),
            ("user", "userid"),
            ("time", "start_time"),
            ("artist", "artist_id"),
        ]
    ]

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks", redshift_conn_id="redshift", tests=data_quality_tests
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    (
        start_operator
        >> [stage_events_to_redshift, stage_songs_to_redshift]
        >> load_songplays_table
        >> [
            load_song_dimension_table,
            load_user_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> run_quality_checks
        >> end_operator
    )
