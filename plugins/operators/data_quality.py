from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", tests=None, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests or []
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test in self.tests:
            sql_query, expected_result = test
            records = redshift_hook.get_records(sql_query)
            actual_result = records[0][0]

            self.log.info(f"Running datat quality check: {sql_query}")
            if actual_result != expected_result:
                raise ValueError(f"Data quality check failed. Expected {expected_result} but got {actual_result}")
            self.log.info(f"Data quality check passed")
