from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", append_mode=False, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_mode = append_mode
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.append_mode is False:
            self.log.info(f"Drop old {self.table} table")
            redshift_hook.run(f"DROP FROM {self.table};")
        self.log.info("Loading dim table: {self.table}")

        redshift_hook.run(f"INSERT INTO {self.table} \n{self.sql}")
