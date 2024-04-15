from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id="",
                 query="",
                 table="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.query = query
        self.table = table
        self.mode = mode

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("staging to dimension")
        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Inserting data into dimension table")
        redshift.run("INSERT INTO {} {}".format(self.table, self.query))