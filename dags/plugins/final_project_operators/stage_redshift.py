from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        SESSION_TOKEN '{}'
        FORMAT AS json '{}';
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 session_token="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.conn_id = conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.session_token = session_token

    def execute(self, context):
        self.log.info('StageToRedshiftOperator implementation')
        metaStore = MetastoreBackend()
        aws_conn = metaStore.get_connection(self.aws_conn_id)
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"aws_conn {aws_conn}")
        self.log.info("dropping table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("copying data")
        key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key)

        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            new_sql = StageToRedshiftOperator.copy_query.format(
                self.table,
                s3_path,
                aws_conn.login,
                aws_conn.password,
                self.session_token,
                self.log_json_file
            )
        else:
            new_sql = StageToRedshiftOperator.copy_query.format(
                self.table,
                s3_path,
                aws_conn.login,
                aws_conn.password,
                self.session_token
                'auto'
            )
        redshift.run(new_sql)
        self.log.info(f"Copied to redshift table: {self.table}")





