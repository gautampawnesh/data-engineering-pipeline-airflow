from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


s3_bucket = 'final-bucket-v1'
events_s3_key = "log-data"
songs_s3_key = "song-data/A/A/"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'gireesh kumar',
    'start_date': pendulum.now(),
    'email_on_retry': False,
    'catchup': False,
    'depends_on_past': False,
    'retries': 3
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        conn_id="redshift",
        aws_conn_id="aws_credentials",
        session_token="IQoJb3JpZ2luX2VjEG0aCXVzLXdlc3QtMiJHMEUCIQCkF7J23z4uzH0XbSN01R2fTFPxjjPqK8mbDUROg+/IQAIgCWm8CP4jU9P6nO0rRx4bpeqEl8fNCGxdi/46p5QDnQgqxwIIpv//////////ARAAGgw1NjI2Njc2NjE4NTciDPwNInzOCf3mPsBOCCqbAtBazVBJyeDsY0nE4s0SDGzdcWOzlUh/AsjlVzjk0bm5SJGzhjQMjF6O5vJsWIJuQ7HqiPdb9doz4KMzzZEZ8cA6DfITTkG1YOcvtPHbvdfZrLtcRnDFq75WwwUkqH7yQvG0z8NN1/W2YjS6hBeyIzHJsPhtL8IoZxKKIQGFp0s7BaDBZcKEHzy3TStZcoH7tEmQWgVDNZnZ7dqzNr+NoHziCZVOAMR89zh4+OXOHSN7pbByqWkBzS/qD0DSHRCbPxyTDFYqN096cvcg0sQbsCB7sFCd5NeaQGDMNWLslsnRk4Dr24OhLQ9DP7tNQg/tzyRmEm/bTQN0Zk0RLE60suBeVb3ac8Cyjcp/ujZt1f4zV1DsZEHB9p4MaoMw3sn0sAY6nQF9CW9ccdmZGGog22ja62ESDs/27g1BrT6GWAnDPHqYI89x2niStaVVCuY1K7EAhB5kX5lCmbogHfT46gDH3l92ibBW8Wy4AYK3Jlm87lQMx15R1RUVj2J7tpdUo7AudYpXoMGDkd2hYuctgf0sqgxwbgN8TGUUa0ajGZYOZ+J+WxAK2EYe/VWtC8V6ELMw5t+J1/JFuzJPiqY9Q3Nn",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key,
        log_json_file=log_json_file
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        session_token="IQoJb3JpZ2luX2VjEG0aCXVzLXdlc3QtMiJHMEUCIQCkF7J23z4uzH0XbSN01R2fTFPxjjPqK8mbDUROg+/IQAIgCWm8CP4jU9P6nO0rRx4bpeqEl8fNCGxdi/46p5QDnQgqxwIIpv//////////ARAAGgw1NjI2Njc2NjE4NTciDPwNInzOCf3mPsBOCCqbAtBazVBJyeDsY0nE4s0SDGzdcWOzlUh/AsjlVzjk0bm5SJGzhjQMjF6O5vJsWIJuQ7HqiPdb9doz4KMzzZEZ8cA6DfITTkG1YOcvtPHbvdfZrLtcRnDFq75WwwUkqH7yQvG0z8NN1/W2YjS6hBeyIzHJsPhtL8IoZxKKIQGFp0s7BaDBZcKEHzy3TStZcoH7tEmQWgVDNZnZ7dqzNr+NoHziCZVOAMR89zh4+OXOHSN7pbByqWkBzS/qD0DSHRCbPxyTDFYqN096cvcg0sQbsCB7sFCd5NeaQGDMNWLslsnRk4Dr24OhLQ9DP7tNQg/tzyRmEm/bTQN0Zk0RLE60suBeVb3ac8Cyjcp/ujZt1f4zV1DsZEHB9p4MaoMw3sn0sAY6nQF9CW9ccdmZGGog22ja62ESDs/27g1BrT6GWAnDPHqYI89x2niStaVVCuY1K7EAhB5kX5lCmbogHfT46gDH3l92ibBW8Wy4AYK3Jlm87lQMx15R1RUVj2J7tpdUo7AudYpXoMGDkd2hYuctgf0sqgxwbgN8TGUUa0ajGZYOZ+J+WxAK2EYe/VWtC8V6ELMw5t+J1/JFuzJPiqY9Q3Nn",
        conn_id='redshift',
        aws_conn_id='aws_credentials',
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id='redshift',
        table='users',
        query=SqlQueries.user_table_insert,
        mode='truncate-insert'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id='redshift',
        table='songs',
        query=SqlQueries.song_table_insert,
        mode='truncate-insert'
    )


    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id='redshift',
        table='artists',
        query=SqlQueries.artist_table_insert,
        mode='truncate-insert'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id='redshift',
        table='time',
        query=SqlQueries.time_table_insert,
        mode='truncate-insert'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )
    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >>[stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
        run_quality_checks >> end_operator
    #start_operator >> end_operator

final_project_dag = final_project()
