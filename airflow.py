from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the default arguments
default_args = {
    'owner': 'dummy_owner',
    'start_date': datetime.today(),
    'email': 'dummy@email.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'ETL_toll_data',
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),  # Daily once
    default_args=default_args,
)

# Define ETL tasks
# Task 1.3: Unzip Data

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C \
    /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)


# Task 1.4: Extract Data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv \
    > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

# Task 1.5: Extract Data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5,6,7 ./tollplaza-data.tsv | sed 's/\t/,/g' > ./tsv_data.csv",
    dag=dag,
)

# Task 1.6: Extract Data from Fixed-width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""awk '{print substr($0, length($0)-8, 3) "," substr($0, length($0)-4, 5)}' ./payment-data.txt \
    > ./fixed_width_data.csv""",
    dag=dag,
)
# Task 1.7: Consolidate Data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d','  ./csv_data.csv ./tsv_data.csv ./fixed_width_data.csv > extracted_data.csv",
    dag=dag,
)

# Task 1.8: Transform Data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cat ./extracted_data.csv | tr "[:lower:]" "[:upper:]" > ./transformed_data.csv',
    dag=dag,
)

# Task 1.9
# Set up task dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
