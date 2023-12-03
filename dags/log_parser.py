import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
from datetime import datetime, timedelta
import httpagentparser
from clickhouse_driver import Client
import re

dag = DAG(
    "log-parser",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="parsing data from laccess.log and insert into db",
    schedule=timedelta(minutes=10),
    start_date=datetime(2023, 10, 25),
    catchup=False,
    tags=["Final_project"],
)


def query_df(query):
    "Execute query and save it to pandas.DataFrame"
    result, columns = client.execute(query, with_column_types=True)
    return pd.DataFrame(result, columns=[v[0] for v in columns])


def get_data():
    result_df = pd.DataFrame()
    d_dict = {}
    d_dict['IP'] = []
    d_dict['timestamp'] = []
    d_dict['response'] = []
    d_dict['numbers'] = []
    d_dict['device_name'] = []
    d_dict['browser'] = []
    regex = '\"(.*?)\"|\[(.*?)\]|(\S+)'
    with open('access.log', 'r') as file:
        for line in file:
            #         print(line)
            parsed_line = re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)
            d_dict['IP'].append(parsed_line[0][2])
            d_dict['timestamp'].append(parsed_line[3][1])
            d_dict['response'].append(parsed_line[5][2])
            d_dict['numbers'].append(parsed_line[6][2])
            try:
                d_dict['device_name'].append(parsed_line[-2][0].split("(")[1].split(")")[0].split(";")[-1])
            except(BaseException):
                d_dict['device_name'].append(None)
            try:
                d_dict['browser'].append(httpagentparser.detect(line)['browser']['name'])
            except(BaseException):
                d_dict['browser'].append(None)
    data = pd.DataFrame.from_dict(d_dict)
    data.to_csv('parsed_logs.csv')


def insert_data():
    client = Client('localhost', settings={'use_numpy': True})
    pd.read_csv('parsed_logs.csv', index_col=0)
    client.insert_dataframe("""insert into test_db.log_info values""", data)



t1 = PythonOperator(
    task_id="get_data_from_access.log",
    provide_context=True,
    python_callable=get_data(),
    dag=dag,
)
t1 = PythonOperator(
    task_id="insert_log_data_to_db",
    provide_context=True,
    python_callable=insert_data(),
    dag=dag,
)

t1 >> t2