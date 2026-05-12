from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue 
from airflow.operators.base import PythonOperator
import requests
from datetime import datetime

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2026,1,1),
    schedule='@daily',
    catchup=False, 
    tags=['stock_market'],
)
def stock_market():
    
    @task.sensor(poke_interval = 30, timeout = 600, mode = 'poke' )
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url,headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value=url)

    is_api_available()

    get_stock_price = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable='_get_stock_prices',
        op_kwargs = {
            'url': '{{ti.xcom_pull("is_api_available")}}',
            'symbol':SYMBOL
        }  
    ) 


stock_market()