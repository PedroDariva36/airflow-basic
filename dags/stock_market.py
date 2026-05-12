from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensor.base import PokeReturnValue 
from datetime import datetime


@dag(
    start_date=datetime(2026,1,1),
    schedule='@daily',
    catchup=False, 
    tags=['stock_market'],
)
def stock_market():
    
    @task.sensor(poke_interval = 30, timeout = '600', mode = 'poke' )
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.url}{api.extra_dejson[endpoint]}"
        print(url)
        response = requests.get(url,headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value=url)

    is_api_available()

stock_market()