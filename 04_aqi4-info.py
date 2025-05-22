import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from dags.factory import DagFactory
from dags.postgres import PostgresOp

# ✅ 新的 DAG ID，避免與 03.py 重複
DAG_ID = "get_moenv_aqi_info_v2"

# ✅ 抓取資料邏輯
def get_moenv_aqi_info_op(shard_id: int = 0, total_shard: int = 1):
    url = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
    params = {
        "api_key": "2df2de9e-db13-4ea8-956a-0d25de5200de",
        "format": "json",
        "limit": 1000
    }

    res = requests.get(url, params=params, verify=False)
    data = res.json().get("records", [])

    target_sites = ["基隆", "林口", "嘉義", "安南"]
    records = []
    for i, d in enumerate(data):
        if d.get("sitename") not in target_sites:
            continue
        if int(i % total_shard) != int(shard_id):
            continue
        records.append({
            "device_id": d.get("sitename"),
            "time": d.get("publishtime"),
            "longitude": float(d.get("longitude", 0)),
            "latitude": float(d.get("latitude", 0)),
            "aqi": int(d.get("aqi", -1)) if d.get("aqi") and d.get("aqi").isdigit() else None,
            "pm2_5": float(d.get("pm2.5")) if d.get("pm2.5") and d.get("pm2.5").replace('.', '', 1).isdigit() else None,
            "pm10": float(d.get("pm10")) if d.get("pm10") and d.get("pm10").replace('.', '', 1).isdigit() else None,
            "o3": float(d.get("o3")) if d.get("o3") and d.get("o3").replace('.', '', 1).isdigit() else None,
            "co": float(d.get("co")) if d.get("co") and d.get("co").replace('.', '', 1).isdigit() else None,
            "so2": float(d.get("so2")) if d.get("so2") and d.get("so2").replace('.', '', 1).isdigit() else None,
            "no2": float(d.get("no2")) if d.get("no2") and d.get("no2").replace('.', '', 1).isdigit() else None
        })
    return {"status": "ok", "value": records}

# ✅ 建立 DAG 的函數
def get_aqi_info_dag():
    with DagFactory(
        DAG_ID,
        "airflow",
        start_date=days_ago(1),
        schedule="@hourly",
        catchup=False,
        params={
            "shard_id": Param(0, type="integer"),
            "total_shard": Param(1, type="integer"),
            "connection_id": Param("postgres_default", type="string")
        },
        template_searchpath=["/tmp", os.path.dirname(os.path.realpath(__file__))]
    ).dag() as dag:

        pop = PostgresOp(dag, dag.params["connection_id"])
        shard_id = dag.params["shard_id"]
        total_shard = dag.params["total_shard"]

        # 建立資料表
        createtabletask = pop.create_table("sql/aqi4_info_schema.sql")

        # 抓資料
        gettask = PythonOperator(
            task_id="get_moenv_aqi_info",
            python_callable=get_moenv_aqi_info_op,
            op_kwargs={"shard_id": shard_id, "total_shard": total_shard},
            do_xcom_push=True,
            dag=dag
        )

        # 產生 SQL 檔案
        tmpfile = f"/tmp/aqi_info_bulk_stmt_{shard_id}_{total_shard}.sql"
        generatesql = pop.generate_bulk_insert_stmt(
            table_name="aqi4_info",
            from_task_id="get_moenv_aqi_info",
            out_sql_file=tmpfile,
            update_on_conflict=False,
            on_conflict_key="device_id"
        )

        # 執行 SQL 寫入
        insertdata = pop.bulk_insert_from_stmt(tmpfile)

        # DAG 流程串接
        createtabletask >> gettask >> generatesql >> insertdata

    return dag

# ✅ 註冊 DAG 給 Airflow 載入
globals()[DAG_ID] = get_aqi_info_dag()
