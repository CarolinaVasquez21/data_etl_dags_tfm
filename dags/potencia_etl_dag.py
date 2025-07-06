from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
import sys

# Se pone en el path para que sean accesibles
sys.path.append('/opt/airflow/data')
sys.path.append('/opt/airflow/utils')
from utilities import construir_columna_fecha, obtener_cliente_db, ajustar_atipicos_por_anio


def extraer_datos(**kwargs):
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_demanda_potencia = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Pot_max_coin", skiprows=2)
    return df_demanda_potencia


def transformar_demandas_maximas(**kwargs):
    ti = kwargs['ti']
    df_demanda_potencia = ti.xcom_pull(task_ids='ETL.extraer_datos', key='return_value')

    melted_demanda_potencia = df_demanda_potencia.melt(id_vars="Meses", var_name="Año", value_name="Potencia (MW)")
    df_potencia_max = melted_demanda_potencia[melted_demanda_potencia["Meses"] == "MAX AÑO"].copy()
    df_potencia_max = df_potencia_max[["Año", "Potencia (MW)"]]
    construir_columna_fecha(df_potencia_max)
    df_potencia_max = df_potencia_max[["Fecha", "Potencia (MW)"]].reset_index(drop=True).sort_values(by="Fecha", ascending=True)
    return df_potencia_max


def transformar_demandas_mensuales(**kwargs):
    ti = kwargs['ti']
    df_demanda_potencia = ti.xcom_pull(task_ids='ETL.extraer_datos', key='return_value')

    melted_demanda_potencia = df_demanda_potencia.melt(id_vars="Meses", var_name="Año", value_name="Potencia (MW)")
    df_potencia = melted_demanda_potencia[melted_demanda_potencia["Meses"] != "MAX AÑO"].copy()
    construir_columna_fecha(df_potencia)
    
    anios_int = [2018, 2020, 2024]
    df_potencia_interpolado = ajustar_atipicos_por_anio(df_potencia, "Potencia (MW)", anios_int)
    
    df_potencia_interpolado = df_potencia_interpolado[["Fecha", "Potencia (MW)"]].reset_index(drop=True).sort_values(by="Fecha", ascending=True)
    return df_potencia_interpolado


def cargar_demandas_maximas(**kwargs):
    ti = kwargs['ti']
    demandas_maximas = ti.xcom_pull(task_ids='ETL.transformar_demandas_maximas', key='return_value')

    db_cliente = obtener_cliente_db()
    datos_insertar = demandas_maximas.to_dict(orient='records')

    db_cliente.potencia.mediciones_max_anuales.delete_many({})
    db_cliente.potencia.mediciones_max_anuales.create_index([("Fecha", 1)])
    db_cliente.potencia.mediciones_max_anuales.insert_many(datos_insertar)


def cargar_demandas_mensuales(**kwargs):
    ti = kwargs['ti']
    demandas_mensuales = ti.xcom_pull(task_ids='ETL.transformar_demandas_mensuales', key='return_value')

    db_cliente = obtener_cliente_db()
    datos_insertar = demandas_mensuales.to_dict(orient='records')

    db_cliente.potencia.mediciones_mensuales.delete_many({})
    db_cliente.potencia.mediciones_mensuales.create_index([("Fecha", 1)])
    db_cliente.potencia.mediciones_mensuales.insert_many(datos_insertar)


with DAG(
    'etl_dag_datos_potencia',
    start_date=datetime(2025, 2, 1),
    schedule_interval=None,
    catchup=False,
    description='DAG de proceso ETL correspondiente a potencia',
) as dag:
    
    inicio_etl = EmptyOperator(
        task_id='Inicio_ETL'
    )

    fin_etl = EmptyOperator(
        task_id='Fin_ETL'
    )

    with TaskGroup("ETL", tooltip="ETL Group") as etl:
        extraer_task = PythonOperator(
            task_id='extraer_datos',
            python_callable=extraer_datos,
            provide_context=True
        )

        transformar_maximas_task = PythonOperator(
            task_id='transformar_demandas_maximas',
            python_callable=transformar_demandas_maximas,
            provide_context=True
        )

        transformar_mensuales_task = PythonOperator(
            task_id='transformar_demandas_mensuales',
            python_callable=transformar_demandas_mensuales,
            provide_context=True
        )

        cargar_maximas_task = PythonOperator(
            task_id='cargar_demandas_maximas',
            python_callable=cargar_demandas_maximas,
            provide_context=True
        )

        cargar_mensuales_task = PythonOperator(
            task_id='cargar_demandas_mensuales',
            python_callable=cargar_demandas_mensuales,
            provide_context=True
        )

        # Dependencias dentro del grupo ETL
        extraer_task >> [transformar_maximas_task, transformar_mensuales_task]
        transformar_maximas_task >> cargar_maximas_task
        transformar_mensuales_task >> cargar_mensuales_task

    # Dependencias externas al grupo ETL
    inicio_etl >> etl >> fin_etl