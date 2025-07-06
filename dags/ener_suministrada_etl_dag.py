from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import sys

# Se pone en el path para que sean accesibles
sys.path.append('/opt/airflow/data')
sys.path.append('/opt/airflow/utils')
from utilities import construir_columna_fecha, obtener_cliente_db


def extraer_datos(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos de potencia
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_ener_suministrada = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Energia_Suministro", skiprows=3)

    # Cargar los valores con xcom
    return df_ener_suministrada


def transformar_ener_suministrada_total(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_ener_suministrada = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Obtención de la energía suminsitrada por cada año
    df_melted_ener_suministrada = df_ener_suministrada.melt(id_vars=['Meses'], var_name='Mes', value_name='Valor')
    df_melted_ener_suministrada.columns = ["Mes", "Año", "Energía suministrada (MWh)"]

    # Obtener solo la energía suministrada total por año y construir el dataframe final
    df_tot_valores_ener_suministrada = df_melted_ener_suministrada[df_melted_ener_suministrada["Mes"]=="TOTAL"].copy()
    df_tot_valores_ener_suministrada = df_tot_valores_ener_suministrada[["Año", "Energía suministrada (MWh)"]].reset_index(drop=True)
    construir_columna_fecha(df_tot_valores_ener_suministrada)

    # Obtener solo las columnas de interés
    df_tot_valores_ener_suministrada = df_tot_valores_ener_suministrada[["Fecha", "Energía suministrada (MWh)"]]

    return df_tot_valores_ener_suministrada



def transformar_ener_suministrada_mensual(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_ener_suministrada = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Obtención de la energía suministrada mensualmente
    df_melted_ener_suministrada = df_ener_suministrada.melt(id_vars=['Meses'], var_name='Mes', value_name='Valor')
    df_melted_ener_suministrada.columns = ["Mes", "Año", "Energía suministrada (MWh)"]

    # Obtener solo la energía suministrada mensual por año y construir el dataframe final
    df_mensual_ener_suministrada = df_melted_ener_suministrada[df_melted_ener_suministrada["Mes"]!="TOTAL"].copy()
    construir_columna_fecha(df_mensual_ener_suministrada)
    df_mensual_ener_suministrada = df_mensual_ener_suministrada[["Fecha", "Energía suministrada (MWh)"]].reset_index(drop=True).sort_values(by="Fecha", ascending=True)

    return df_mensual_ener_suministrada



def cargar_ener_suministrada_total(**kwargs):
    # Obtener el dataframe con los datos de las demandas máximas
    ti = kwargs['ti']
    energia_suministrada_total_anio = ti.xcom_pull(task_ids='transformar_ener_suministrada_total', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_suministrada_total_anio.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_suministrada.mediciones_totales_anuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_suministrada.mediciones_totales_anuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_suministrada.mediciones_totales_anuales.insert_many(datos_insertar)


def cargar_ener_suministrada_mensual(**kwargs):
    # Obtener el dataframe con los datos de las demandas mensuales
    ti = kwargs['ti']
    energia_suministrada_mensual = ti.xcom_pull(task_ids='transformar_ener_suministrada_mensual', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_suministrada_mensual.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_suministrada.mediciones_mensuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_suministrada.mediciones_mensuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_suministrada.mediciones_mensuales.insert_many(datos_insertar)




with DAG('etl_dag_datos_ener_suministrada',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL correspondiente a energía suministrada',
        ) as dag:
    extraer_task = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer_datos,
        provide_context=True
    )

    transformar_totales_task = PythonOperator(
        task_id='transformar_ener_suministrada_total',
        python_callable=transformar_ener_suministrada_total,
        provide_context=True
    )

    transformar_mensuales_task = PythonOperator(
        task_id='transformar_ener_suministrada_mensual',
        python_callable=transformar_ener_suministrada_mensual,
        provide_context=True
    )

    cargar_totales_task = PythonOperator(
        task_id='cargar_ener_suministrada_total',
        python_callable=cargar_ener_suministrada_total,
        provide_context=True
    )

    cargar_mensuales_task = PythonOperator(
        task_id='cargar_ener_suministrada_mensual',
        python_callable=cargar_ener_suministrada_mensual,
        provide_context=True
    )

    extraer_task >> [transformar_totales_task, transformar_mensuales_task]
    transformar_totales_task >> cargar_totales_task
    transformar_mensuales_task >> cargar_mensuales_task
