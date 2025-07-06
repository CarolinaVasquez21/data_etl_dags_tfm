from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import numpy as np
import sys

# Se pone en el path para que sean accesibles
sys.path.append('/opt/airflow/data')
sys.path.append('/opt/airflow/utils')
from utilities import construir_columna_fecha, obtener_cliente_db


def extraer_datos(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos de potencia
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_clientes = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Consumo_x_grupo")

    # Cargar los valores con xcom
    return df_clientes


def transformar_clientes_anuales_acum(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_clientes = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Eliminar nulos y limpiar columna año
    df_clientes_anuales_acum = df_clientes.dropna(subset=["Año"])
    df_clientes_anuales_acum["Año"] = df_clientes_anuales_acum["Año"].astype('string').apply(lambda x: x.split('.')[0])

    # Construir primera columna fecha para agrupar y obtener el valor acumulado del último mes del año
    construir_columna_fecha(df_clientes_anuales_acum)
    df_clientes_anuales_acum = df_clientes_anuales_acum[['Año','Fecha', 'Clientes']]
    df_clientes_anuales_acum = df_clientes_anuales_acum.groupby('Fecha')['Clientes'].apply(np.sum).reset_index(drop=False)
    df_clientes_anuales_acum = df_clientes_anuales_acum[df_clientes_anuales_acum["Fecha"].dt.month==12]

    # Obtener nuevamente el año y dejar solo las columnas Año y Clientes
    df_clientes_anuales_acum["Año"] = df_clientes_anuales_acum["Fecha"].astype("string").apply(lambda x: x.split('-')[0])
    df_clientes_anuales_acum = df_clientes_anuales_acum[["Año", "Clientes"]]

    # Asegurarse que el número de clientes sea un entero
    df_clientes_anuales_acum["Clientes"] = df_clientes_anuales_acum["Clientes"].astype("int")

    # Construir columna Fecha a partir de la columna Año (Cogimos el mes 12, debemos tener por ejemplo 2000-01-01, no 2000-12-01)
    construir_columna_fecha(df_clientes_anuales_acum)
    df_clientes_anuales_acum = df_clientes_anuales_acum[["Fecha", "Clientes"]]

    return df_clientes_anuales_acum



def transformar_clientes_mensuales_acum(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_clientes = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Eliminar nulos y generar columna año
    df_clientes_mensuales_acum = df_clientes.dropna(subset=["Año"])
    df_clientes_mensuales_acum["Año"] = df_clientes_mensuales_acum["Año"].astype('string').apply(lambda x: x.split('.')[0])

    # Construir columna fecha de tipo datetime
    construir_columna_fecha(df_clientes_mensuales_acum)
    df_clientes_mensuales_acum = df_clientes_mensuales_acum[['Fecha', 'Clientes']]

    # Agrupar y obtener los valores mensuales acumulados
    df_clientes_mensuales_acum = df_clientes_mensuales_acum.groupby("Fecha")["Clientes"].apply(np.sum)
    df_clientes_mensuales_acum = df_clientes_mensuales_acum.reset_index(drop=False)
    df_clientes_mensuales_acum = df_clientes_mensuales_acum[['Fecha', 'Clientes']].copy()
    df_clientes_mensuales_acum["Clientes"] = df_clientes_mensuales_acum["Clientes"].astype("int")

    return df_clientes_mensuales_acum


def cargar_clientes_anuales_acum(**kwargs):
    # Obtener el dataframe con los datos de los clientes anuales acumulados
    ti = kwargs['ti']
    clientes_anuales_acum = ti.xcom_pull(task_ids='transformar_clientes_anuales_acum', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = clientes_anuales_acum.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.num_clientes_sisdat.mediciones_anuales_acum.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.num_clientes_sisdat.mediciones_anuales_acum.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.num_clientes_sisdat.mediciones_anuales_acum.insert_many(datos_insertar)


def cargar_clientes_mensuales_acum(**kwargs):
    # Obtener el dataframe con los datos de los clientes mensuales acumulados
    ti = kwargs['ti']
    clientes_mensuales_acum = ti.xcom_pull(task_ids='transformar_clientes_mensuales_acum', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = clientes_mensuales_acum.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.num_clientes_sisdat.mediciones_mensuales_acum.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.num_clientes_sisdat.mediciones_mensuales_acum.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.num_clientes_sisdat.mediciones_mensuales_acum.insert_many(datos_insertar)




with DAG('etl_dag_datos_num_clientes_sisdat',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL correspondiente a número de clientes (SISDAT)',
        ) as dag:
    extraer_task = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer_datos,
        provide_context=True
    )

    transformar_totales_task = PythonOperator(
        task_id='transformar_clientes_anuales_acum',
        python_callable=transformar_clientes_anuales_acum,
        provide_context=True
    )

    transformar_mensuales_task = PythonOperator(
        task_id='transformar_clientes_mensuales_acum',
        python_callable=transformar_clientes_mensuales_acum,
        provide_context=True
    )

    cargar_totales_task = PythonOperator(
        task_id='cargar_clientes_anuales_acum',
        python_callable=cargar_clientes_anuales_acum,
        provide_context=True
    )

    cargar_mensuales_task = PythonOperator(
        task_id='cargar_clientes_mensuales_acum',
        python_callable=cargar_clientes_mensuales_acum,
        provide_context=True
    )

    extraer_task >> [transformar_totales_task, transformar_mensuales_task]
    transformar_totales_task >> cargar_totales_task
    transformar_mensuales_task >> cargar_mensuales_task
