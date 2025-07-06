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
from utilities import construir_columna_fecha, obtener_cliente_db, ajustar_atipicos_por_anio


def extraer_datos(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos de potencia
    path_archivo = r"/opt/airflow/data/Resumen_datos.xlsx"
    df_ener_facturada = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Consumo_x_grupo")

    # Cargar los valores con xcom
    return df_ener_facturada


def transformar_ener_facturada_total(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_ener_facturada = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Eliminar nulos y generar columna año
    df_demanda_total_ener_sisdat = df_ener_facturada.dropna(subset=["Año"])
    df_demanda_total_ener_sisdat['Año'] = df_demanda_total_ener_sisdat['Año'].astype('string').apply(lambda x: x.split('.')[0])

    # Obtener valores totales por año
    df_demanda_total_ener_sisdat = df_demanda_total_ener_sisdat[['Año', 'Energía Facturada (MWh)']]
    df_demanda_total_ener_sisdat = df_demanda_total_ener_sisdat.groupby('Año')['Energía Facturada (MWh)'].apply(np.sum).reset_index(drop=False)

    # Construir una columna fecha de tipo datetime y conservar columans de interés
    construir_columna_fecha(df_demanda_total_ener_sisdat)
    df_demanda_total_ener_sisdat = df_demanda_total_ener_sisdat[["Fecha", "Energía Facturada (MWh)"]]

    return df_demanda_total_ener_sisdat



def transformar_ener_facturada_mensual(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    df_ener_facturada = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Eliminar nulos y generar columna año
    df_demanda_mensual_sisdat = df_ener_facturada.dropna(subset=["Año"])
    df_demanda_mensual_sisdat["Año"] = df_demanda_mensual_sisdat["Año"].astype('string').apply(lambda x: x.split('.')[0])

    # Construir la columna fecha de tipo datetime
    construir_columna_fecha(df_demanda_mensual_sisdat)
    df_demanda_mensual_sisdat = df_demanda_mensual_sisdat[['Fecha', 'Energía Facturada (MWh)']]

    # Agrupar y obtener los valores mensuales por año
    df_demanda_mensual_sisdat = df_demanda_mensual_sisdat.groupby("Fecha")["Energía Facturada (MWh)"].apply(np.sum)
    df_demanda_mensual_sisdat = df_demanda_mensual_sisdat.reset_index(drop=False)
    df_demanda_mensual_sisdat = df_demanda_mensual_sisdat[['Fecha', 'Energía Facturada (MWh)']].copy()

    # En el análisis exploratorio nos dimos cuenta que habían atípicos, se interpolarán con un nuevo valor
    anios = [2017, 2022]
    df_demanda_ener_sisdat_interpolado = ajustar_atipicos_por_anio(df_demanda_mensual_sisdat, "Energía Facturada (MWh)", anios)

    return df_demanda_ener_sisdat_interpolado


def cargar_ener_facturada_total(**kwargs):
    # Obtener el dataframe con los datos de la energía facturada total por año
    ti = kwargs['ti']
    energia_facturada_total_anio = ti.xcom_pull(task_ids='transformar_ener_facturada_total', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_facturada_total_anio.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_facturada_sisdat.mediciones_totales_anuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_facturada_sisdat.mediciones_totales_anuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_facturada_sisdat.mediciones_totales_anuales.insert_many(datos_insertar)


def cargar_ener_facturada_mensual(**kwargs):
    # Obtener el dataframe con los datos de la energía mensual facturada
    ti = kwargs['ti']
    energia_facturada_mensual = ti.xcom_pull(task_ids='transformar_ener_facturada_mensual', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_facturada_mensual.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_facturada_sisdat.mediciones_mensuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_facturada_sisdat.mediciones_mensuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_facturada_sisdat.mediciones_mensuales.insert_many(datos_insertar)




with DAG('etl_dag_datos_ener_facturada_sisdat',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL correspondiente a energía facturada (SISDAT)',
        ) as dag:
    extraer_task = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer_datos,
        provide_context=True
    )

    transformar_totales_task = PythonOperator(
        task_id='transformar_ener_facturada_total',
        python_callable=transformar_ener_facturada_total,
        provide_context=True
    )

    transformar_mensuales_task = PythonOperator(
        task_id='transformar_ener_facturada_mensual',
        python_callable=transformar_ener_facturada_mensual,
        provide_context=True
    )

    cargar_totales_task = PythonOperator(
        task_id='cargar_ener_facturada_total',
        python_callable=cargar_ener_facturada_total,
        provide_context=True
    )

    cargar_mensuales_task = PythonOperator(
        task_id='cargar_ener_facturada_mensual',
        python_callable=cargar_ener_facturada_mensual,
        provide_context=True
    )

    extraer_task >> [transformar_totales_task, transformar_mensuales_task]
    transformar_totales_task >> cargar_totales_task
    transformar_mensuales_task >> cargar_mensuales_task
