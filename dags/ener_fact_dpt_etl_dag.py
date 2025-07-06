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
from utilities import construir_columna_fecha, obtener_cliente_db, desagregar_demanda_mensual


def extraer_datos(**kwargs):
    
    # Leer el archivo en la hoja específica que contiene los datos anuales de energía facturada de DPT
    path_archivo = r"/opt/airflow/data/Resumen de datos3.xlsx"
    df_ener_facturada_anual_dpt = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Mwh-facturados",skiprows=4, nrows=60)
    
    # Leer el archivo en la hoja específica que contiene los datos mensuales de energía facturada de DPT
    df_ener_facturada_mensual_dpt = pd.read_excel(path_archivo, engine="openpyxl", skiprows=3, sheet_name = 'Energia_consumida')

    # Cargar los valores con xcom
    return df_ener_facturada_anual_dpt, df_ener_facturada_mensual_dpt


def transformar_ener_facturada_total(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    facturada_anual_dpt, _ = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Conservar columans de interés
    df_demanda_total_ener_plan_hist = facturada_anual_dpt[['MWh', 'TOTAL']].copy()
    
    # Transformar columna a tipo de dato 'int' para obtener solo las mediciones reales (2023 en adelante son predicciones)
    df_demanda_total_ener_plan_hist['MWh'] = df_demanda_total_ener_plan_hist['MWh'].astype("int")
    df_demanda_total_ener_plan_hist = df_demanda_total_ener_plan_hist[df_demanda_total_ener_plan_hist['MWh']<=2024]
    
    # Renombrar columnas
    df_demanda_total_ener_plan_hist.columns = ['Año', 'Energía Facturada (MWh)']
    df_demanda_total_ener_plan_hist['Año'] = df_demanda_total_ener_plan_hist['Año'].astype("int")
    
    # Construir una columna fecha de tipo datetime y conservar columans de interés
    construir_columna_fecha(df_demanda_total_ener_plan_hist)
    df_demanda_total_ener_plan_hist = df_demanda_total_ener_plan_hist[["Fecha", "Energía Facturada (MWh)"]]

    return df_demanda_total_ener_plan_hist



def transformar_ener_facturada_mensual(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    _, facturada_mensual_dpt = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Transformar la energia facturada mensual del DPT (Mediciones disponibles)
    melted_mensual_dpt = facturada_mensual_dpt.melt(id_vars=['Meses'], var_name='Mes', value_name='Valor')
    melted_mensual_dpt.columns = ["Mes", "Año", "Energía Facturada (MWh)"]
    melted_mensual_dpt = melted_mensual_dpt[melted_mensual_dpt['Año']!="2024"]
    melted_mensual_dpt = melted_mensual_dpt[melted_mensual_dpt['Energía Facturada (MWh)']>0]
    melted_mensual_dpt = melted_mensual_dpt.dropna(subset=['Energía Facturada (MWh)', 'Mes'])
    
    # Ya descartamos la columna '2024' de la izquierda, ahora debemos poner la derecha (AZ del excel), como 2024
    melted_mensual_dpt["Año"] = melted_mensual_dpt["Año"].replace({"2024.1": "2024"})

    # Crear la columna fecha y conservar columans de interés
    melted_mensual_dpt = melted_mensual_dpt[(melted_mensual_dpt["Mes"]!="TOTAL") & (melted_mensual_dpt["Mes"]!="FC (%)")].copy()
    construir_columna_fecha(melted_mensual_dpt)
    melted_mensual_dpt = melted_mensual_dpt[["Fecha", "Energía Facturada (MWh)"]]
    
    # Retornar los datos mensuales disponibles de energía facturada de DPT
    return melted_mensual_dpt

def desagregar_ener_facturada_total_dpt(**kwargs):
    
    ti = kwargs['ti']
    mensuales_dpt = ti.xcom_pull(task_ids='transformar_ener_facturada_mensual', key='return_value')
    totales_dpt = ti.xcom_pull(task_ids='transformar_ener_facturada_total', key='return_value')
    
    # Obtener los totales desagregados de DPT usando los mensuales disponibles de DPT como referencia
    mensuales_dpt_desag = desagregar_demanda_mensual(mensuales_dpt, totales_dpt, "Energía Facturada (MWh)")
    construir_columna_fecha(mensuales_dpt_desag)
    mensuales_dpt_desag = mensuales_dpt_desag[["Fecha", "Energía Facturada (MWh)"]]

    # Hacer merge para conservar los valores originales que si se tienen
    merged_desag_og = mensuales_dpt_desag.merge(mensuales_dpt, on="Fecha", how="left", suffixes=("_desag","_og"))
    merged_desag_og["Energía Facturada (MWh)"] = merged_desag_og["Energía Facturada (MWh)_og"].fillna(merged_desag_og["Energía Facturada (MWh)_desag"])
    merged_desag_og = merged_desag_og[["Fecha", "Energía Facturada (MWh)"]]

    # Retornar el DataFrame con los datos desagregados    
    return merged_desag_og

def cargar_ener_facturada_total(**kwargs):
    # Obtener el dataframe con los datos de la energía facturada total por año
    ti = kwargs['ti']
    energia_facturada_total_anio = ti.xcom_pull(task_ids='transformar_ener_facturada_total', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_facturada_total_anio.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_facturada_dpt.mediciones_totales_anuales.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_facturada_dpt.mediciones_totales_anuales.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_facturada_dpt.mediciones_totales_anuales.insert_many(datos_insertar)


def cargar_ener_facturada_mensual(**kwargs):
    # Obtener el dataframe con los datos de la energía mensual facturada
    ti = kwargs['ti']
    energia_facturada_mensual = ti.xcom_pull(task_ids='desagregar_ener_facturada_anual_dpt', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_facturada_mensual.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.energia_facturada_dpt.mediciones_mensuales_dsg.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.energia_facturada_dpt.mediciones_mensuales_dsg.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.energia_facturada_dpt.mediciones_mensuales_dsg.insert_many(datos_insertar)




with DAG('etl_dag_datos_ener_facturada_dpt',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL correspondiente a energía facturada (DPT)',
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
    
    desagregar_totales_dpt_task = PythonOperator(
        task_id='desagregar_ener_facturada_anual_dpt',
        python_callable=desagregar_ener_facturada_total_dpt,
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
    transformar_totales_task >> [cargar_totales_task, desagregar_totales_dpt_task]
    transformar_mensuales_task >> desagregar_totales_dpt_task
    desagregar_totales_dpt_task >> cargar_mensuales_task