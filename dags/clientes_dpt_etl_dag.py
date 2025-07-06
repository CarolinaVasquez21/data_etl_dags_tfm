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
    
    # Leer el archivo en la hoja específica que contiene los datos anuales acumulados de clientes de DPT
    path_archivo = r"/opt/airflow/data/Resumen de datos2.xlsx"
    df_clientes_acum_anuales_dpt = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Cantidad_clientes_anual",skiprows=0, usecols=["Unnamed: 0", "TOTAL"], dtype={"TOTAL":str})
    
    # Obtener los datos mensuales de la hoja 'Clientes' del archivo de excel
    df_clientes_mensuales_acum_dpt = pd.read_excel(path_archivo, engine="openpyxl", sheet_name="Clientes",skiprows=23)

    # Retornar los valores
    return df_clientes_acum_anuales_dpt, df_clientes_mensuales_acum_dpt


def transformar_clientes_acum_anuales(**kwargs):

    # Obtener el dataframe extraido
    ti = kwargs['ti']
    clientes_anuales_dpt, _= ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Filtrar las columnas que contienen los datos
    clientes_anuales_dpt = clientes_anuales_dpt[["Unnamed: 0", "TOTAL"]]
    
    # Solo desde 1984 existen datos
    clientes_anuales_dpt = clientes_anuales_dpt[clientes_anuales_dpt["Unnamed: 0"]>=1984]
    
    # Renombrar las columnas y transformar año a tipo de dato entero
    clientes_anuales_dpt.columns = ["Año", "Clientes"]
    clientes_anuales_dpt["Clientes"] = clientes_anuales_dpt["Clientes"].apply(lambda x: x.replace(',','')).astype("float").apply(np.round).astype("int")
    clientes_anuales_dpt["Año"] = clientes_anuales_dpt["Año"].astype("int")
    clientes_anuales_dpt_og = clientes_anuales_dpt.copy()

    # Construir la columna fecha y conservar columnas de interés
    construir_columna_fecha(clientes_anuales_dpt)
    clientes_anuales_dpt = clientes_anuales_dpt[["Fecha","Clientes"]]

    # Retornar el dataframe con los clientes anuales acumulados por año
    return clientes_anuales_dpt, clientes_anuales_dpt_og


def transformar_clientes_acum_mensuales(**kwargs):

    # Obtener el dataframe con los datos de clientes mensuales disponibles de DPT
    ti = kwargs['ti']
    _ , clientes_mensuales_dpt = ti.xcom_pull(task_ids='extraer_datos', key='return_value')

    # Mantener solo registros con valor en la columna del mes 'Etiquetas de fila'
    # y también, solo registros de interés
    clientes_mensuales_dpt = clientes_mensuales_dpt.dropna(subset=["Etiquetas de fila"])
    clientes_mensuales_dpt = clientes_mensuales_dpt.iloc[:-1,:-27]

    # Cambiar formato de dataframe de ancho a largo para obtener registros por cada año y mes
    melted_clientes_mensuales_dpt = clientes_mensuales_dpt.melt(id_vars=['Etiquetas de fila'], var_name='Mes', value_name='Valor')
    melted_clientes_mensuales_dpt.columns = ["Mes", "Año", "Clientes"]

    # Construir la columna de fecha en formato datetime
    construir_columna_fecha(melted_clientes_mensuales_dpt)

    # Mantener solo columnas de interés
    melted_clientes_mensuales_dpt = melted_clientes_mensuales_dpt[["Fecha", "Clientes"]]
    melted_clientes_mensuales_dpt["Clientes"] = melted_clientes_mensuales_dpt["Clientes"].astype("int")

    # Retornar el dataframe transformado
    return melted_clientes_mensuales_dpt


def desagregar_clientes_acum_anuales_dpt(**kwargs):
    
    ti = kwargs['ti']
    clientes_mensuales_dpt = ti.xcom_pull(task_ids='transformar_clientes_acum_mensuales', key='return_value')
    _, clientes_totales_dpt = ti.xcom_pull(task_ids='transformar_clientes_acum_anuales', key='return_value')
    
    # Desagregar los clientes utilizando la función ya definida
    # Notar que en este caso 'acumulada=True', pues los clientes como tal no se reparten por porcentajes, pues
    # se tiene que desagregar de manera que el crecimiento sea acumulado por mes hasta llegar al total acumulado.
    df_desagregado_clientes_dpt = \
        desagregar_demanda_mensual(clientes_mensuales_dpt, clientes_totales_dpt, nombre_col_valor="Clientes", acumulada=True)
    
    # Redondear el número de clientes y transformar a tipo de dato entero
    df_desagregado_clientes_dpt["Clientes"] = df_desagregado_clientes_dpt["Clientes"].apply(np.round).astype("int")
    
    # Agregar la columna 'Fecha' de tipo 'datetime'
    construir_columna_fecha(df_desagregado_clientes_dpt)
    
    # Conservar únicamente las dos columnas de interés
    df_desagregado_clientes_dpt = df_desagregado_clientes_dpt[["Fecha", "Clientes"]]

    # Hacer merge para conservar los valores originales que si se tienen
    #merged_desag_og = df_desagregado_clientes_dpt.merge(clientes_mensuales_dpt, on="Fecha", how="left", suffixes=("_desag","_og"))
    #merged_desag_og["Clientes"] = merged_desag_og["Clientes_og"].fillna(merged_desag_og["Clientes_desag"])
    #merged_desag_og = merged_desag_og[["Fecha", "Clientes"]]
    
    # Retornar el dataframe con las desagregaciones mensuales
    return df_desagregado_clientes_dpt

def cargar_clientes_acum_anuales(**kwargs):
    # Obtener el dataframe que contiene los datos de los clientes acumulados anuales
    ti = kwargs['ti']
    total_clientes_acum_anual, _ = ti.xcom_pull(task_ids='transformar_clientes_acum_anuales', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = total_clientes_acum_anual.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.num_clientes_dpt.mediciones_anuales_acum.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.num_clientes_dpt.mediciones_anuales_acum.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.num_clientes_dpt.mediciones_anuales_acum.insert_many(datos_insertar)


def cargar_clientes_acum_mensuales(**kwargs):
    # Obtener el dataframe con los datos de clientes acumulados mensuales desagregados en base a los totales
    ti = kwargs['ti']
    energia_facturada_mensual = ti.xcom_pull(task_ids='desagregar_clientes_acum_anuales_dpt', key='return_value')

    # Obtener cliente para conectar a la db
    db_cliente = obtener_cliente_db()

    # Convertir el dataframe a diccionario
    datos_insertar = energia_facturada_mensual.to_dict(orient='records')

    # Eliminar cualquier documento existente en la colección
    db_cliente.num_clientes_dpt.mediciones_mensuales_acum_dsg.delete_many({})

    # Creación de índice para el campo de la fecha
    db_cliente.num_clientes_dpt.mediciones_mensuales_acum_dsg.create_index([("Fecha", 1)])

    # Insertar en la base de datos
    db_cliente.num_clientes_dpt.mediciones_mensuales_acum_dsg.insert_many(datos_insertar)




with DAG('etl_dag_datos_num_clientes_dpt',
        start_date=datetime(2025, 2, 1), 
        schedule_interval=None, 
        catchup=False,
        description='DAG de proceso ETL correspondiente a número de clientes (DPT)',
        ) as dag:
    extraer_task = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer_datos,
        provide_context=True
    )

    transformar_totales_task = PythonOperator(
        task_id='transformar_clientes_acum_anuales',
        python_callable=transformar_clientes_acum_anuales,
        provide_context=True
    )

    transformar_mensuales_task = PythonOperator(
        task_id='transformar_clientes_acum_mensuales',
        python_callable=transformar_clientes_acum_mensuales,
        provide_context=True
    )
    
    desagregar_totales_dpt_task = PythonOperator(
        task_id='desagregar_clientes_acum_anuales_dpt',
        python_callable=desagregar_clientes_acum_anuales_dpt,
        provide_context=True
    )    

    cargar_totales_task = PythonOperator(
        task_id='cargar_clientes_acum_anuales',
        python_callable=cargar_clientes_acum_anuales,
        provide_context=True
    )

    cargar_mensuales_task = PythonOperator(
        task_id='cargar_clientes_acum_mensuales',
        python_callable=cargar_clientes_acum_mensuales,
        provide_context=True
    )

    extraer_task >> [transformar_totales_task, transformar_mensuales_task]
    transformar_mensuales_task >> desagregar_totales_dpt_task
    transformar_totales_task >> [cargar_totales_task, desagregar_totales_dpt_task]
    desagregar_totales_dpt_task >> cargar_mensuales_task