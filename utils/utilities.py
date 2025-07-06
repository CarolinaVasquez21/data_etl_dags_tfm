import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

dict_meses = {
    "Ene": "01",
    "Feb": "02",
    "Mar": "03",
    "Abr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Ago": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dic": "12",
}

def obtener_cliente_db():
    # Cargar las variables del archivo .env
    load_dotenv()

    # Obtener las credenciales y el cluster de MongoDB
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    cluster = os.getenv("DB_CLUSTER")

    # Construir la uri con las credenciales
    uri = rf"mongodb+srv://{username}:{password}@{cluster.lower()}.lmvum.mongodb.net/?retryWrites=true&w=majority&appName={cluster}"


    # Crear un cliente y conectarlo al servidor
    client = MongoClient(uri, 
                        server_api=ServerApi('1'),
                        connectTimeoutMS=60000,
                        socketTimeoutMS=60000,
                        serverSelectionTimeoutMS=60000,
                        tls=True)

    # Si el cliente existe, retornarlo
    if client is not None:
        return client

def ajustar_atipicos_por_anio(df, columna_valor, anios, columna_fecha="Fecha"):
    """
    Ajusta los valores atípicos de una columna numérica en un DataFrame por año usando interpolación lineal.
    
    Parámetros:
    - df: DataFrame con los datos.
    - columna_valor: Nombre de la columna con los valores numéricos a ajustar.
    - anios: Lista de años a procesar.
    - columna_fecha: Nombre de la columna de fechas. Por defecto es "Fecha"

    Retorna:
    - DataFrame con los valores atípicos ajustados.
    """

    # Copia del dataframe para no modificar el original
    df = df.copy() 
    
    # Convertir a datetime
    df[columna_fecha] = pd.to_datetime(df[columna_fecha])

    for anio in anios:
        # Filtrar por año
        df_anio = df[df[columna_fecha].dt.year == anio]

        # Calcular IQR
        q1, q3 = np.percentile(df_anio[columna_valor].dropna(), [25, 75])
        iqr = q3 - q1
        lim_inf = q1 - 1.45 * iqr
        lim_sup = q3 + 1.5 * iqr

        # Reemplazar atípicos con nan para después interpolar
        df.loc[(df[columna_fecha].dt.year == anio) & 
               ((df[columna_valor] < lim_inf) | (df[columna_valor] > lim_sup)), 
               columna_valor] = np.nan

    # Interpolar
    df[columna_valor] = df[columna_valor].interpolate()

    return df

def construir_columna_fecha(df):
    """
    Esta función crea una nueva columna de fecha en un DataFrame dado. 
    Si el DataFrame contiene una columna relacionada con el mes, se utiliza 
    para construir una fecha en formato 'Año/Mes'. Si no hay columna de mes, 
    se asigna el primer día del año ('Año/01/01'). 
    
    - Convierte la columna 'Año' a tipo string.
    - Si la columna de mes está en formato de texto, la mapea a valores numéricos.
    - Si la columna de mes es un número entero, la convierte a string.
    - Finalmente, combina 'Año' y 'Mes' en una columna 'Fecha' con formato datetime.
    """
    
    tiene_mes = False
    cols_df = df.columns.values

    # Verificar si el dataframe tiene una columna de mes
    for col in cols_df:
        if "mes" in col.lower():
            col_mes = col
            tiene_mes = True
    
    # Transformar el año a cadena de texto para posteriormente concatenarlo
    # y transformarlo a datetime
    df["Año"] = df["Año"].astype("int")
    df["Año"] = df["Año"].astype("string")

    # En caso de que tenga mes
    if tiene_mes:
        if pd.api.types.is_string_dtype(df[col_mes]) or df[col_mes].dtype == "string" or df[col_mes].dtype == "object":
            df[col_mes] = df[col_mes].map(dict_meses)

        if df[col_mes].dtype == "int64" or df[col_mes].dtype == "int32":
            df[col_mes] = df[col_mes].astype("string")
            
        df["Fecha"] = df["Año"] + "/" + df[col_mes]
        df["Fecha"] = pd.to_datetime(df["Fecha"], yearfirst=True)
    else:
        df["Fecha"] = df["Año"] + "/01/01"
        df["Fecha"] = pd.to_datetime(df["Fecha"], yearfirst=True)


def desagregar_demanda_mensual(df_mensual, df_anual, nombre_col_valor, acumulada=False):
    """
    Esta función desagrega los valores de demanda anual en valores mensuales utilizando 
    datos históricos de un DataFrame mensual como referencia para calcular un patrón de distribución.

    Parámetros:
    - df_mensual: DataFrame con valores mensuales de referencia.
    - df_anual: DataFrame con valores anuales a desagregar.
    - nombre_col_valor: Nombre de la columna que contiene los valores de demanda.
    - acumulada: Booleano que indica si los valores anuales son acumulados (True) 
      o representan totales anuales no acumulados (False).

    Proceso:
    - Convierte la columna 'Fecha' a datetime y extrae mes y año.
    - Si los valores no son acumulados, calcula un patrón mensual promedio basado en 
      los porcentajes mensuales sobre la demanda anual.
    - Si los valores son acumulados, utiliza el valor de diciembre como referencia 
      para calcular un patrón de distribución.
    - Distribuye los valores anuales en valores mensuales utilizando el patrón promedio.
    - Ajusta posibles diferencias debido a redondeos para mantener la suma total anual.

    Retorna:
    - Un DataFrame con los valores desagregados en formato [Año, Mes, Valor].
    """

    # Asegurarse que la columna 'Fecha' sea datetime y extraer el mes y año
    df_mensual = df_mensual.copy()
    df_mensual['Fecha'] = pd.to_datetime(df_mensual['Fecha'])
    df_mensual['Mes'] = df_mensual['Fecha'].dt.month
    df_mensual['Año'] = df_mensual['Fecha'].dt.year

    # Lista de meses (1 a 12)
    meses = list(range(1, 13))
    datos_desagregados = []

    if not acumulada:
        # Caso en el cual los valores no son acumulados
        # El porcentaje SI debe ser repartido

        # Calcular la demanda total por año en el dataframe mensual y el porcentaje mensual
        df_mensual['valor_anual'] = df_mensual.groupby('Año')[nombre_col_valor].transform('sum')

        # Calcular el porcentaje mensual sobre cada mes
        df_mensual['porcentaje_mensual'] = df_mensual[nombre_col_valor] / df_mensual['valor_anual']
        
        # Calcular el patrón mensual promedio usando todos los años disponibles en el dataframe mensual
        patron_mensual = df_mensual.groupby('Mes')['porcentaje_mensual'].median()
        print(patron_mensual)
        
        # Iterar sobre cada año en el DataFrame anual
        for _, row in df_anual.iterrows():
            año = row['Fecha'].year
            total_anual = row[nombre_col_valor]
            
            # Estimar los valores mensuales usando el patrón promedio multiplicado por el total anual
            # Se toma el patrón para cada mes
            valores_mensuales = patron_mensual.loc[meses] * total_anual
            
            # Ajuste para que la suma de los 12 meses sea exactamente igual al total anual (por posibles redondeos)
            diferencia = total_anual - valores_mensuales.sum()
            ajuste = diferencia / 12  # Distribución uniforme del ajuste
            valores_mensuales += ajuste
            
            for mes, demanda_mes in zip(meses, valores_mensuales):
                datos_desagregados.append([año, mes, demanda_mes])


    else:
        # Caso en el cual los valores son acumulados
        # El porcentaje en este caso NO debe ser repartido

        meses = np.arange(1, 13)  # Meses de 1 a 12

        # Calcular el patrón de crecimiento mensual basado en datos históricos
        df_mensual['porcentaje_mensual_mes12'] = df_mensual[nombre_col_valor] / df_mensual.groupby('Año')[nombre_col_valor].transform('last')

        # Se usa la mediana de cada mes para evitar sesgos de valores atípicos
        patron_mensual_ac = df_mensual.groupby('Mes')['porcentaje_mensual_mes12'].median()
        patron_mensual_ac /= patron_mensual_ac.sum()  # Normalizamos a 1

        # Ordenar el dataframe anual por año
        df_anual = df_anual.sort_values('Año').reset_index(drop=True)

        # Inicializar diciembre del primer año conocido
        valor_diciembre_anterior = df_anual.loc[0, nombre_col_valor]

        # Iterar sobre los años a partir del segundo año
        for i in range(1, len(df_anual)):
            año_actual = df_anual.loc[i, 'Año']
            total_actual = df_anual.loc[i, nombre_col_valor]

            # Calcular el incremento anual
            incremento_anual = total_actual - valor_diciembre_anterior

            # Distribuir el crecimiento mensual acumulativo
            valores_mensuales = [valor_diciembre_anterior]  # Empezamos desde diciembre del año anterior
            for mes in meses[:-1]:  # Enero a noviembre
                nuevo_valor = valores_mensuales[-1] + (incremento_anual * patron_mensual_ac.loc[mes])
                valores_mensuales.append(nuevo_valor)

            # Último mes (diciembre) debe coincidir con el total anual
            valores_mensuales.append(total_actual)

            # Guardar datos desagregados
            for mes, demanda_mes in zip(meses, valores_mensuales):
                datos_desagregados.append([año_actual, mes, demanda_mes])

            # Actualizar diciembre como base para el próximo año
            valor_diciembre_anterior = total_actual


    # Crear DataFrame de salida
    df_desagregado = pd.DataFrame(datos_desagregados, columns=['Año', 'Mes', nombre_col_valor])
    
    if acumulada:
        # Desplazamiento de valores hacia atrás
        df_desagregado[nombre_col_valor] = df_desagregado[nombre_col_valor].shift(-1)
        
        # Debido a que se desplazó, rellenar el último valor nulo
        df_desagregado = df_desagregado.fillna(df_anual.iloc[-1][nombre_col_valor])
          
            
    return df_desagregado