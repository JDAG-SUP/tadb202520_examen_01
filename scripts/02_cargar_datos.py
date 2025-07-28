import pandas as pd
from sqlalchemy import create_engine
import time
import os

# --- Configuración de la Conexión a la Base de Datos ---
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "transporte_medellin"

# Construir la URL de conexión de SQLAlchemy
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Configuración de Archivos ---
# Obtener la ruta del directorio actual del script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Construir la ruta al archivo CSV en la carpeta 'datos' que está al mismo nivel que 'scripts'
CSV_PATH = os.path.join(os.path.dirname(SCRIPT_DIR), 'datos', 'registro_llegadas_a_medellin_diciembre_2024.csv')

def cargar_dimension(engine, df, nombre_columna, nombre_tabla, columnas_tabla=None):
    """Carga datos únicos de una columna del DataFrame a una tabla de dimensión."""
    if columnas_tabla is None:
        columnas_tabla = ['nombre']
    
    print(f"Cargando dimensión: {nombre_tabla}...")
    dimension_df = pd.DataFrame(df[nombre_columna].unique(), columns=columnas_tabla)
    dimension_df = dimension_df.dropna()
    
    try:
        dimension_df.to_sql(nombre_tabla, engine, if_exists='append', index=False)
        print(f"Se cargaron {len(dimension_df)} registros en '{nombre_tabla}'.")
    except Exception as e:
        print(f"Error al cargar la dimensión {nombre_tabla}: {e}")
        print("Es posible que los datos ya existan. El proceso continuará.")

def cargar_ciudades(engine, df):
    """Función especializada para cargar la dimensión de ciudades con su subregión."""
    print("Cargando dimensión: ciudades...")
    # Cargar subregiones desde la BD para mapeo
    subregiones_db = pd.read_sql("SELECT subregion_id, nombre FROM subregiones", engine)
    mapa_subregiones = dict(zip(subregiones_db['nombre'], subregiones_db['subregion_id']))

    ciudades_df = df[['nombre_ciudad_origen', 'subregion']].drop_duplicates().dropna()
    ciudades_df.rename(columns={'nombre_ciudad_origen': 'nombre'}, inplace=True)
    ciudades_df['subregion_id'] = ciudades_df['subregion'].map(mapa_subregiones)

    # Seleccionar solo las columnas necesarias para la tabla 'ciudades'
    ciudades_para_insertar = ciudades_df[['nombre', 'subregion_id']].dropna()
    
    try:
        ciudades_para_insertar.to_sql('ciudades', engine, if_exists='append', index=False)
        print(f"Se cargaron {len(ciudades_para_insertar)} registros en 'ciudades'.")
    except Exception as e:
        print(f"Error al cargar la dimensión ciudades: {e}")
        print("Es posible que los datos ya existan. El proceso continuará.")

def main():
    """Función principal para orquestar el proceso de ETL."""
    start_time = time.time()

    print(f"Verificando la existencia del archivo de datos en: {CSV_PATH}")
    if not os.path.exists(CSV_PATH):
        print("="*50)
        print(f"¡ERROR CRÍTICO! No se pudo encontrar el archivo CSV.")
        print(f"Ruta esperada: {CSV_PATH}")
        print("Por favor, asegúrate de que el archivo 'registro_llegadas_a_medellin_diciembre_2024.csv' se encuentra en la carpeta 'datos'.")
        print("="*50)
        return # Detener la ejecución

    print("Archivo CSV encontrado. Iniciando proceso de ETL...")
    print(f"Conectando a la base de datos en {DB_HOST}...")
    engine = create_engine(DATABASE_URL)

    df = pd.read_csv(CSV_PATH, sep=';')
    print(f"Se leyeron {len(df)} filas del archivo CSV.")

    # --- 1. Cargar Tablas de Dimensión ---
    cargar_dimension(engine, df, 'nombre_terminal', 'terminales')
    cargar_dimension(engine, df, 'nombre_empresa', 'empresas')
    cargar_dimension(engine, df, 'subregion', 'subregiones')
    cargar_dimension(engine, df, 'clase_vehiculo', 'clases_vehiculo')
    cargar_ciudades(engine, df)

    # --- 2. Preparar y Cargar Tabla de Hechos (viajes) ---
    print("Preparando datos para la tabla de hechos 'viajes'...")
    
    # Cargar todas las dimensiones desde la BD para mapeo
    terminales_map = pd.read_sql("SELECT terminal_id, nombre FROM terminales", engine).set_index('nombre')['terminal_id']
    empresas_map = pd.read_sql("SELECT empresa_id, nombre FROM empresas", engine).set_index('nombre')['empresa_id']
    clases_vehiculo_map = pd.read_sql("SELECT clase_vehiculo_id, nombre FROM clases_vehiculo", engine).set_index('nombre')['clase_vehiculo_id']
    ciudades_map = pd.read_sql("SELECT ciudad_id, nombre, subregion_id FROM ciudades", engine)
    subregiones_map = pd.read_sql("SELECT subregion_id, nombre FROM subregiones", engine).set_index('nombre')['subregion_id']
    
    # Mapeo de ciudades es más complejo por la subregión
    df['subregion_id'] = df['subregion'].map(subregiones_map)
    df_merged = pd.merge(df, ciudades_map, 
                         left_on=['nombre_ciudad_origen', 'subregion_id'], 
                         right_on=['nombre', 'subregion_id'], 
                         how='left')

    # Mapear IDs
    df['terminal_id'] = df['nombre_terminal'].map(terminales_map)
    df['empresa_id'] = df['nombre_empresa'].map(empresas_map)
    df['clase_vehiculo_id'] = df['clase_vehiculo'].map(clases_vehiculo_map)
    df['origen_ciudad_id'] = df_merged['ciudad_id']

    # Convertir fechas a formato de timestamp
    df['fecha_salida'] = pd.to_datetime(df['fecha_salida'], format='%d/%m/%Y %H:%M')
    df['fecha_llegada'] = pd.to_datetime(df['fecha_llegada'], format='%d/%m/%Y %H:%M')

    # Seleccionar y renombrar columnas para la tabla 'viajes'
    viajes_df = df[[
        'terminal_id', 'empresa_id', 'origen_ciudad_id', 'clase_vehiculo_id',
        'fecha_salida', 'fecha_llegada', 'cantidad_pasajeros'
    ]].dropna()

    # Convertir IDs a enteros
    for col in ['terminal_id', 'empresa_id', 'origen_ciudad_id', 'clase_vehiculo_id']:
        viajes_df[col] = viajes_df[col].astype(int)

    print(f"Iniciando carga de {len(viajes_df)} registros en la tabla 'viajes'...")
    try:
        viajes_df.to_sql('viajes', engine, if_exists='append', index=False, chunksize=1000)
        print("¡Carga de la tabla 'viajes' completada con éxito!")
    except Exception as e:
        print(f"Error al cargar la tabla de hechos 'viajes': {e}")

    end_time = time.time()
    print(f"Proceso de ETL finalizado en {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
