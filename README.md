# Análisis del Transporte de Pasajeros a Medellín - Diciembre 2024

Este repositorio contiene la solución completa para el examen de Tópicos Avanzados de Base de Datos, enfocado en el diseño, implementación y análisis de una base de datos para gestionar la información del transporte terrestre de pasajeros hacia Medellín en diciembre de 2024.

## Integrantes

- Juan David Acevedo Gómez (solo yo esta vez XD)
NRC: 000521113

## Contenido del Repositorio

- **/datos**: Contiene el conjunto de datos original (`registro_llegadas_a_medellin_diciembre_2024.csv`).
- **/documentos**: Contiene los entregables de documentación, como este README, el diagrama relacional y los archivos de salida de las consultas.
- **/scripts**: Contiene todos los scripts necesarios para la creación y operación de la base de datos.
  - `01_modelo_ddl.sql`: Script DDL para crear la estructura de la base de datos (tablas, claves, etc.).
  - `02_cargar_datos.py`: Script de Python (ETL) para poblar la base de datos desde el archivo CSV.
  - `03_consultas_analisis.sql`: Consultas de análisis (Etapa 4) con CTEs y Funciones Ventana.
  - `04_logica_almacenada.sql`: Creación de la función y el procedimiento almacenado (Etapa 5).
  - `05_prueba_funcionalidad.sql`: Script para verificar el correcto funcionamiento de todo el sistema.
  - `requirements.txt`: Dependencias de Python para el script de carga.
- `.gitignore`: Archivo para ignorar archivos no deseados en el control de versiones.
- `docker-compose.yml`: Archivo de configuración para levantar el contenedor de PostgreSQL con Docker.
- `README.md`: Este archivo.

## Cómo Ejecutar el Proyecto

### Prerrequisitos

- **Docker Desktop**: Asegúrate de que esté instalado y en ejecución.
- **Python 3**: Necesario para ejecutar el script de carga de datos.

### Pasos para la Configuración

1.  **Levantar la Base de Datos**:
    En la terminal, desde la raíz del proyecto, ejecuta el siguiente comando para iniciar el contenedor de PostgreSQL:
    ```bash
    docker-compose up -d
    ```

2.  **Crear la Estructura de la Base de Datos**:
    Ejecuta el script DDL para crear todas las tablas. Puedes hacerlo desde un IDE de base de datos o con el siguiente comando en la terminal:
    ```powershell
    Get-Content .\scripts\01_modelo_ddl.sql | docker exec -i postgres_transporte psql -U postgres -d transporte_medellin
    ```

3.  **Instalar Dependencias de Python**:
    Instala las librerías necesarias para el script de carga:
    ```bash
    python -m pip install -r .\scripts\requirements.txt
    ```

4.  **Cargar los Datos**:
    Ejecuta el script de Python para poblar la base de datos. Este proceso puede tardar unos segundos.
    ```bash
    python .\scripts\02_cargar_datos.py
    ```

5.  **Crear Lógica Almacenada**:
    Ejecuta el script para crear la función y el procedimiento:
    ```powershell
    Get-Content .\scripts\04_logica_almacenada.sql | docker exec -i postgres_transporte psql -U postgres -d transporte_medellin
    ```

6.  **Verificar y Probar**:
    En este punto, el sistema está completamente funcional. Puedes ejecutar las consultas en `03_consultas_analisis.sql` o el script de prueba `05_prueba_funcionalidad.sql` para verificar los resultados.
