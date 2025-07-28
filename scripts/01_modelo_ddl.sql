-- =====================================================================
-- Script de Creación del Modelo de Datos (DDL)
-- Base de Datos: PostgreSQL
-- Proyecto: Análisis de Transporte de Pasajeros a Medellín
-- =====================================================================

-- Limpieza inicial (opcional, útil para re-ejecutar el script)
DROP TABLE IF EXISTS estadisticas_diarias CASCADE;
DROP TABLE IF EXISTS viajes CASCADE;
DROP TABLE IF EXISTS terminales CASCADE;
DROP TABLE IF EXISTS empresas CASCADE;
DROP TABLE IF EXISTS ciudades CASCADE;
DROP TABLE IF EXISTS subregiones CASCADE;
DROP TABLE IF EXISTS clases_vehiculo CASCADE;


-- =============================================
-- Creación de Tablas de Dimensión (Catálogos)
-- =============================================

-- Tabla para almacenar las terminales de Medellín
CREATE TABLE terminales (
    terminal_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla para almacenar las empresas de transporte
CREATE TABLE empresas (
    empresa_id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL UNIQUE
);

-- Tabla para almacenar las subregiones de origen
CREATE TABLE subregiones (
    subregion_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla para almacenar las ciudades de origen, vinculadas a una subregión
CREATE TABLE ciudades (
    ciudad_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    subregion_id INT NOT NULL,
    CONSTRAINT fk_subregion FOREIGN KEY (subregion_id) REFERENCES subregiones(subregion_id),
    UNIQUE (nombre, subregion_id) -- Una ciudad con el mismo nombre puede existir en otra subregión
);

-- Tabla para almacenar las clases de vehículo
CREATE TABLE clases_vehiculo (
    clase_vehiculo_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);


-- =============================================
-- Creación de la Tabla de Hechos
-- =============================================

-- Tabla principal que registra cada viaje individual
CREATE TABLE viajes (
    viaje_id BIGSERIAL PRIMARY KEY, -- Usamos BIGSERIAL por si hay más de 2 mil millones de viajes a futuro
    terminal_id INT NOT NULL,
    empresa_id INT NOT NULL,
    origen_ciudad_id INT NOT NULL,
    clase_vehiculo_id INT NOT NULL,
    fecha_salida TIMESTAMP NOT NULL,
    fecha_llegada TIMESTAMP NOT NULL,
    cantidad_pasajeros INT NOT NULL,

    -- Definición de claves foráneas
    CONSTRAINT fk_terminal FOREIGN KEY (terminal_id) REFERENCES terminales(terminal_id),
    CONSTRAINT fk_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(empresa_id),
    CONSTRAINT fk_ciudad_origen FOREIGN KEY (origen_ciudad_id) REFERENCES ciudades(ciudad_id),
    CONSTRAINT fk_clase_vehiculo FOREIGN KEY (clase_vehiculo_id) REFERENCES clases_vehiculo(clase_vehiculo_id)
);


-- =============================================
-- Creación de la Tabla de Agregación (Etapa 5)
-- =============================================

-- Tabla para almacenar estadísticas diarias pre-calculadas
CREATE TABLE estadisticas_diarias (
    estadistica_id SERIAL PRIMARY KEY,
    fecha_llegada_dia DATE NOT NULL,
    terminal_id INT NOT NULL,
    empresa_id INT NOT NULL,
    total_viajes INT NOT NULL,
    total_pasajeros INT NOT NULL,
    tiempo_promedio_minutos INT, -- Puede ser nulo si no hay viajes
    ultima_actualizacion TIMESTAMP NOT NULL,

    -- Claves foráneas
    CONSTRAINT fk_terminal_stats FOREIGN KEY (terminal_id) REFERENCES terminales(terminal_id),
    CONSTRAINT fk_empresa_stats FOREIGN KEY (empresa_id) REFERENCES empresas(empresa_id),

    -- Restricción para evitar duplicados de estadísticas para el mismo día, terminal y empresa
    CONSTRAINT uq_stats_dia_terminal_empresa UNIQUE (fecha_llegada_dia, terminal_id, empresa_id)
);


-- Mensaje de finalización

-- Nota: La creación de usuario y asignación de privilegios se asume realizada
-- durante la configuración del servidor de base de datos (usuario 'postgres' en Docker).
-- Los índices en claves foráneas son creados automáticamente por PostgreSQL.
