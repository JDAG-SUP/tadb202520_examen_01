-- =====================================================================
-- Script de Consultas de Análisis (Etapa 4)
-- Base de Datos: PostgreSQL
-- Proyecto: Análisis de Transporte de Pasajeros a Medellín
-- =====================================================================

-- Dimensión de Análisis: Operacional y de Negocio

-- =====================================================================
-- Consulta 1: Ranking de Empresas por Terminal y Participación de Mercado
-- =====================================================================
-- Objetivo: Identificar las empresas de transporte más importantes en cada
-- terminal (Norte y Sur) según el número de pasajeros transportados.
-- Se calcula el total de viajes, total de pasajeros y la cuota de mercado
-- porcentual de cada empresa dentro de su respectiva terminal.

-- CTE (Common Table Expression) y Funciones Ventana (Window Functions)

WITH EstadisticasEmpresaTerminal AS (
    -- 1. CTE para agregar los datos básicos por empresa y terminal
    SELECT
        t.nombre AS nombre_terminal,
        e.nombre AS nombre_empresa,
        COUNT(v.viaje_id) AS total_viajes,
        SUM(v.cantidad_pasajeros) AS total_pasajeros
    FROM
        viajes v
    JOIN
        terminales t ON v.terminal_id = t.terminal_id
    JOIN
        empresas e ON v.empresa_id = e.empresa_id
    GROUP BY
        t.nombre, e.nombre
),
CalculoCuotaMercado AS (
    -- 2. CTE para calcular la cuota de mercado usando funciones de ventana
    SELECT
        nombre_terminal,
        nombre_empresa,
        total_viajes,
        total_pasajeros,
        -- Calculamos el total de pasajeros por terminal para poder sacar el porcentaje
        SUM(total_pasajeros) OVER (PARTITION BY nombre_terminal) AS pasajeros_totales_terminal,
        -- Asignamos un ranking a cada empresa dentro de su terminal
        ROW_NUMBER() OVER (PARTITION BY nombre_terminal ORDER BY total_pasajeros DESC) as ranking_empresa
    FROM
        EstadisticasEmpresaTerminal
)
-- 3. Consulta final que presenta los resultados formateados
SELECT
    c.nombre_terminal,
    c.ranking_empresa,
    c.nombre_empresa,
    c.total_viajes,
    c.total_pasajeros,
    -- Calculamos el porcentaje de participación de mercado
    ROUND((c.total_pasajeros::DECIMAL / c.pasajeros_totales_terminal) * 100, 2) AS cuota_mercado_porcentaje
FROM
    CalculoCuotaMercado c
ORDER BY
    c.nombre_terminal, c.ranking_empresa;


-- =====================================================================
-- Consulta 2: Análisis de Rutas Más Concurridas en Horas Pico vs. Horas Valle
-- =====================================================================
-- Objetivo: Identificar las rutas (origen-destino) que experimentan el mayor
-- flujo de pasajeros durante las horas pico y comparar su rendimiento con
-- las horas de menor demanda (valle).
--
-- Horas Pico: 06:00-09:59 y 16:00-19:59
-- Horas Valle: El resto del día

-- CTE (Common Table Expression) y Funciones Ventana (Window Functions)

WITH ViajesConHorario AS (
    -- 1. CTE para extraer la hora de llegada y clasificarla en franjas horarias
    SELECT
        c.nombre AS nombre_ciudad_origen,
        t.nombre AS nombre_terminal,
        v.cantidad_pasajeros,
        CASE
            WHEN EXTRACT(HOUR FROM v.fecha_llegada) BETWEEN 6 AND 9 THEN 'Pico'
            WHEN EXTRACT(HOUR FROM v.fecha_llegada) BETWEEN 16 AND 19 THEN 'Pico'
            ELSE 'Valle'
        END AS franja_horaria
    FROM
        viajes v
    JOIN
        ciudades c ON v.origen_ciudad_id = c.ciudad_id
    JOIN
        terminales t ON v.terminal_id = t.terminal_id
),
RutasAgregadas AS (
    -- 2. CTE para agregar el total de pasajeros por ruta y franja horaria
    SELECT
        nombre_ciudad_origen,
        nombre_terminal,
        franja_horaria,
        SUM(cantidad_pasajeros) AS total_pasajeros
    FROM
        ViajesConHorario
    GROUP BY
        nombre_ciudad_origen,
        nombre_terminal,
        franja_horaria
),
RutasPivote AS (
    -- 3. CTE para pivotar los datos y tener el tráfico de horas pico y valle en columnas separadas
    SELECT
        nombre_ciudad_origen,
        nombre_terminal,
        SUM(CASE WHEN franja_horaria = 'Pico' THEN total_pasajeros ELSE 0 END) AS pasajeros_pico,
        SUM(CASE WHEN franja_horaria = 'Valle' THEN total_pasajeros ELSE 0 END) AS pasajeros_valle,
        SUM(total_pasajeros) AS pasajeros_totales
    FROM
        RutasAgregadas
    GROUP BY
        nombre_ciudad_origen,
        nombre_terminal
)
-- 4. Consulta final que presenta los resultados, usando una función de ventana para el ranking
SELECT
    RANK() OVER (ORDER BY pasajeros_totales DESC) AS ranking_ruta,
    nombre_ciudad_origen,
    nombre_terminal,
    pasajeros_totales,
    pasajeros_pico,
    pasajeros_valle,
    -- Calcula la diferencia de pasajeros entre los periodos
    (pasajeros_pico - pasajeros_valle) AS diferencia_pico_vs_valle
FROM
    RutasPivote
ORDER BY
    ranking_ruta
LIMIT 100; -- Limitar a las 100 rutas más importantes para un reporte conciso

