-- =====================================================================
-- Script de Lógica Almacenada (Funciones y Procedimientos - Etapa 5)
-- Base de Datos: PostgreSQL
-- Proyecto: Análisis de Transporte de Pasajeros a Medellín
-- =====================================================================

-- Limpieza inicial de los objetos a crear
DROP FUNCTION IF EXISTS calcular_duracion_promedio(DATE, INT, INT);
DROP PROCEDURE IF EXISTS actualizar_estadisticas_diarias(DATE);

-- =====================================================================
-- Función: calcular_duracion_promedio
-- =====================================================================
-- Objetivo: Calcular el tiempo promedio de viaje en minutos para una fecha,
-- terminal y empresa específicas.
-- Parámetros:
--   p_fecha: La fecha de llegada para la cual se calcula el promedio.
--   p_terminal_id: El ID de la terminal.
--   p_empresa_id: El ID de la empresa.
-- Retorna: El promedio de duración en minutos (INT) o NULL si no hay viajes.

CREATE OR REPLACE FUNCTION calcular_duracion_promedio(
    p_fecha DATE,
    p_terminal_id INT,
    p_empresa_id INT
) RETURNS INT AS $$
DECLARE
    v_duracion_promedio INT;
BEGIN
    SELECT
        -- Calcula la diferencia en épocas (segundos) y convierte a minutos
        -- Se usa COALESCE para manejar el caso de que no haya viajes y evitar un NULL dentro de AVG
        ROUND(AVG(EXTRACT(EPOCH FROM (fecha_llegada - fecha_salida)) / 60))::INT
    INTO
        v_duracion_promedio
    FROM
        viajes
    WHERE
        DATE(fecha_llegada) = p_fecha
        AND terminal_id = p_terminal_id
        AND empresa_id = p_empresa_id;

    RETURN v_duracion_promedio;
END;
$$ LANGUAGE plpgsql;


-- =====================================================================
-- Procedimiento Almacenado: actualizar_estadisticas_diarias
-- =====================================================================
-- Objetivo: Calcular y almacenar (o actualizar) las estadísticas diarias
-- de viajes para una fecha específica.
-- Parámetros:
--   p_fecha_proceso: La fecha para la cual se generarán las estadísticas.

CREATE OR REPLACE PROCEDURE actualizar_estadisticas_diarias(p_fecha_proceso DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Cursor para iterar sobre cada combinación de terminal y empresa que tuvo actividad en la fecha dada
    v_rec RECORD;
    c_grupos CURSOR FOR
        SELECT DISTINCT terminal_id, empresa_id
        FROM viajes
        WHERE DATE(fecha_llegada) = p_fecha_proceso;
BEGIN
    -- Abrir el cursor
    OPEN c_grupos;

    LOOP
        -- Obtener la siguiente combinación de terminal/empresa
        FETCH c_grupos INTO v_rec;
        EXIT WHEN NOT FOUND;

        -- Declaración de variables para almacenar las métricas calculadas
        DECLARE
            v_total_viajes INT;
            v_total_pasajeros INT;
            v_duracion_promedio INT;
        BEGIN
            -- Calcular total de viajes y pasajeros para el grupo actual
            SELECT
                COUNT(*),
                SUM(cantidad_pasajeros)
            INTO
                v_total_viajes, v_total_pasajeros
            FROM
                viajes
            WHERE
                DATE(fecha_llegada) = p_fecha_proceso
                AND terminal_id = v_rec.terminal_id
                AND empresa_id = v_rec.empresa_id;

            -- Usar la función para calcular la duración promedio
            v_duracion_promedio := calcular_duracion_promedio(p_fecha_proceso, v_rec.terminal_id, v_rec.empresa_id);

            -- Insertar o actualizar el registro en la tabla de estadísticas
            -- ON CONFLICT es una característica de PostgreSQL que simplifica el "UPSERT"
            INSERT INTO estadisticas_diarias (
                fecha_llegada_dia,
                terminal_id,
                empresa_id,
                total_viajes,
                total_pasajeros,
                tiempo_promedio_minutos,
                ultima_actualizacion
            )
            VALUES (
                p_fecha_proceso,
                v_rec.terminal_id,
                v_rec.empresa_id,
                v_total_viajes,
                v_total_pasajeros,
                v_duracion_promedio,
                NOW() -- Fecha y hora actual
            )
            ON CONFLICT (fecha_llegada_dia, terminal_id, empresa_id)
            DO UPDATE SET
                total_viajes = EXCLUDED.total_viajes,
                total_pasajeros = EXCLUDED.total_pasajeros,
                tiempo_promedio_minutos = EXCLUDED.tiempo_promedio_minutos,
                ultima_actualizacion = EXCLUDED.ultima_actualizacion;
        END;
    END LOOP;

    -- Cerrar el cursor
    CLOSE c_grupos;

    -- Confirmar la transacción (en procedimientos, la transacción se maneja implícitamente)
    RAISE NOTICE 'Proceso de actualización de estadísticas para la fecha % completado.', p_fecha_proceso;
END;
$$;

-- Ejemplo de cómo llamar al procedimiento para un día específico:
-- CALL actualizar_estadisticas_diarias('2024-12-25');

-- Para verificar los resultados:
-- SELECT * FROM estadisticas_diarias WHERE fecha_llegada_dia = '2024-12-25' ORDER BY total_pasajeros DESC;
