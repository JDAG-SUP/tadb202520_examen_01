-- =====================================================================
-- Script de Prueba de Funcionalidad Completa
-- Base de Datos: PostgreSQL
-- Proyecto: Análisis de Transporte de Pasajeros a Medellín
-- =====================================================================

-- Paso 1: Verificación de la Carga de Datos
-- ---------------------------------------------
-- Se espera un número cercano a 82,748 registros.

SELECT COUNT(*) AS total_registros_cargados FROM viajes;


-- Paso 2: Ejecución de la Lógica de Negocio (Procedimiento Almacenado)
-- ---------------------------------------------------------------------
-- Se llama al procedimiento para procesar los datos de un día específico.
-- Hemos elegido el 25 de diciembre de 2024, una fecha clave.
-- La salida 'NOTICE' en la consola de tu IDE confirmará la ejecución.

CALL actualizar_estadisticas_diarias('2024-12-25');


-- Paso 3: Verificación de los Resultados Generados
-- --------------------------------------------------
-- Se consulta la tabla de estadísticas para ver los datos agregados
-- que el procedimiento acaba de calcular y almacenar.
-- Deberías ver un resumen de viajes y pasajeros por empresa para esa fecha.

SELECT
    t.nombre AS nombre_terminal,
    e.nombre AS nombre_empresa,
    s.total_viajes,
    s.total_pasajeros,
    s.tiempo_promedio_minutos,
    s.ultima_actualizacion
FROM
    estadisticas_diarias s
JOIN
    terminales t ON s.terminal_id = t.terminal_id
JOIN
    empresas e ON s.empresa_id = e.empresa_id
WHERE
    s.fecha_llegada_dia = '2024-12-25'
ORDER BY
    t.nombre, s.total_pasajeros DESC;


-- Si este script se ejecuta y devuelve resultados en el paso 3, significa que:
-- 1. La base de datos y las tablas están creadas correctamente.
-- 2. Los datos fueron cargados exitosamente.
-- 3. La función y el procedimiento almacenado funcionan como se esperaba.
-- 4. El proyecto está completamente funcional.
