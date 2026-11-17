
from prefect import flow, task
from pathlib import Path
import duckdb
import json
import time
from datetime import datetime


# TAREA 1: VERIFICAR EL ARCHIVO

@task(name="Verificar archivo")
def verificar_archivo_completo(ruta):

    archivo = Path(ruta)

    if not archivo.is_file():
        raise FileNotFoundError("No se encontró el archivo CSV")

    tamaño = archivo.stat().st_size

    print("Archivo:", archivo.name)
    print("Tamaño:", round(tamaño / 1_000_000_000, 2), "GB")

    return str(archivo)


# TAREA 2: VALIDAR LOS REGISTROS

@task(name="Validar registros")
def validar_archivo_completo(ruta):

    con = duckdb.connect()

    try:
        consulta = """
        SELECT
            COUNT(*) AS total_registros,
            COUNT(*) FILTER (
                WHERE MSFL_CODE IS NOT NULL
                AND MED_FECHA_INICIO IS NOT NULL
            ) AS registros_identificados,
            COUNT(*) FILTER (
                WHERE MSFL_CODE IS NULL
                OR MED_FECHA_INICIO IS NULL
            ) AS registros_incompletos
        FROM read_csv(
            ?,
            header = true,
            all_varchar = true,
            ignore_errors = true
        )
        """

        resultado = con.execute(
            consulta, [ruta]
        ).fetchone()

    finally:
        con.close()

    resumen = {
        "total_registros": int(resultado[0]),
        "registros_identificados": int(resultado[1]),
        "registros_incompletos": int(resultado[2])
    }

    # Detectar el mensaje de error del servidor
    with open(ruta, "rb") as archivo:
        archivo.seek(max(0, Path(ruta).stat().st_size - 4096))
        final = archivo.read().decode(
            "utf-8", errors="replace"
        )

    error_servidor = (
        '"Internal error"' in final
        and '"status"' in final
        and '500' in final
    )

    resumen["error_servidor_detectado"] = error_servidor

    print("\nVALIDACIÓN DEL ARCHIVO")

    for nombre, valor in resumen.items():
        print(nombre, ":", valor)

    if error_servidor:
        print(
            "ADVERTENCIA: La descarga termina "
            "con un error del servidor."
        )

    return resumen


# TAREA 3: ANALIZAR LOS CONTAMINANTES

@task(name="Analizar contaminantes")
def analizar_contaminantes_completo(ruta):

    con = duckdb.connect()

    try:
        consulta = """
        WITH datos AS (
            SELECT
                MSFL_CODE AS Contaminante,
                SIGLA_UNIDAD AS Unidad,
                TRY_CAST(
                    REPLACE(
                        TRIM(MED_CONCENTRACION_ESTANDAR),
                        ',', '.'
                    ) AS DOUBLE
                ) AS Concentracion
            FROM read_csv(
                ?,
                header = true,
                all_varchar = true,
                ignore_errors = true
            )
            WHERE MSFL_CODE IN (
                'PM2.5', 'PM10', 'NO2',
                'O3', 'SO2', 'CO', 'NO'
            )
        )
        SELECT
            Contaminante,
            Unidad,
            COUNT(*) AS Mediciones,
            COUNT(Concentracion) AS Valores_numericos,
            ROUND(AVG(Concentracion), 2) AS Promedio,
            ROUND(MEDIAN(Concentracion), 2) AS Mediana,
            ROUND(MIN(Concentracion), 2) AS Minimo,
            ROUND(MAX(Concentracion), 2) AS Maximo,
            ROUND(
                STDDEV_SAMP(Concentracion), 2
            ) AS Desviacion
        FROM datos
        GROUP BY Contaminante, Unidad
        ORDER BY Mediciones DESC
        """

        resultados = con.execute(
            consulta, [ruta]
        ).df()

    finally:
        con.close()

    if resultados.empty:
        raise ValueError(
            "No se encontraron datos de contaminantes"
        )

    print("\nANÁLISIS FINALIZADO")
    print("Contaminantes analizados:", len(resultados))

    return resultados


# TAREA 4: GUARDAR RESULTADOS

@task(name="Guardar resultados")
def guardar_resultados(
    ruta, estadisticas, calidad, segundos
):

    carpeta = Path(ruta).parent.parent / "Resultados"
    carpeta.mkdir(parents=True, exist_ok=True)

    archivo_csv = carpeta / "estadisticas_contaminantes.csv"
    archivo_json = carpeta / "resumen_procesamiento.json"

    estadisticas.to_csv(
        archivo_csv,
        index=False,
        encoding="utf-8-sig"
    )

    resumen = {
        "fecha_ejecucion": datetime.now().isoformat(),
        "archivo_original": Path(ruta).name,
        "tamano_bytes": Path(ruta).stat().st_size,
        "validacion": calidad,
        "tiempo_segundos": round(segundos, 2),
        "advertencia": (
            "La integridad de la descarga original "
            "no ha sido confirmada. "
            "Los valores extremos requieren validación."
        ),
        "metodo_lectura": (
            "DuckDB con ignore_errors=True. "
            "Los registros omitidos no se contabilizan "
            "automáticamente."
        )
    }

    with open(
        archivo_json, "w", encoding="utf-8"
    ) as archivo:
        json.dump(
            resumen,
            archivo,
            indent=4,
            ensure_ascii=False
        )

    print("\nARCHIVOS GENERADOS:")
    print(archivo_csv)
    print(archivo_json)

    return str(carpeta)


# FLUJO PRINCIPAL DE PREFECT

@flow(name="Calidad_Aire_Procesamiento_Completo")
def flujo_completo_calidad_aire(ruta):

    inicio = time.time()

    archivo = verificar_archivo_completo(ruta)

    calidad = validar_archivo_completo(archivo)

    estadisticas = analizar_contaminantes_completo(
        archivo
    )

    segundos = time.time() - inicio

    carpeta = guardar_resultados(
        archivo,
        estadisticas,
        calidad,
        segundos
    )

    print("\nPROCESAMIENTO FINALIZADO")
    print("Tiempo:", round(segundos, 2), "segundos")
    print("Resultados guardados en:", carpeta)

    return carpeta


# CREAR EL DEPLOYMENT PROGRAMADO

if __name__ == "__main__":

    ruta_csv = (
        r"C:\Users\ADMIN\OneDrive\Documentos\BigData"
        r"\Proyecto\Datos"
        r"\Calidad_del_Aire_en_Colombia_20261112.csv"
    )

    flujo_completo_calidad_aire.serve(
        name="calidad-aire-programado",
        cron="0 8 * * 1",
        parameters={"ruta": ruta_csv}
    )


