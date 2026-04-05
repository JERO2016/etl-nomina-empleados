import pandas as pd
import json
import os
from datetime import datetime

# ============================================================
# EXTRACT — Leer el archivo CSV
# ============================================================

def extract(filepath):
    """
    Lee el archivo CSV de empleados y lo convierte en un DataFrame.
    Un DataFrame es una tabla de datos que pandas puede manipular.
    """
    print(f"[EXTRACT] Leyendo archivo: {filepath}")
    df = pd.read_csv(filepath)
    print(f"[EXTRACT] {len(df)} registros encontrados.")
    return df


# ============================================================
# TRANSFORM — Limpiar y enriquecer los datos
# ============================================================

def transform(df):
    """
    Limpia los datos y calcula campos nuevos:
    - Elimina filas con datos incompletos
    - Calcula el Salario Diario (SD)
    - Calcula el Factor de Integración (FI)
    - Calcula el Salario Diario Integrado (SDI)
    - Calcula la antigüedad en años
    """
    print("[TRANSFORM] Iniciando transformación de datos...")

    # Eliminar filas con cualquier valor vacío
    df = df.dropna()

    # Convertir la columna fecha_alta a tipo fecha
    df["fecha_alta"] = pd.to_datetime(df["fecha_alta"])

    # Calcular antigüedad en años desde la fecha de alta hasta hoy
    hoy = datetime.today()
    df["antiguedad_años"] = df["fecha_alta"].apply(
        lambda f: (hoy - f).days // 365
    )

    # Calcular Salario Diario (SD)
    df["salario_diario"] = df["sueldo_mensual"] / 30

    # Calcular Factor de Integración (FI)
    # Fórmula: 1 + (días_aguinaldo / 365) + (12 / 365) * prima_vacacional
    dias_aguinaldo = 15
    prima_vacacional = 0.25
    df["factor_integracion"] = (
        1 + (dias_aguinaldo / 365) + (12 / 365) * prima_vacacional
    )

    # Calcular Salario Diario Integrado (SDI)
    df["sdi"] = df["salario_diario"] * df["factor_integracion"]

    # Redondear a 2 decimales
    df["salario_diario"] = df["salario_diario"].round(2)
    df["factor_integracion"] = df["factor_integracion"].round(4)
    df["sdi"] = df["sdi"].round(2)

    print(f"[TRANSFORM] Transformación completada. {len(df)} registros procesados.")
    return df


# ============================================================
# LOAD — Guardar el resultado
# ============================================================

def load(df, output_path):
    """
    Guarda el DataFrame procesado en dos formatos:
    - CSV con todos los campos calculados
    - JSON para consumo por otras aplicaciones
    """
    print(f"[LOAD] Guardando resultados en: {output_path}")

    # Guardar como CSV
    csv_path = os.path.join(output_path, "empleados_procesados.csv")
    df.to_csv(csv_path, index=False)
    print(f"[LOAD] CSV guardado: {csv_path}")

    # Guardar como JSON
    json_path = os.path.join(output_path, "empleados_procesados.json")
    df.to_json(json_path, orient="records", indent=2, date_format="iso", force_ascii=False)
    print(f"[LOAD] JSON guardado: {json_path}")


# ============================================================
# MAIN — Orquesta las tres fases
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("ETL - Nómina Empleados")
    print("=" * 50)

    # Rutas
    input_path = os.path.join("data", "empleados.csv")
    output_path = "output"

    # Ejecutar las tres fases
    df_raw = extract(input_path)
    df_transformed = transform(df_raw)
    load(df_transformed, output_path)

    print("=" * 50)
    print("ETL completado exitosamente.")
    print("=" * 50)