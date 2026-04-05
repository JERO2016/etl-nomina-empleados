# ETL Nómina Empleados

Pipeline ETL desarrollado en Python para el procesamiento de catálogos de empleados con cálculo automático de conceptos de nómina conforme a la legislación laboral mexicana.

---

## ¿Qué hace este proyecto?

Lee un archivo CSV con datos de empleados, aplica transformaciones y cálculos de nómina, y genera los resultados en formato CSV y JSON listos para consumo por otros sistemas.

Las tres fases del pipeline:

- **Extract** — lee el catálogo de empleados desde un archivo CSV
- **Transform** — limpia los datos y calcula conceptos de nómina
- **Load** — exporta los resultados en CSV y JSON

---

## Conceptos de nómina calculados

| Concepto | Fórmula |
|---|---|
| Salario Diario (SD) | `sueldo_mensual / 30` |
| Factor de Integración (FI) | `1 + (días_aguinaldo / 365) + (12 / 365) × prima_vacacional` |
| Salario Diario Integrado (SDI) | `SD × FI` |
| Antigüedad | Años completos desde fecha de alta |

Valores base utilizados: 15 días de aguinaldo y 25% de prima vacacional (mínimos de ley, LFT).

---

## Estructura del proyecto

etl-nomina-empleados/
├── data/
│   └── empleados.csv          # Catálogo de empleados (entrada)
├── src/
│   └── etl.py                 # Script principal del pipeline
├── output/                    # Archivos generados (ignorados por Git)
│   ├── empleados_procesados.csv
│   └── empleados_procesados.json
├── .gitignore
└── README.md

---

## Cómo ejecutar el proyecto

### Requisitos previos

- Python 3.9+
- pip

### Instalación
```bash
git clone https://github.com/JERO2016/etl-nomina-empleados.git
cd etl-nomina-empleados
pip install pandas
```

### Ejecución
```bash
python src/etl.py
```

### Resultado esperado

==================================================
ETL - Nómina Empleados
[EXTRACT] Leyendo archivo: data\empleados.csv
[EXTRACT] 10 registros encontrados.
[TRANSFORM] Iniciando transformación de datos...
[TRANSFORM] Transformación completada. 10 registros procesados.
[LOAD] Guardando resultados en: output
[LOAD] CSV guardado: output\empleados_procesados.csv
[LOAD] JSON guardado: output\empleados_procesados.json
ETL completado exitosamente.


---

## Stack

- Python 3.9+
- pandas
- json (librería estándar)
- os (librería estándar)

---

## Autor

Jesús Emmanuel Ramírez Orozco — [JERO2016](https://github.com/JERO2016)