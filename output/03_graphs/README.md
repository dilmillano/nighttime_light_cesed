# Gráficas NTL por grupo PNIS

Generadas por `code/02_graph_NTL_PNIS.R`.

## Qué muestran

Cada gráfica compara la evolución temporal del promedio de luces nocturnas (NTL) entre dos grupos de municipios de Colombia, clasificados según su probabilidad de presencia del Programa Nacional Integral de Sustitución de Cultivos Ilícitos (PNIS):

- **Alta probabilidad PNIS** (azul): 109 municipios con PNIS = 1
- **Baja probabilidad PNIS** (rojo): 1 013 municipios con PNIS ≠ 1

El valor graficado es el **promedio de las medias municipales** por grupo y año — es decir, primero se calcula la media NTL de cada municipio (zonal statistics sobre sus píxeles), y luego se promedia entre todos los municipios del grupo.

## Fuentes de datos

| Prefijo | Fuente | Cobertura | Unidad |
|---------|--------|-----------|--------|
| `EOG_DMSP` | EOG Elvidge — DMSP-OLS | 1992–2013 | DN (0–63) |
| `EOG_VIIRS` | EOG Elvidge — VIIRS-DNB | 2012–2021 | nW/cm²/sr |
| `Li2020_v8` | Li et al. (2020) — versión 8 | 1992–2021 | DN armonizado |
| `Li2020_v10` | Li et al. (2020) — versión 10 | 1992–2024 | DN armonizado |
| `Zhong2025` | Zhong et al. (2025) — LRCC-DVNL | 1992–2022 | DN (0–63) |

## Tipos de gráfica

Cada fuente produce 3 gráficas:

| Sufijo | Descripción | Eje Y | Línea de referencia |
|--------|-------------|-------|---------------------|
| `_raw` | Valores crudos | Promedio NTL en unidades originales | — |
| `_norm{AÑO}` | Índice normalizado | Índice con año base = 100 | y = 100 |
| `_log{AÑO}` | Log-normalizado | log(NTL_t) − log(NTL_base) | y = 0 |

**Año base de normalización:**
- EOG DMSP, Li2020 v8, Li2020 v10, Zhong2025 → base **2005**
- EOG VIIRS → base **2013** (primer año completo disponible)

## Nomenclatura de archivos

```
{##}_{FUENTE}_{tipo}.png

Ejemplos:
  07_Li2020_v8_raw.png        → Li2020 v8, valores crudos
  08_Li2020_v8_norm2005.png   → Li2020 v8, índice 2005 = 100
  09_Li2020_v8_log2005.png    → Li2020 v8, log-normalizado base 2005
  10_Li2020_v10_raw.png       → Li2020 v10, valores crudos
```

## Lista completa de archivos

| Archivo | Fuente | Tipo |
|---------|--------|------|
| `01_EOG_DMSP_raw.png` | EOG DMSP | Valores crudos |
| `02_EOG_DMSP_norm2005.png` | EOG DMSP | Índice 2005=100 |
| `03_EOG_DMSP_log2005.png` | EOG DMSP | Log-norm base 2005 |
| `04_EOG_VIIRS_raw.png` | EOG VIIRS | Valores crudos |
| `05_EOG_VIIRS_norm2013.png` | EOG VIIRS | Índice 2013=100 |
| `06_EOG_VIIRS_log2013.png` | EOG VIIRS | Log-norm base 2013 |
| `07_Li2020_v8_raw.png` | Li2020 v8 | Valores crudos |
| `08_Li2020_v8_norm2005.png` | Li2020 v8 | Índice 2005=100 |
| `09_Li2020_v8_log2005.png` | Li2020 v8 | Log-norm base 2005 |
| `10_Li2020_v10_raw.png` | Li2020 v10 | Valores crudos |
| `11_Li2020_v10_norm2005.png` | Li2020 v10 | Índice 2005=100 |
| `12_Li2020_v10_log2005.png` | Li2020 v10 | Log-norm base 2005 |
| `13_Zhong2025_raw.png` | Zhong2025 | Valores crudos |
| `14_Zhong2025_norm2005.png` | Zhong2025 | Índice 2005=100 |
| `15_Zhong2025_log2005.png` | Zhong2025 | Log-norm base 2005 |
