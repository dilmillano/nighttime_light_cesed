# Nighttime Light Data

Repositorio de datos y código para el análisis de luces nocturnas (*nighttime lights*, NTL) a nivel municipal en Colombia. Cubre tres fuentes distintas: datos oficiales EOG (DMSP 1992–2013 y VIIRS 2012–2021), la serie armonizada de Li et al. (2020) (1992–2024) y la serie LRCC-DVNL de Zhong et al. (2025) (1992–2022).

---

## Estructura

```
nighttime_light/
├── code/
│   └── 01_process_ntl.R                   ← Script de procesamiento
├── output/
│   └── stats/
│       ├── stats_EOG_DMSP.csv             ← NTL promedio por municipio, DMSP 1992-2013
│       ├── stats_EOG_VIIRS.csv            ← NTL promedio por municipio, VIIRS 2012-2021
│       ├── stats_Li2020.csv               ← NTL promedio por municipio, Li2020 1992-2024
│       └── stats_Zhong2025.csv            ← NTL promedio por municipio, Zhong2025 1992-2022
└── README.md
```

Los siguientes archivos **no están en este repositorio** por su tamaño y se almacenan en OneDrive:

| Carpeta | Contenido | Por qué no está en GitHub |
|---------|-----------|---------------------------|
| `input/ntl/` | Rasters globales crudos (DMSP, VIIRS, Li2020, Zhong2025) | ~40 GB |
| `input/geo/` | Shapefile de municipios Colombia (`MPIOS_limpio.shp`) | Datos DANE |
| `output/clipped/` | TIFs recortados a Colombia (96 archivos, uno por año/fuente) | ~2 GB |
| `output/stats/*.shp` | Shapefiles con estadísticas por municipio | Requieren 5 archivos asociados; los CSVs son equivalentes y más portables |

Para reproducir el procesamiento desde cero, descarga los datos crudos desde los enlaces de cada fuente (ver sección **Fuentes**) y ajusta la variable `base_dir` en el script.

---

## Código

**Script:** `code/01_process_ntl.R`

Para cada fuente, el script:
1. Descomprime archivos `.gz` (VIIRS) por streaming sin cargar en RAM
2. Reproyecta al sistema de referencia correcto
3. Recorta cada raster global al extent de Colombia (`MPIOS_limpio.shp`)
4. Calcula el promedio de NTL por municipio (*zonal statistics*)
5. Exporta TIFs comprimidos en `output/clipped/` y CSVs + shapefiles en `output/stats/`

Los pasos 2–4 corren en paralelo sobre todos los años de cada fuente (12 workers). Optimizado para: 48 GB RAM · AMD Ryzen 7 7840U (16 threads).

**Paquetes R requeridos:** `terra`, `sf`, `dplyr`, `stringr`, `tidyr`, `R.utils`, `future`, `future.apply`

**Outputs generados:**

| Archivo | Descripción |
|---------|-------------|
| `output/clipped/EOG_DMSP/` | 22 TIFs DMSP recortados (1992–2013) |
| `output/clipped/EOG_VIIRS/` | 10 TIFs VIIRS recortados (2012–2021) |
| `output/clipped/Li2020/` | 33 TIFs armonizados recortados (1992–2024) |
| `output/clipped/Zhong2025/` | 31 TIFs LRCC-DVNL recortados (1992–2022) |
| `output/stats/mpios_ntl_*.shp` | Shapefile por fuente con columna `ntl_YYYY` por municipio |
| `output/stats/stats_*.csv` | Tabla equivalente en formato CSV — 1 122 municipios × N años |

---

## Fuentes

### Fuente 1 — EOG Elvidge: DMSP-OLS y VIIRS-DNB

**Carpeta:** `input/ntl/EOG_Elvidge_DMSP-VIIRS/`
**Institución:** Earth Observation Group (EOG), Payne Institute, Colorado School of Mines
**Descarga:** https://eogdata.mines.edu
**Cobertura:** DMSP 1992–2013 | VIIRS 2012–2021

Son los datos **oficiales y sin procesar** de los que derivan las otras dos fuentes. Provienen de dos sensores con características muy distintas:

#### DMSP-OLS — Defense Meteorological Satellite Program

El programa DMSP operó **seis satélites distintos** entre 1992 y 2013 (F10, F12, F14, F15, F16, F18). Cada uno tiene su propia calibración y período de operación.

| Satélite | Período | Nota |
|----------|---------|------|
| F10 | 1992–1994 | Primer satélite de la serie |
| F12 | 1994–1996 | |
| F14 | 1997–2003 | |
| F15 | 2000–2007 | Órbita derivó a amanecer/ocaso desde ~2005 |
| F16 | 2004–2009 | También sufrió deriva orbital |
| F18 | 2010–2013 | Último satélite operacional |

> Varios años tienen datos de dos satélites simultáneamente (1994, 2000–2007). En este repositorio se usa el satélite de número mayor (más nuevo) para cada año.

**Cómo mide la luz:**
- Sensor de escaneo de línea (*whisk-broom*): barre el suelo en franjas mientras avanza
- **Sin calibración radiométrica a bordo**: la sensibilidad cambia con el tiempo y entre satélites sin corrección automática
- Produce valores **DN** (*Digital Number*), enteros entre 0 y 63 — escala arbitraria sin unidades físicas
- Frecuente **saturación** en núcleos urbanos: el detector llega a DN = 63 y pierde información real de ciudades brillantes
- Pronunciado **bloom**: la luz se derrama sobre píxeles vecinos, haciendo que las ciudades aparezcan más grandes

**Nombre de archivo:** `F##YYYY.v4b.global.intercal.stable_lights.avg_vis.tif`

| Componente | Significado |
|------------|-------------|
| `F##` | Número del satélite DMSP |
| `YYYY` | Año |
| `v4b` | Versión 4b — estándar actual de la serie histórica |
| `intercal` | Inter-calibrado: corrige diferencias entre satélites usando coeficientes empíricos de años de solapamiento |
| `stable_lights` | Luces estables: filtrados incendios, explosiones y fuentes efímeras; fondo = 0 |
| `avg_vis` | Promedio anual del canal visible (0.5–0.9 μm), filtradas nubes, luz solar y lunar |

**Procesamiento del producto:**
1. Uso del 50% central del swath para mejor geolocalización
2. Filtrado solar (ángulo elevación > −6°), lunar y de nubes (bandas térmicas + NCEP)
3. Filtrado de auroras boreales al norte de 45°N
4. Composición anual promediada a 30 arc-segundos (~1 km)
5. Inter-calibración entre satélites con coeficientes empíricos

---

#### VIIRS-DNB — Visible Infrared Imaging Radiometer Suite

El satélite **Suomi NPP**, lanzado en octubre de 2011, representa un salto tecnológico mayor frente al DMSP.

**Cómo mide la luz:**
- Sensor de empuje (*push-broom*): fila fija de detectores, sin partes móviles — mayor estabilidad
- **Calibración automática en cada pasada**: los valores de hoy son directamente comparables con los de hace años
- Produce **radiancias físicas** en nW/cm²/sr — unidad absoluta comparable entre instrumentos
- Rango dinámico **7 órdenes de magnitud mayor** que DMSP: detecta desde una fogata aislada hasta el centro de Tokio sin saturación
- **Bloom significativamente menor**: resolución de ~463 m vs ~1 km de DMSP

**Nombre de archivo:** `VNL_v21_npp_YYYY_global_vcmslcfg_c########.average.dat.tif.gz`

| Componente | Significado |
|------------|-------------|
| `VNL` | VIIRS Nighttime Lights |
| `v21` | Versión 2.1 del producto anual compuesto |
| `npp` | Satélite Suomi NPP |
| `YYYY` | Año (o `201204-201212` para 2012 parcial) |
| `vcmslcfg` | Stray light corregido algorítmicamente — mejor cobertura polar (equivalente a `vcmcfg` para Colombia) |
| `average` | Promedio anual de los compuestos mensuales |
| `.tif.gz` | GeoTIFF comprimido — debe descomprimirse antes de usar |

**Procesamiento del producto:**
1. Composiciones mensuales: filtrado de luz solar, lunar y nubosidad
2. Filtrado de valores atípicos con mediana de 12 meses (elimina incendios y eventos transitorios)
3. Composición anual como promedio de los compuestos mensuales filtrados

**Años disponibles:** 2012 (parcial, abr–dic), 2013–2021

---

#### ¿Por qué no son directamente comparables?

| Característica | DMSP-OLS | VIIRS-DNB |
|----------------|----------|-----------|
| Unidad | DN (0–63, relativo) | nW/cm²/sr (físico absoluto) |
| Resolución | 30 arc-seg (~1 km) | 15 arc-seg (~463 m) |
| Saturación urbana | Frecuente (DN = 63 es techo) | Prácticamente nula |
| Calibración a bordo | Ninguna | Sí, automática |
| Rango dinámico | Bajo | 7 órdenes de magnitud mayor |

Concatenar ambas series sin corrección genera un **salto artificial en 2014** que no refleja cambios reales. Las fuentes 2 y 3 resuelven esto.

**Citar:**
> Elvidge et al. (2021). Annual Time Series of Global VIIRS Nighttime Lights. *Remote Sensing*, 13(5), 922. https://doi.org/10.3390/rs13050922

---

### Fuente 2 — Li et al. (2020): Serie armonizada DMSP + VIIRS

**Carpeta:** `input/ntl/Li2020_Harmonized_DMSP-VIIRS/`
**Descarga:** https://figshare.com/articles/dataset/Harmonization_of_DMSP_and_VIIRS_nighttime_light_data_from_1992-2018_at_the_global_scale/9828827
**DOI artículo:** https://doi.org/10.1038/s41597-020-0510-y
**Cobertura:** 1992–2024 (version_10) | 1992–2021 (version_8)
**Resolución:** 30 arc-seg (~1 km) | **Unidad:** DN armonizado (0–63) | **Proyección:** WGS84

Serie continua que resuelve la incompatibilidad entre sensores produciendo una sola escala homogénea comparable año a año desde 1992.

#### Método de armonización (3 pasos)

**Paso 1 — Inter-calibración interna de DMSP (→ `calDMSP`)**
Los seis satélites DMSP son inconsistentes entre sí: el mismo lugar en el mismo año medido por dos satélites distintos da valores distintos. Este paso aplica coeficientes empíricos derivados de los años de solapamiento entre satélites para llevar toda la serie 1992–2013 a una escala común.

**Paso 2 — Conversión VIIRS → escala DMSP (→ `simVIIRS`)**
Las radiancias VIIRS (nW/cm²/sr) se transforman a DN equivalentes DMSP mediante una **función sigmoide** ajustada con los datos del año 2013, donde ambos sensores operaron simultáneamente. La sigmoide captura la relación no-lineal entre ambas escalas, incluyendo la saturación en áreas brillantes:

```
DN_sim = 63 / (1 + exp(−(a × radiancia_VIIRS + b)))
```

Los parámetros *a* y *b* se derivan empíricamente del año 2013 y se aplican a todos los años VIIRS.

**Paso 3 — Empalme y validación**
Se une `calDMSP` (1992–2013) con `simVIIRS` (2014+) y se verifica la continuidad temporal en el año de transición.

#### Versiones y archivos

```
Li2020_Harmonized_DMSP-VIIRS/
├── version_8/    ← publicación original (2020), cubre 1992–2021
└── version_10/   ← versión extendida y actualizada, cubre 1992–2024  ← usar esta
```

| Archivo | Qué es |
|---------|--------|
| `Harmonized_DN_NTL_YYYY_calDMSP.tif` | DMSP inter-calibrado internamente — años 1992–2013 |
| `Harmonized_DN_NTL_YYYY_simVIIRS.tif` | VIIRS convertido a escala DMSP — años 2014–2024 |
| `DN_NTL_2013_simVIIRS.tif` | 2013 en formato simVIIRS (año puente para verificar continuidad) |

#### Limitaciones
- Los parámetros del sigmoide se derivan de un solo año (2013); se asume estabilidad en el tiempo
- DN < 20 tienen mayor incertidumbre en zonas de baja luminosidad
- La saturación DMSP en ciudades grandes no se elimina completamente

**Citar:**
> Li, X., Zhou, Y., Zhao, M., & Zhao, X. (2020). A harmonized global nighttime light dataset 1992–2018. *Scientific Data*, 7, 168. https://doi.org/10.1038/s41597-020-0510-y

---

### Fuente 3 — Zhong et al. (2025): LRCC-DVNL

**Carpeta:** `input/ntl/Zhong2025_LRCC-DVNL/`
**Descarga:** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/15IKI5
**DOI artículo:** https://doi.org/10.1038/s41597-025-05246-8
**Cobertura:** 1992–2022 | **Resolución:** 1 km | **Unidad:** DN (0–63)
**Proyección:** WGS 1984 Equal Earth Greenwich (EPSG:8857) — distinta a las fuentes anteriores

Serie de largo plazo diseñada específicamente para **zonas de baja luminosidad** (zonas rurales, países de bajos ingresos, áreas protegidas), que representan ~80% de la superficie terrestre y son subestimadas por otros productos. Validada con correlación R² = 0.885 con PIB mundial.

#### Glosario de siglas

| Sigla | Nombre completo | Qué es |
|-------|----------------|--------|
| **DVNL** | DMSP-like VIIRS Nighttime Lights | VIIRS convertido a escala DMSP (DN 0–63) mediante red neuronal ResU-Net |
| **C-DVNL** | Corrected DVNL | DVNL con vacíos en latitudes altas reparados y extendido hasta 2022 |
| **LR-DVNL** | Linear-trend-Registered DVNL | C-DVNL + DMSP alineados píxel a píxel por patrones de cambio temporal |
| **LRCC-DVNL** | LR-DVNL + Continuity Correction | Producto final: LR-DVNL con corrección anual de continuidad (LACC) |
| **LACC** | Linear-trend Annual Continuity Correction | Corrección anual que suaviza la transición 2013 preservando eventos reales |

#### Metodología en 4 etapas

**Etapa 1 → C-DVNL**
Conversión VIIRS → escala DMSP usando una red neuronal convolucional **ResU-Net** (más sofisticada que la sigmoide de Li2020). Repara vacíos de datos en latitudes altas del año 2013 y extiende la serie hasta 2022.

**Etapa 2 → LR-DVNL**
Alineación temporal entre DMSP (1992–2013) y C-DVNL (2014–2022) clasificando cada píxel del globo en uno de **5 patrones de cambio**:
- Constante · Crecimiento · Oscuro-a-iluminado · Declive · Iluminado-a-oscuro

**Etapa 3 → LRCC-DVNL**
Aplicación de la corrección LACC para eliminar la discontinuidad artificial del año 2013, preservando sensibilidad a eventos reales (crisis financiera 2008, apagones del conflicto sirio 2012–2015).

**Etapa 4 — Validación**
Correlación con PIB mundial R² = 0.885, superior a Li2020 y Chen-NTL, especialmente en zonas de baja luminosidad.

#### Productos incluidos

| Carpeta / Archivo | Qué contiene | Estado |
|-------------------|-------------|--------|
| `LRCC-DVNL data/LACC_YYYY.tif` | Serie principal 1992–2022 — **usar este** | Listo para usar |
| `Calibrated DVNL files/C_DVNL YYYY.tif.7z` | Serie C-DVNL 2013–2022 | Requiere 7-Zip |
| `Cloud Raster File/LRCC_DVNL_1992_2022.crf.7z` | Serie completa en formato ArcGIS Pro | Requiere 7-Zip + ArcGIS |

> **Nota técnica:** Este dataset usa la proyección Equal Earth (EPSG:8857) con un datum definido como "Unknown" en los metadatos del TIF, lo que impide la reproyección directa en PROJ. El script `01_process_ntl.R` corrige esto forzando `EPSG:8857` antes de procesar.

**Citar:**
> Zhong et al. (2025). Global nighttime light dataset from 1992 to 2022 with focus on low-light areas. *Scientific Data*. https://doi.org/10.1038/s41597-025-05246-8
