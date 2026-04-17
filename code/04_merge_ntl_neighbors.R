# =============================================================================
# 04_merge_ntl_neighbors.R
# Merge de los 5 paises vecinos con NTL Li2020 v10 (2006-2019)
# Ultima actualizacion: 2026-04-17
#
# Que hace este script:
#   1. Lee los 5 shapefiles generados en 03_process_ntl_neighbors.R
#   2. Estandariza las columnas que difieren entre paises:
#        IPUM_YYYY   -> ipum_code     (codigo IPUMS armonizado, unico global)
#        CANT/MUNI/PROV_YYYY -> muni_code  (codigo nacional original)
#      Agrega columnas de trazabilidad:
#        ipum_ref_year  -> ano censal al que corresponde el codigo IPUMS
#        muni_orig_col  -> nombre original de la columna nacional
#   3. Une los 5 paises en un solo dataset
#   4. Exporta en 3 formatos a output/04_ntl_neighbors/merge/:
#        merge_ntl_neighbors.shp   -> shapefile unido
#        merge_ntl_neighbors.csv   -> CSV plano
#        merge_ntl_neighbors.dta   -> Stata con variable labels
#   5. Genera merge_ntl_neighbors_codebook.txt con descripcion de cada variable
#
# Estructura del archivo final:
#   country | cntry_code | admin_name | ipum_code | ipum_ref_year |
#   muni_code | muni_orig_col | parent | ntl_2006 ... ntl_2019
#
# Outputs:
#   output/04_ntl_neighbors/merge/merge_ntl_neighbors.shp
#   output/04_ntl_neighbors/merge/merge_ntl_neighbors.csv
#   output/04_ntl_neighbors/merge/merge_ntl_neighbors.dta
#   output/04_ntl_neighbors/merge/merge_ntl_neighbors_codebook.txt
# =============================================================================

# --- Paquetes ----------------------------------------------------------------
required <- c("sf", "dplyr", "haven")
missing  <- required[!sapply(required, requireNamespace, quietly = TRUE)]
if (length(missing) > 0) install.packages(missing, repos = "https://cloud.r-project.org")

library(sf)
library(dplyr)
library(haven)

# =============================================================================
# 0. RUTA BASE - solo cambia USER_NAME segun el equipo
# =============================================================================
# Laptop Diana : diana
# Servidor     : d.millanorduz

USER_NAME <- "d.millanorduz"   # <-- cambiar segun equipo

BASE_DIR  <- file.path("C:/Users", USER_NAME,
                       "OneDrive - Universidad de los Andes",
                       "nighttime_light")

# =============================================================================
# 1. Rutas
# =============================================================================

IN_STATS <- file.path(BASE_DIR, "output", "04_ntl_neighbors", "stats")
OUT_DIR  <- file.path(BASE_DIR, "output", "04_ntl_neighbors", "merge")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# 2. Metadatos por pais: columnas originales y trazabilidad
# =============================================================================
# Cada entrada define como mapear las columnas especificas de ese pais
# a los nombres estandarizados del merge.
#
# ipum_col      : nombre original de la columna IPUMS en el shapefile
# muni_col      : nombre original de la columna de codigo nacional
# ipum_ref_year : ano censal al que corresponde el shapefile IPUMS
# muni_orig_col : etiqueta de trazabilidad (queda como columna en el output)

country_meta <- list(
  Ecuador   = list(shp_name    = "mpios_ntl_Ecuador_Li2020_v10.shp",
                   ipum_col    = "IPUM2010",
                   muni_col    = "CANT2010",
                   ipum_ref_year = 2010,
                   muni_orig_col = "CANT2010"),

  Venezuela = list(shp_name    = "mpios_ntl_Venezuela_Li2020_v10.shp",
                   ipum_col    = "IPUM2001",
                   muni_col    = "MUNI2001",
                   ipum_ref_year = 2001,
                   muni_orig_col = "MUNI2001"),

  Peru      = list(shp_name    = "mpios_ntl_Peru_Li2020_v10.shp",
                   ipum_col    = "IPUM2017",
                   muni_col    = "PROV2017",
                   ipum_ref_year = 2017,
                   muni_orig_col = "PROV2017"),

  Mexico    = list(shp_name    = "mpios_ntl_Mexico_Li2020_v10.shp",
                   ipum_col    = "IPUM2020",
                   muni_col    = "MUNI2020",
                   ipum_ref_year = 2020,
                   muni_orig_col = "MUNI2020"),

  Brasil    = list(shp_name    = "mpios_ntl_Brasil_Li2020_v10.shp",
                   ipum_col    = "IPUM2010",
                   muni_col    = "MUNI2010",
                   ipum_ref_year = 2010,
                   muni_orig_col = "MUNI2010")
)

# =============================================================================
# 3. Leer, estandarizar y apilar los 5 paises
# =============================================================================

cat("Leyendo y estandarizando paises...\n")

shp_list <- lapply(names(country_meta), function(cname) {

  meta <- country_meta[[cname]]
  shp  <- st_read(file.path(IN_STATS, meta$shp_name), quiet = TRUE)

  # Renombrar columnas variables a nombres estandarizados
  shp <- shp |>
    rename(
      country      = CNTRY_NAME,
      cntry_code   = CNTRY_CODE,
      admin_name   = ADMIN_NAME,
      parent       = PARENT,
      ipum_code    = !!meta$ipum_col,
      muni_code    = !!meta$muni_col
    ) |>
    mutate(
      ipum_ref_year = meta$ipum_ref_year,
      muni_orig_col = meta$muni_orig_col
    ) |>
    select(country, cntry_code, admin_name,
           ipum_code, ipum_ref_year,
           muni_code, muni_orig_col,
           parent,
           starts_with("ntl_"),
           geometry)

  cat(sprintf("  OK %-10s -> %d unidades\n", cname, nrow(shp)))
  shp
})

# Apilar todos en un solo sf
merged_sf <- bind_rows(shp_list)

cat(sprintf("\nTotal unidades: %d\n", nrow(merged_sf)))
cat(sprintf("Columnas     : %s\n\n", paste(names(merged_sf), collapse = ", ")))

# =============================================================================
# 4. Exportar shapefile
# =============================================================================

shp_out <- file.path(OUT_DIR, "merge_ntl_neighbors.shp")
st_write(merged_sf, shp_out, delete_dsn = TRUE, quiet = TRUE)
cat("Shapefile exportado: merge_ntl_neighbors.shp\n")

# =============================================================================
# 5. Exportar CSV (sin geometria)
# =============================================================================

df <- st_drop_geometry(merged_sf)
write.csv(df, file.path(OUT_DIR, "merge_ntl_neighbors.csv"), row.names = FALSE)
cat("CSV exportado      : merge_ntl_neighbors.csv\n")

# =============================================================================
# 6. Exportar Stata .dta con variable labels
# =============================================================================

NTL_YEARS <- 2006:2019

# Asignar label a cada columna
add_labels <- function(df) {

  attr(df$country,       "label") <- "Country name"
  attr(df$cntry_code,    "label") <- "IPUMS numeric country code"
  attr(df$admin_name,    "label") <- "Municipality/Canton/Province name (IPUMS standardized)"
  attr(df$ipum_code,     "label") <- "IPUMS harmonized geographic code (globally unique)"
  attr(df$ipum_ref_year, "label") <- "Census year of IPUMS boundary definition"
  attr(df$muni_code,     "label") <- "National municipality code (original source)"
  attr(df$muni_orig_col, "label") <- "Original IPUMS column name for traceability"
  attr(df$parent,        "label") <- "Parent administrative unit code (state/department)"

  for (yr in NTL_YEARS) {
    col <- paste0("ntl_", yr)
    attr(df[[col]], "label") <- paste0("Mean NTL ", yr,
                                       " - Li2020 Harmonized DMSP-VIIRS v10")
  }
  df
}

df_stata <- add_labels(df)
write_dta(df_stata, file.path(OUT_DIR, "merge_ntl_neighbors.dta"))
cat("Stata exportado    : merge_ntl_neighbors.dta\n")

# =============================================================================
# 7. Generar codebook .txt
# =============================================================================

ntl_year_range <- paste0(min(NTL_YEARS), "-", max(NTL_YEARS))

codebook <- paste0(
"================================================================================
CODEBOOK: merge_ntl_neighbors
Dataset de luces nocturnas (NTL) por municipio - 5 paises vecinos de Colombia
================================================================================

FUENTE NTL  : Li2020 Harmonized DMSP-VIIRS version 10
              Chen et al. (2021) - https://doi.org/10.1016/j.rse.2020.112276
              Anos cubiertos: ", ntl_year_range, "
              2006-2013: producto calDMSP (calibrado DMSP)
              2014-2019: producto simVIIRS (simulado VIIRS)

FUENTE GEO  : IPUMS International - GIS Boundary Files
              Minnesota Population Center, University of Minnesota
              https://international.ipums.org/international/gis.shtml
              Limites generalizados y armonizados para analisis estadistico.

PAISES      : Ecuador (141 unidades), Venezuela (238), Peru (169),
              Mexico (2469), Brasil (2422)
              Total: ", nrow(df), " unidades

ARCHIVOS    : merge_ntl_neighbors.shp  -> shapefile con geometria
              merge_ntl_neighbors.csv  -> CSV plano sin geometria
              merge_ntl_neighbors.dta  -> Stata 14+ con variable labels

================================================================================
DESCRIPCION DE VARIABLES
================================================================================

Variable        Tipo     Descripcion
--------------------------------------------------------------------------------
country         string   Nombre del pais en ingles (ej. Brazil, Ecuador)

cntry_code      numeric  Codigo numerico del pais segun IPUMS International

admin_name      string   Nombre del municipio/canton/provincia estandarizado
                         por IPUMS. Equivalente al nivel 2 administrativo.

ipum_code       string   Codigo geografico armonizado de IPUMS (unico a nivel
                         global). Permite linkear con microdatos censales IPUMS.
                         Formato: concatenacion de codigo pais + codigo unidad.

ipum_ref_year   numeric  Ano del censo al que corresponde la definicion de
                         limites del shapefile IPUMS utilizado:
                           Ecuador   -> 2010
                           Venezuela -> 2001
                           Peru      -> 2017
                           Mexico    -> 2020
                           Brasil    -> 2010

muni_code       string   Codigo oficial nacional del municipio segun el censo
                         de referencia de cada pais. Ver muni_orig_col para
                         saber a que columna original corresponde.

muni_orig_col   string   Nombre original de la columna de codigo nacional en
                         el shapefile IPUMS. Permite trazabilidad exacta:
                           Ecuador   -> CANT2010  (canton, censo 2010)
                           Venezuela -> MUNI2001  (municipio, censo 2001)
                           Peru      -> PROV2017  (provincia, censo 2017)
                           Mexico    -> MUNI2020  (municipio, censo 2020)
                           Brasil    -> MUNI2010  (municipio, censo 2010)

parent          string   Codigo de la unidad administrativa de nivel superior
                         (estado/departamento/provincia) al que pertenece
                         el municipio, segun codificacion IPUMS.

ntl_2006 ...    numeric  Media del valor NTL (Digital Number, DN) de todos los
ntl_2019                 pixeles dentro del municipio para cada ano.
                         Fuente: Li2020 Harmonized DMSP-VIIRS v10.
                         Unidades: numero digital (DN), escala 0-63 aprox.
                         NA: municipios sin pixeles validos en ese ano.

================================================================================
NOTAS
================================================================================

1. ipum_code es el identificador recomendado para joins con otras fuentes
   IPUMS (microdatos censales, geospatial contextuals, etc.).

2. Los codigos nacionales (muni_code) pueden no ser comparables entre paises
   ya que cada pais tiene su propio sistema de codificacion.

3. La columna ipum_ref_year indica el ano del shapefile base. Los limites
   municipales pueden haber cambiado despues de ese ano.

")

writeLines(codebook, file.path(OUT_DIR, "merge_ntl_neighbors_codebook.txt"))
cat("Codebook generado  : merge_ntl_neighbors_codebook.txt\n")

# =============================================================================
# 8. Resumen final
# =============================================================================

cat("\n=== MERGE COMPLETADO ===\n")
cat(sprintf("Unidades totales : %d\n", nrow(df)))
cat(sprintf("Anos NTL         : %s\n", ntl_year_range))
cat(sprintf("Columnas         : %d\n", ncol(df)))
cat("\nUnidades por pais:\n")
print(as.data.frame(table(df$country)))
cat("\nOutputs en: output/04_ntl_neighbors/merge/\n")
