# =============================================================================
# 01_process_ntl.R
# Procesamiento de datos de luces nocturnas (NTL) para Colombia
#
# Hardware detectado:
#   RAM:  48 GB  → terra usa hasta 29 GB (memfrac = 0.6)
#   CPU:  AMD Ryzen 7 7840U — 8 núcleos / 16 threads → 12 workers en paralelo
#   GPU:  AMD RX 7600M XT — terra/GDAL no soporta GPU AMD para raster ops;
#         los 16 threads del CPU son el recurso principal para este workload
#
# Para cada fuente NTL:
#   1. Descomprime archivos .gz (VIIRS) en streaming, sin cargar en RAM
#   2. Reproyecta si es necesario (Zhong2025 usa Equal Earth)
#   3. Recorta al extent de Colombia usando MPIOS_limpio.shp
#   4. Calcula el promedio de valor NTL por municipio (zonal statistics)
#   Pasos 2–4 corren en paralelo sobre todos los años de cada fuente
#
# Output:
#   output/clipped/  → TIFs comprimidos recortados a Colombia, por fuente
#   output/stats/    → CSV + shapefile con ntl_mean por municipio y año
#
# Paquetes requeridos:
#   terra, sf, dplyr, stringr, tidyr, R.utils, future, future.apply
# =============================================================================

# --- Instalar paquetes faltantes automáticamente ---------------------------
required <- c("terra", "sf", "dplyr", "stringr", "tidyr",
               "R.utils", "future", "future.apply")
missing  <- required[!sapply(required, requireNamespace, quietly = TRUE)]
if (length(missing) > 0) {
  install.packages(missing, repos = "https://cloud.r-project.org")
}

library(terra)
library(sf)
library(dplyr)
library(stringr)
library(tidyr)
library(R.utils)
library(future)
library(future.apply)

# =============================================================================
# 0. Configuración de hardware
# =============================================================================

RAM_GB      <- 48
N_THREADS   <- 16
N_WORKERS   <- 12   # deja 4 threads para el SO y el proceso principal

# terra: 60% del total de RAM para operaciones raster
terraOptions(
  memfrac  = 0.6,          # ~29 GB disponibles para terra
  tempdir  = tempdir(),    # directorio temporal para chunks en disco
  progress = 0             # sin barra de progreso interna (usamos la propia)
)

cat(sprintf(
  "Hardware configurado: %.0f GB RAM | %d workers paralelos\n",
  RAM_GB, N_WORKERS
))

# Plan de paralelismo: multisession funciona en Windows y Linux/Mac
plan(multisession, workers = N_WORKERS)

# =============================================================================
# 1. Rutas base
# =============================================================================

base_dir  <- "C:/Users/diana/OneDrive - Universidad de los Andes/nighttime_light"

input_ntl <- file.path(base_dir, "input", "ntl")
input_geo <- file.path(base_dir, "input", "geo")
out_clip  <- file.path(base_dir, "output", "01_clipped")
out_stats <- file.path(base_dir, "output", "02_stats")

for (d in c(out_clip, out_stats)) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# 2. Cargar shapefile de municipios en el proceso principal (para exportar)
# =============================================================================

shp_path <- file.path(input_geo, "MPIOS_limpio.shp")
mpios    <- st_read(shp_path, quiet = TRUE)
cat(sprintf("Municipios cargados: %d | CRS: %s\n", nrow(mpios), st_crs(mpios)$input))

# Columna ID del shapefile (primera columna no-geométrica)
id_col <- names(st_drop_geometry(mpios))[1]

# =============================================================================
# 3. Función auxiliar: recortar un raster y calcular media por municipio
#    (corre dentro de cada worker paralelo)
#    IMPORTANTE: recibe shp_path (string), no un objeto terra/sf,
#    porque los punteros C++ de terra no son serializables entre procesos.
# =============================================================================

clip_and_zonal <- function(raster_path, shp_path, out_folder, file_label) {

  library(terra)   # cada worker carga sus propias librerías

  tryCatch({

    r          <- rast(raster_path)
    mpios_vect <- vect(shp_path)
    mpios_wgs84 <- project(mpios_vect, "EPSG:4326")

    if (is.lonlat(r)) {
      # Raster ya en WGS84 (DMSP, VIIRS, Li2020): recortar directamente
      r_crop <- crop(r, ext(mpios_wgs84))
      r_mask <- mask(r_crop, mpios_wgs84)

    } else {
      # Raster proyectado (ej. Zhong2025 Equal Earth EPSG:8857):
      # 1. Forzar CRS correcto (el TIF tiene datum "Unknown" que bloquea PROJ)
      crs(r) <- "EPSG:8857"
      # 2. Proyectar el polígono pequeño al CRS del raster (mucho más rápido
      #    que reprojectar el raster global completo)
      mpios_proj <- project(mpios_wgs84, "EPSG:8857")
      # 3. Recortar y enmascarar en EPSG:8857
      r_crop <- crop(r, ext(mpios_proj))
      r_mask <- mask(r_crop, mpios_proj)
      # 4. Reprojectar solo el resultado pequeño (Colombia) a WGS84
      r_mask <- project(r_mask, "EPSG:4326")
    }

    # Guardar TIF comprimido (LZW + tiles para lectura rápida)
    out_path <- file.path(out_folder, paste0(file_label, "_colombia.tif"))
    writeRaster(r_mask, out_path, overwrite = TRUE,
                gdal = c("COMPRESS=LZW", "TILED=YES",
                         "BLOCKXSIZE=512", "BLOCKYSIZE=512"))

    # Estadísticas zonales: devolver solo row_uid + ntl_mean
    # (el join con atributos del shapefile se hace en el proceso principal)
    z <- terra::extract(r_mask, mpios_wgs84, fun = mean, na.rm = TRUE, bind = FALSE)
    list(ok    = TRUE,
         data  = data.frame(row_uid = z[[1]], ntl_mean = z[[2]]),
         label = file_label)

  }, error = function(e) {
    list(ok = FALSE, data = NULL, label = file_label, error = conditionMessage(e))
  })
}

# =============================================================================
# 4. Función para procesar una fuente completa en paralelo
# =============================================================================

process_source <- function(meta_df,       # data.frame con columnas: path, label
                            shp_path,     # ruta al shapefile (string serializable)
                            out_folder,
                            source_name,
                            extra_cols = list()) {

  dir.create(out_folder, showWarnings = FALSE)
  n <- nrow(meta_df)
  cat(sprintf("\n%s: procesando %d archivos en paralelo...\n", source_name, n))
  t0 <- proc.time()

  # future.globals declara solo strings — evita que future serialice
  # objetos terra del entorno global (SpatRaster/SpatVector), que fallan
  # al ser wrapped/unwrapped entre procesos.
  paths_vec  <- meta_df$path
  labels_vec <- meta_df$label

  results <- future_lapply(
    seq_len(n),
    function(i) {
      clip_and_zonal(
        raster_path = paths_vec[i],
        shp_path    = shp_path,
        out_folder  = out_folder,
        file_label  = labels_vec[i]
      )
    },
    future.globals  = list(
      clip_and_zonal = clip_and_zonal,
      paths_vec      = paths_vec,
      labels_vec     = labels_vec,
      shp_path       = shp_path,
      out_folder     = out_folder
    ),
    future.packages = c("terra", "dplyr"),
    future.seed     = TRUE
  )

  elapsed <- (proc.time() - t0)[["elapsed"]]

  # Separar éxitos y errores
  ok_idx  <- sapply(results, `[[`, "ok")
  if (any(!ok_idx)) {
    cat(sprintf("  ⚠ %d archivo(s) fallaron:\n", sum(!ok_idx)))
    for (r in results[!ok_idx]) cat(sprintf("    - %s: %s\n", r$label, r$error))
  }

  # Combinar resultados exitosos, añadiendo columnas extra por fila
  ok_results <- results[ok_idx]
  ok_extra   <- lapply(extra_cols, `[`, ok_idx)

  df_list <- lapply(seq_along(ok_results), function(j) {
    d <- ok_results[[j]]$data
    for (col in names(ok_extra)) d[[col]] <- ok_extra[[col]][j]
    d
  })

  df <- bind_rows(df_list)

  cat(sprintf("  ✓ %d/%d completados en %.1f min\n",
              sum(ok_idx), n, elapsed / 60))
  return(df)
}

# =============================================================================
# 5. FUENTE 1a: EOG – DMSP
#    F##YYYY.v4b.global.intercal.stable_lights.avg_vis.tif
#    Años con múltiples satélites: usar el de número mayor
# =============================================================================

dmsp_dir <- file.path(input_ntl, "EOG_Elvidge_DMSP-VIIRS")
out_dmsp <- file.path(out_clip, "EOG_DMSP")

dmsp_meta <- data.frame(
  path     = list.files(dmsp_dir, pattern = "\\.tif$", full.names = TRUE),
  filename = list.files(dmsp_dir, pattern = "\\.tif$")
) |>
  filter(!str_detect(filename, "\\(")) |>   # eliminar duplicados " (1)"
  mutate(
    satellite = as.integer(str_extract(filename, "(?<=F)\\d{2}")),
    year      = as.integer(str_extract(filename, "(?<=F\\d{2})\\d{4}"))  # año tras F##
  ) |>
  group_by(year) |>
  slice_max(satellite, n = 1, with_ties = FALSE) |>
  ungroup() |>
  arrange(year) |>
  mutate(label = paste0("DMSP_F", satellite, "_", year))

stats_dmsp_df <- process_source(
  meta_df     = dmsp_meta,
  shp_path    = shp_path,
  out_folder  = out_dmsp,
  source_name = "EOG_DMSP",
  extra_cols  = list(
    source    = rep("EOG_DMSP", nrow(dmsp_meta)),
    year      = dmsp_meta$year,
    satellite = paste0("F", dmsp_meta$satellite)
  )
)

gc()

# =============================================================================
# 6. FUENTE 1b: EOG – VIIRS
#    VNL_v21_npp_YYYY_global_vcmslcfg_*.average.dat.tif.gz
#    Descompresión por streaming antes del procesamiento paralelo
# =============================================================================

out_viirs     <- file.path(out_clip, "EOG_VIIRS")
viirs_unzip   <- file.path(input_ntl, "EOG_Elvidge_DMSP-VIIRS", "unzipped")
dir.create(viirs_unzip, showWarnings = FALSE)

viirs_gz <- list.files(dmsp_dir, pattern = "\\.tif\\.gz$", full.names = TRUE)

cat(sprintf("\nVIIRS: descomprimiendo %d archivos (streaming, sin cargar en RAM)...\n",
            length(viirs_gz)))

viirs_tif_paths <- sapply(viirs_gz, function(gz) {
  tif_path <- file.path(viirs_unzip, str_remove(basename(gz), "\\.gz$"))
  if (!file.exists(tif_path)) {
    R.utils::gunzip(gz, destname = tif_path, remove = FALSE, overwrite = TRUE)
  }
  tif_path
})

viirs_meta <- data.frame(path = viirs_tif_paths) |>
  mutate(
    year_str = str_extract(basename(path), "\\d{4,8}(?=_global)"),
    year     = as.integer(str_sub(year_str, 1, 4)),
    label    = paste0("VIIRS_", year_str)
  ) |>
  distinct(label, .keep_all = TRUE)   # eliminar duplicados de año 2018

stats_viirs_df <- process_source(
  meta_df     = viirs_meta,
  shp_path    = shp_path,
  out_folder  = out_viirs,
  source_name = "EOG_VIIRS",
  extra_cols  = list(
    source    = rep("EOG_VIIRS", nrow(viirs_meta)),
    year      = viirs_meta$year,
    satellite = rep("NPP", nrow(viirs_meta))
  )
)

gc()

# =============================================================================
# 7. FUENTE 2: Li2020 – Harmonized DMSP-VIIRS (version_8 y version_10)
#    Harmonized_DN_NTL_YYYY_calDMSP.tif / _simVIIRS.tif
#    version_8:  1992–2021
#    version_10: 1992–2024 (extendida y actualizada)
# =============================================================================

out_li_v8  <- file.path(out_clip, "Li2020", "version_8")
out_li_v10 <- file.path(out_clip, "Li2020", "version_10")

# -- version_8 (1992–2021) ----------------------------------------------------
li_v8_dir  <- file.path(input_ntl, "Li2020_Harmonized_DMSP-VIIRS", "version_8")

li_v8_meta <- data.frame(
  path = list.files(li_v8_dir,
                    pattern = "^Harmonized_DN_NTL_\\d{4}_(calDMSP|simVIIRS)\\.tif$",
                    full.names = TRUE)
) |>
  mutate(
    year    = as.integer(str_extract(basename(path), "\\d{4}")),
    product = str_extract(basename(path), "(calDMSP|simVIIRS)"),
    label   = paste0("Li2020_v8_", year, "_", product)
  ) |>
  arrange(year)

stats_li_v8_df <- process_source(
  meta_df     = li_v8_meta,
  shp_path    = shp_path,
  out_folder  = out_li_v8,
  source_name = "Li2020_v8",
  extra_cols  = list(
    source  = rep("Li2020_v8", nrow(li_v8_meta)),
    year    = li_v8_meta$year,
    product = li_v8_meta$product,
    version = rep("version_8", nrow(li_v8_meta))
  )
)

gc()

# -- version_10 (1992–2024) ---------------------------------------------------
li_v10_dir  <- file.path(input_ntl, "Li2020_Harmonized_DMSP-VIIRS", "version_10")

li_v10_meta <- data.frame(
  path = list.files(li_v10_dir,
                    pattern = "^Harmonized_DN_NTL_\\d{4}_(calDMSP|simVIIRS)\\.tif$",
                    full.names = TRUE)
) |>
  mutate(
    year    = as.integer(str_extract(basename(path), "\\d{4}")),
    product = str_extract(basename(path), "(calDMSP|simVIIRS)"),
    label   = paste0("Li2020_v10_", year, "_", product)
  ) |>
  arrange(year)

stats_li_v10_df <- process_source(
  meta_df     = li_v10_meta,
  shp_path    = shp_path,
  out_folder  = out_li_v10,
  source_name = "Li2020_v10",
  extra_cols  = list(
    source  = rep("Li2020_v10", nrow(li_v10_meta)),
    year    = li_v10_meta$year,
    product = li_v10_meta$product,
    version = rep("version_10", nrow(li_v10_meta))
  )
)

gc()

# =============================================================================
# 8. FUENTE 3: Zhong2025 – LRCC-DVNL
#    LACC_YYYY.tif — proyección Equal Earth (se reproyecta internamente)
# =============================================================================

zhong_dir <- file.path(input_ntl, "Zhong2025_LRCC-DVNL", "LRCC-DVNL data")
out_zhong <- file.path(out_clip, "Zhong2025")

zhong_meta <- data.frame(
  path = list.files(zhong_dir, pattern = "^LACC_\\d{4}\\.tif$", full.names = TRUE)
) |>
  mutate(
    year  = as.integer(str_extract(basename(path), "\\d{4}")),
    label = paste0("Zhong2025_", year)
  ) |>
  arrange(year)

stats_zhong_df <- process_source(
  meta_df     = zhong_meta,
  shp_path    = shp_path,
  out_folder  = out_zhong,
  source_name = "Zhong2025",
  extra_cols  = list(
    source = rep("Zhong2025", nrow(zhong_meta)),
    year   = zhong_meta$year
  )
)

gc()

# =============================================================================
# 9. Cerrar workers paralelos
# =============================================================================

plan(sequential)

# =============================================================================
# 10. Exportar resultados
# =============================================================================

cat("\nExportando resultados...\n")

# --- 10a. CSVs ---------------------------------------------------------------
write.csv(stats_dmsp_df,   file.path(out_stats, "stats_EOG_DMSP.csv"),    row.names = FALSE)
write.csv(stats_viirs_df,  file.path(out_stats, "stats_EOG_VIIRS.csv"),   row.names = FALSE)
write.csv(stats_li_v8_df,  file.path(out_stats, "stats_Li2020_v8.csv"),   row.names = FALSE)
write.csv(stats_li_v10_df, file.path(out_stats, "stats_Li2020_v10.csv"),  row.names = FALSE)
write.csv(stats_zhong_df,  file.path(out_stats, "stats_Zhong2025.csv"),   row.names = FALSE)

# --- 10b. Shapefiles: una columna ntl_YYYY por año -------------------------

export_shapefile <- function(stats_df, municipios_sf, source_name, out_folder) {

  if (is.null(stats_df) || nrow(stats_df) == 0) {
    cat(sprintf("  ⚠ %s: sin datos, shapefile no generado.\n", source_name))
    return(invisible(NULL))
  }

  # row_uid es único por municipio (índice de fila del shapefile original)
  wide <- stats_df |>
    select(row_uid, year, ntl_mean) |>
    pivot_wider(
      names_from   = year,
      values_from  = ntl_mean,
      names_prefix = "ntl_",
      values_fn    = mean    # por si hubiera algún duplicado residual
    )

  # Unir con geometría por posición de fila
  out_sf <- municipios_sf |>
    mutate(.row_uid = seq_len(n())) |>
    left_join(wide, by = c(".row_uid" = "row_uid")) |>
    select(-.row_uid)

  out_path <- file.path(out_folder, paste0("mpios_ntl_", source_name, ".shp"))
  st_write(out_sf, out_path, delete_dsn = TRUE, quiet = TRUE)
  cat(sprintf("  Shapefile: mpios_ntl_%s.shp\n", source_name))
}

export_shapefile(stats_dmsp_df,   mpios, "EOG_DMSP",    out_stats)
export_shapefile(stats_viirs_df,  mpios, "EOG_VIIRS",   out_stats)
export_shapefile(stats_li_v8_df,  mpios, "Li2020_v8",   out_stats)
export_shapefile(stats_li_v10_df, mpios, "Li2020_v10",  out_stats)
export_shapefile(stats_zhong_df,  mpios, "Zhong2025",   out_stats)

cat("\n=== Proceso completado ===\n")
cat("TIFs recortados:\n")
cat("  output/01_clipped/EOG_DMSP/\n")
cat("  output/01_clipped/EOG_VIIRS/\n")
cat("  output/01_clipped/Li2020/version_8/\n")
cat("  output/01_clipped/Li2020/version_10/\n")
cat("  output/01_clipped/Zhong2025/\n")
cat("Shapefiles/CSV:   output/02_stats/\n")
