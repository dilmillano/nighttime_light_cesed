# =============================================================================
# 03_process_ntl_neighbors.R
# NTL promedio por municipio - 5 paises vecinos de Colombia
#   Ecuador, Venezuela, Peru, Mexico, Brasil
#
# Fuente NTL : Li2020 Harmonized DMSP-VIIRS version_10, anos 2006-2019
#   2006-2013 -> Harmonized_DN_NTL_YYYY_calDMSP.tif
#   2014-2019 -> Harmonized_DN_NTL_YYYY_simVIIRS.tif
#
# Shapefiles : input/geo/ipums_internacional/geo2_XX####/
#   Fuente IPUMS International - WGS84, columna ADMIN_NAME estandarizada
#
# Que hace este script:
#   1. Detecta RAM y CPUs disponibles y configura el paralelismo automaticamente
#   2. Lanza 14 workers en paralelo - cada worker se encarga de UN ano
#   3. Cada worker:
#        a. Lee el raster global de su ano desde disco -> queda en RAM
#        b. Con ese raster en RAM, recorre los 5 paises en secuencia:
#             crop    -> recorta al extent del pais (desde RAM, no vuelve al disco)
#             mask    -> pone NA fuera del territorio
#             guarda  -> TIF recortado en output/04_ntl_neighbors/clipped/<pais>/
#             extract -> calcula la media NTL por municipio
#   4. Cuando los 14 workers terminan, consolida los resultados por pais
#   5. Exporta por cada pais: CSV largo + shapefile con columnas ntl_2006...ntl_2019
#
#   Ventaja: el raster global se lee 14 veces (una por ano)
#   en vez de 70 (14 anos x 5 paises). Los 5 crops de cada ano salen de RAM.
#
# Outputs:
#   output/04_ntl_neighbors/clipped/<pais>/    -> TIFs recortados por ano
#   output/04_ntl_neighbors/stats/             -> shapefile + CSV con ntl_YYYY
#   output/04_ntl_neighbors/processing_log.csv -> registro de anos procesados
# =============================================================================

# --- Paquetes ----------------------------------------------------------------
required <- c("terra", "sf", "dplyr", "stringr", "tidyr",
               "future", "future.apply", "parallel")
missing  <- required[!sapply(required, requireNamespace, quietly = TRUE)]
if (length(missing) > 0) install.packages(missing, repos = "https://cloud.r-project.org")

library(terra); library(sf); library(dplyr)
library(stringr); library(tidyr)
library(future); library(future.apply)
library(parallel)

# =============================================================================
# 0. RUTA BASE - solo cambia USER_NAME segun el equipo
# =============================================================================
# Laptop Diana : diana
# Servidor     : d.millanorduz

USER_NAME <- "d.millanorduz"   

BASE_DIR <- file.path("C:/Users", USER_NAME,
                      "OneDrive - Universidad de los Andes",
                      "nighttime_light")

# =============================================================================
# 1. Rutas derivadas
# =============================================================================

INPUT_NTL <- file.path(BASE_DIR, "input", "ntl")
INPUT_GEO <- file.path(BASE_DIR, "input", "geo", "ipums_internacional")
OUT_BASE  <- file.path(BASE_DIR, "output", "04_ntl_neighbors")
OUT_CLIP  <- file.path(OUT_BASE, "clipped")
OUT_STATS <- file.path(OUT_BASE, "stats")
LOG_FILE  <- file.path(OUT_BASE, "processing_log.csv")

for (d in c(OUT_CLIP, OUT_STATS)) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# 2. Deteccion automatica de hardware
# =============================================================================

# --- RAM ---------------------------------------------------------------------
ram_bytes <- tryCatch({
  raw <- system("wmic ComputerSystem get TotalPhysicalMemory /value",
                intern = TRUE, ignore.stderr = TRUE)
  as.numeric(gsub(".*=", "", raw[grepl("TotalPhysicalMemory=", raw)]))
}, error = function(e) NA_real_)

if (!is.na(ram_bytes) && ram_bytes > 0) {
  total_ram_gb <- ram_bytes / 1024^3
  MEMFRAC      <- min(0.92, (total_ram_gb - 50) / total_ram_gb)
} else {
  total_ram_gb <- NA
  MEMFRAC      <- 0.6
}

# --- CPU ---------------------------------------------------------------------
# N_WORKERS: un worker por ano, maximo 14 (no hay mas anos que procesar)
# N_THREADS: threads internos de terra por worker (cores sobrantes / workers)
total_cores <- detectCores(logical = TRUE)
N_WORKERS   <- min(14L, max(1L, total_cores - 2L))
N_THREADS   <- max(1L, floor((total_cores - N_WORKERS) / N_WORKERS))

terraOptions(memfrac = MEMFRAC, tempdir = tempdir(), progress = 0)
plan(multisession, workers = N_WORKERS)

cat("=== Configuracion de hardware ===\n")
if (!is.na(total_ram_gb)) {
  cat(sprintf("  RAM total     : %.0f GB\n", total_ram_gb))
  cat(sprintf("  RAM para terra: %.0f GB (memfrac = %.2f, buffer = 50 GB)\n",
              total_ram_gb * MEMFRAC, MEMFRAC))
} else {
  cat(sprintf("  RAM           : no detectada - memfrac = %.2f\n", MEMFRAC))
}
cat(sprintf("  CPU logical   : %d -> %d workers x %d threads internos\n",
            total_cores, N_WORKERS, N_THREADS))
cat(sprintf("  Base dir      : %s\n\n", BASE_DIR))

# =============================================================================
# 3. Paises
# =============================================================================

countries <- list(
  list(name = "Ecuador",   shp = file.path(INPUT_GEO, "geo2_ec2010", "geo2_ec2010.shp")),
  list(name = "Venezuela", shp = file.path(INPUT_GEO, "geo2_ve2001", "geo2_ve2001.shp")),
  list(name = "Peru",      shp = file.path(INPUT_GEO, "geo2_pe2017", "geo2_pe2017.shp")),
  list(name = "Mexico",    shp = file.path(INPUT_GEO, "geo2_mx2020", "geo2_mx2020.shp")),
  list(name = "Brasil",    shp = file.path(INPUT_GEO, "geo2_br2010", "geo2_br2010.shp"))
)

for (co in countries) {
  if (!file.exists(co$shp)) stop(sprintf("Shapefile no encontrado: %s", co$shp))
  dir.create(file.path(OUT_CLIP, co$name), showWarnings = FALSE)
}

cat("Shapefiles verificados: OK\n\n")

# =============================================================================
# 4. Archivos NTL - Li2020 v10, anos 2006-2019
# =============================================================================

li_v10_dir <- file.path(INPUT_NTL, "Li2020_Harmonized_DMSP-VIIRS", "version_10")

li_meta <- data.frame(
  path = list.files(li_v10_dir,
                    pattern = "^Harmonized_DN_NTL_\\d{4}_(calDMSP|simVIIRS)\\.tif$",
                    full.names = TRUE)
) |>
  mutate(
    year    = as.integer(str_extract(basename(path), "\\d{4}")),
    product = str_extract(basename(path), "(calDMSP|simVIIRS)"),
    label   = paste0("Li2020_v10_", year, "_", product)
  ) |>
  filter(year >= 2006, year <= 2019) |>
  arrange(year)

cat(sprintf("Archivos NTL (2006-2019): %d\n\n", nrow(li_meta)))

# =============================================================================
# 5. Funcion del worker: procesa UN ano completo para los 5 paises
#    El raster global se carga una sola vez en RAM al inicio.
#    Cada crop/mask/extract opera sobre ese raster ya en memoria.
#    Escribe un log propio que el proceso principal imprime al terminar.
# =============================================================================

process_year <- function(raster_path, year, label, countries_list,
                         out_clip_base, log_dir, n_threads) {
  library(terra)
  terraOptions(threads = n_threads, progress = 0)

  log_file <- file.path(log_dir, paste0(label, ".log"))
  log <- function(msg) cat(paste0(msg, "\n"), file = log_file, append = TRUE)

  tryCatch({

    r <- rast(raster_path)   # lectura unica del raster global -> RAM
    log(sprintf("  [%d] raster cargado en RAM", year))

    results <- list()

    for (co in countries_list) {

      country_name <- co$name
      file_label   <- paste0(country_name, "_", label)

      mpios_wgs84 <- project(vect(co$shp), "EPSG:4326")

      r_crop <- crop(r, ext(mpios_wgs84))
      r_mask <- mask(r_crop, mpios_wgs84)

      writeRaster(r_mask,
                  file.path(out_clip_base, country_name,
                            paste0(file_label, "_clip.tif")),
                  overwrite = TRUE,
                  gdal = c("COMPRESS=LZW", "TILED=YES",
                           "BLOCKXSIZE=512", "BLOCKYSIZE=512"))

      z <- terra::extract(r_mask, mpios_wgs84, fun = mean, na.rm = TRUE, bind = FALSE)
      log(sprintf("  [%d] %s - clip + extract OK", year, country_name))

      results[[country_name]] <- data.frame(
        row_uid  = z[[1]],
        ntl_mean = z[[2]],
        year     = year,
        country  = country_name
      )
    }

    list(ok = TRUE, year = year, label = label, data = results)

  }, error = function(e) {
    log(sprintf("  [%d] ERROR: %s", year, conditionMessage(e)))
    list(ok = FALSE, year = year, label = label, data = NULL,
         error = conditionMessage(e))
  })
}

# =============================================================================
# 6. Lanzar los 14 workers en paralelo
# =============================================================================

log_dir <- file.path(OUT_BASE, "_worker_logs")
dir.create(log_dir, showWarnings = FALSE)
if (file.exists(LOG_FILE)) file.remove(LOG_FILE)

t_total <- proc.time()

cat(sprintf("[%s] INICIO - %d workers procesando %d anos x 5 paises\n\n",
            format(Sys.time(), "%H:%M:%S"), N_WORKERS, nrow(li_meta)))

paths_vec  <- li_meta$path
years_vec  <- li_meta$year
labels_vec <- li_meta$label
n          <- nrow(li_meta)

results_all <- future_lapply(
  seq_len(n),
  function(i) {
    process_year(
      raster_path    = paths_vec[i],
      year           = years_vec[i],
      label          = labels_vec[i],
      countries_list = countries_list,
      out_clip_base  = out_clip_base,
      log_dir        = log_dir,
      n_threads      = n_threads
    )
  },
  future.globals = list(
    process_year   = process_year,
    paths_vec      = paths_vec,
    years_vec      = years_vec,
    labels_vec     = labels_vec,
    countries_list = countries,
    out_clip_base  = OUT_CLIP,
    log_dir        = log_dir,
    n_threads      = N_THREADS
  ),
  future.packages = "terra",
  future.seed     = TRUE
)

total_min <- (proc.time() - t_total)[["elapsed"]] / 60

# Imprimir log de cada ano en orden
log_files <- sort(list.files(log_dir, pattern = "\\.log$", full.names = TRUE))
for (lf in log_files) cat(paste(readLines(lf), collapse = "\n"), "\n")
unlink(log_dir, recursive = TRUE)

# =============================================================================
# 7. Cerrar workers
# =============================================================================

plan(sequential)

# =============================================================================
# 8. Consolidar resultados y escribir log CSV
# =============================================================================

ok_idx <- sapply(results_all, `[[`, "ok")
n_ok   <- sum(ok_idx)
n_fail <- sum(!ok_idx)

if (n_fail > 0) {
  cat(sprintf("\n! %d ano(s) fallaron:\n", n_fail))
  for (r in results_all[!ok_idx]) cat(sprintf("  - %d: %s\n", r$year, r$error))
}

# Agrupar por pais
stats_list <- setNames(
  lapply(countries, function(co) {
    bind_rows(lapply(results_all[ok_idx], function(r) r$data[[co$name]]))
  }),
  sapply(countries, `[[`, "name")
)

# Log CSV por ano
log_df <- bind_rows(lapply(results_all, function(r) {
  data.frame(year  = r$year,
             label = r$label,
             ok    = r$ok,
             error = if (r$ok) "" else r$error)
}))
write.csv(log_df, LOG_FILE, row.names = FALSE)

cat(sprintf("\n[%s] FIN - %d/%d anos OK | %.1f min totales\n",
            format(Sys.time(), "%H:%M:%S"), n_ok, n, total_min))

# =============================================================================
# 9. Exportar shapefile y CSV por pais
# =============================================================================

cat("\n=== Exportando resultados ===\n")

for (co in countries) {
  stats_df <- stats_list[[co$name]]

  if (is.null(stats_df) || nrow(stats_df) == 0) {
    cat(sprintf("  ! %s: sin datos.\n", co$name)); next
  }

  write.csv(stats_df,
            file.path(OUT_STATS, paste0("stats_", co$name, "_Li2020_v10.csv")),
            row.names = FALSE)

  wide <- stats_df |>
    select(row_uid, year, ntl_mean) |>
    pivot_wider(names_from = year, values_from = ntl_mean,
                names_prefix = "ntl_", values_fn = mean)

  mpios_sf <- st_read(co$shp, quiet = TRUE) |>
    mutate(.row_uid = seq_len(n())) |>
    left_join(wide, by = c(".row_uid" = "row_uid")) |>
    select(-.row_uid)

  st_write(mpios_sf,
           file.path(OUT_STATS, paste0("mpios_ntl_", co$name, "_Li2020_v10.shp")),
           delete_dsn = TRUE, quiet = TRUE)

  cat(sprintf("  OK %s -> %d unidades | ntl_2006 ... ntl_2019\n",
              co$name, nrow(mpios_sf)))
}

# =============================================================================
# 10. Resumen final
# =============================================================================

cat(sprintf("\n=== PROCESO COMPLETADO - %.1f min totales ===\n", total_min))
cat("\nOutputs:\n")
cat("  output/04_ntl_neighbors/clipped/<pais>/\n")
cat("  output/04_ntl_neighbors/stats/\n")
cat("  output/04_ntl_neighbors/processing_log.csv\n")
