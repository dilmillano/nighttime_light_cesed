# 02_graph_NTL_PNIS.R
# Gráficas de series de tiempo NTL por grupo PNIS (Alta vs Baja probabilidad)
# para los 5 datasets del repositorio:
#   EOG DMSP, EOG VIIRS, Li2020 version_8, Li2020 version_10, Zhong2025
#
# Tipos de gráfica por fuente: valores crudos · índice normalizado · log-normalizado
# Output: output/03_graphs/
#
# Join: row_uid (CSV) → MPIOS_limpio (posición de fila) → MPIO_CDPMP → PNIS

library(dplyr)
library(tidyr)
library(ggplot2)
library(foreign)   # read.dbf

# ── Rutas ─────────────────────────────────────────────────────────────────────
base_ntl  <- "C:/Users/diana/OneDrive - Universidad de los Andes/nighttime_light"
shp_pnis  <- "C:/Users/diana/OneDrive - Universidad de los Andes/Diana_CESED/pnis/etapa3_proces_vecinos_manuel/MPIOS_PNIS_VECINOS.dbf"
shp_mpios <- file.path(base_ntl, "input/geo/MPIOS_limpio.dbf")
stats_dir <- file.path(base_ntl, "output/02_stats")
out_dir   <- file.path(base_ntl, "output/03_graphs")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# ── 1. Tabla municipios con PNIS ──────────────────────────────────────────────
mpios <- read.dbf(shp_mpios) |>
  mutate(row_uid = row_number()) |>
  select(row_uid, MPIO_CDPMP)

pnis_tbl <- read.dbf(shp_pnis) |>
  select(MPIO_CDPMP, PNIS) |>
  mutate(grupo = ifelse(PNIS == 1, "Alta probabilidad PNIS", "Baja probabilidad PNIS"))

mpio_pnis <- mpios |> left_join(pnis_tbl, by = "MPIO_CDPMP")

cat(sprintf("Municipios: %d | PNIS=1: %d | PNIS!=1: %d\n",
            nrow(mpio_pnis),
            sum(mpio_pnis$PNIS == 1, na.rm = TRUE),
            sum(mpio_pnis$PNIS != 1, na.rm = TRUE)))

# ── 2. Leer CSVs de estadísticas ──────────────────────────────────────────────
leer <- function(nombre) {
  read.csv(file.path(stats_dir, paste0("stats_", nombre, ".csv")))
}

dmsp   <- leer("EOG_DMSP")
viirs  <- leer("EOG_VIIRS")
zhong  <- leer("Zhong2025")

# Li2020 v8 (1992–2021): calDMSP para 1992-2013, simVIIRS para 2014-2021
li_v8 <- leer("Li2020_v8") |>
  filter((product == "calDMSP" & year <= 2013) |
         (product == "simVIIRS" & year >= 2014))

# Li2020 v10 (1992–2024): calDMSP para 1992-2013, simVIIRS para 2014-2024
li_v10 <- leer("Li2020_v10") |>
  filter((product == "calDMSP" & year <= 2013) |
         (product == "simVIIRS" & year >= 2014))

# ── 3. Agregar por grupo PNIS y año ───────────────────────────────────────────
agregar <- function(df) {
  df |>
    left_join(mpio_pnis, by = "row_uid") |>
    filter(!is.na(grupo)) |>
    group_by(grupo, year) |>
    summarise(mean_ntl = mean(ntl_mean, na.rm = TRUE), .groups = "drop")
}

dmsp_g   <- agregar(dmsp)
viirs_g  <- agregar(viirs)
li_v8_g  <- agregar(li_v8)
li_v10_g <- agregar(li_v10)
zhong_g  <- agregar(zhong)

# ── 4. Paleta y tema ──────────────────────────────────────────────────────────
colores <- c("Alta probabilidad PNIS" = "#2166ac",
             "Baja probabilidad PNIS" = "#d6604d")

tema_base <- function() {
  theme_minimal(base_size = 12) +
    theme(
      legend.position   = "bottom",
      legend.title      = element_blank(),
      axis.text.x       = element_text(angle = 90, vjust = 0.5, size = 9),
      panel.grid.minor  = element_blank(),
      plot.title        = element_text(face = "bold", size = 13),
      plot.subtitle     = element_text(color = "gray40", size = 10)
    )
}

# ── 5. Funciones de graficación ───────────────────────────────────────────────

# 5a. Valores crudos
plot_raw <- function(df, fuente) {
  n_grp <- df |> count(grupo) |> mutate(lbl = paste0(grupo, " (n años=", n, ")"))
  ggplot(df, aes(x = year, y = mean_ntl, color = grupo, group = grupo)) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2.2) +
    scale_color_manual(values = colores) +
    scale_x_continuous(breaks = seq(min(df$year), max(df$year), by = 2)) +
    labs(
      title    = paste("NTL promedio por grupo PNIS —", fuente),
      subtitle = "Promedio de medias municipales",
      x = "Año", y = "NTL promedio (DN o nW/cm²/sr)"
    ) +
    tema_base()
}

# 5b. Índice normalizado (base_year = 100)
plot_norm <- function(df, base_year, fuente) {
  base <- df |>
    filter(year == base_year) |>
    select(grupo, base_val = mean_ntl)

  df_n <- df |>
    left_join(base, by = "grupo") |>
    mutate(idx = (mean_ntl / base_val) * 100)

  ggplot(df_n, aes(x = year, y = idx, color = grupo, group = grupo)) +
    geom_hline(yintercept = 100, linetype = "dashed", color = "gray50", linewidth = 0.8) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2.2) +
    scale_color_manual(values = colores) +
    scale_x_continuous(breaks = seq(min(df_n$year), max(df_n$year), by = 2)) +
    labs(
      title    = paste("NTL normalizado por grupo PNIS —", fuente),
      subtitle = paste0("Índice: ", base_year, " = 100"),
      x = "Año", y = paste0("Índice NTL (", base_year, " = 100)")
    ) +
    tema_base()
}

# 5c. Log-normalizado (base_year = 0)
plot_log <- function(df, base_year, fuente) {
  base <- df |>
    filter(year == base_year) |>
    select(grupo, base_val = mean_ntl)

  df_l <- df |>
    left_join(base, by = "grupo") |>
    filter(mean_ntl > 0, base_val > 0) |>
    mutate(log_norm = log(mean_ntl) - log(base_val))

  ggplot(df_l, aes(x = year, y = log_norm, color = grupo, group = grupo)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50", linewidth = 0.8) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 2.2) +
    scale_color_manual(values = colores) +
    scale_x_continuous(breaks = seq(min(df_l$year), max(df_l$year), by = 2)) +
    labs(
      title    = paste("NTL log-normalizado por grupo PNIS —", fuente),
      subtitle = paste0("log(NTL\u209c) \u2212 log(NTL_", base_year, ")  |  base ", base_year, " = 0"),
      x = "Año", y = paste0("log(NTL) \u2212 log(NTL_", base_year, ")")
    ) +
    tema_base()
}

# ── 6. Guardar ────────────────────────────────────────────────────────────────
guardar <- function(p, nombre, w = 11, h = 5.5) {
  ruta <- file.path(out_dir, paste0(nombre, ".png"))
  ggsave(ruta, p, width = w, height = h, dpi = 150)
  cat("OK →", basename(ruta), "\n")
}

# ── 7. Producir todas las gráficas ────────────────────────────────────────────
cat("\n── EOG DMSP (1992–2013) ──\n")
guardar(plot_raw (dmsp_g,           "EOG DMSP (1992\u20132013)"),               "01_EOG_DMSP_raw")
guardar(plot_norm(dmsp_g,  2005,    "EOG DMSP (1992\u20132013)"),               "02_EOG_DMSP_norm2005")
guardar(plot_log (dmsp_g,  2005,    "EOG DMSP (1992\u20132013)"),               "03_EOG_DMSP_log2005")

cat("\n── EOG VIIRS (2012–2021) ──\n")
guardar(plot_raw (viirs_g,          "EOG VIIRS (2012\u20132021)"),              "04_EOG_VIIRS_raw")
guardar(plot_norm(viirs_g, 2013,    "EOG VIIRS (2012\u20132021)"),              "05_EOG_VIIRS_norm2013")
guardar(plot_log (viirs_g, 2013,    "EOG VIIRS (2012\u20132021)"),              "06_EOG_VIIRS_log2013")

cat("\n── Li2020 version_8 (1992–2021) ──\n")
guardar(plot_raw (li_v8_g,          "Li2020 version_8 (1992\u20132021)"),       "07_Li2020_v8_raw")
guardar(plot_norm(li_v8_g, 2005,    "Li2020 version_8 (1992\u20132021)"),       "08_Li2020_v8_norm2005")
guardar(plot_log (li_v8_g, 2005,    "Li2020 version_8 (1992\u20132021)"),       "09_Li2020_v8_log2005")

cat("\n── Li2020 version_10 (1992–2024) ──\n")
guardar(plot_raw (li_v10_g,         "Li2020 version_10 (1992\u20132024)"),      "10_Li2020_v10_raw")
guardar(plot_norm(li_v10_g, 2005,   "Li2020 version_10 (1992\u20132024)"),      "11_Li2020_v10_norm2005")
guardar(plot_log (li_v10_g, 2005,   "Li2020 version_10 (1992\u20132024)"),      "12_Li2020_v10_log2005")

cat("\n── Zhong2025 (1992–2022) ──\n")
guardar(plot_raw (zhong_g,          "Zhong2025 (1992\u20132022)"),              "13_Zhong2025_raw")
guardar(plot_norm(zhong_g, 2005,    "Zhong2025 (1992\u20132022)"),              "14_Zhong2025_norm2005")
guardar(plot_log (zhong_g, 2005,    "Zhong2025 (1992\u20132022)"),              "15_Zhong2025_log2005")

cat(sprintf("\nListo. 15 gráficas guardadas en:\n%s\n", out_dir))
