library(arrow)
library(duckdb)
library(dplyr)
library(duckplyr)
library(sf)
library(geoarrow)
library(DBI)
library(tictoc)

# Create test and write to parquet
num_pts <- 100000
x <- runif(num_pts, min = -90, max = -71)
y <- runif(num_pts, min = 30, max = 45)
xy_df <- tibble(x, y)
rm(x, y)
gc()
xy_df <- mutate(xy_df, 
                id = 1:num_pts, 
                mygroup = sample(letters[1:10], num_pts, replace = TRUE),
                value = rnorm(num_pts, mean = 100, sd = 30))
xy_sf <- st_as_sf(xy_df, coords = c("x", "y"), crs = 4326)
write_parquet(as_tibble(xy_sf), "points.parquet")
rm(xy_df)
gc()

xy_sf_buff <- function() {
  library(sf)
  xy_sf <- open_dataset("points.parquet") |> st_as_sf()
  st_transform(xy_sf, crs = 5072) |> 
  st_buffer(100) |>
  st_transform(4326)}

xy_duck_buff <- function(){
# Create connection with duckdb
con <- dbConnect(duckdb())
# Install and load Spatial
dbExecute(con, "LOAD spatial;")
# Read in from parquet, convert struct to geometry and then to WKB
# https://github.com/duckdb/duckdb-r/issues/117

dbExecute(con,
          "CREATE TABLE geo_pts AS 
          SELECT id, mygroup, value, 
                 st_transform(st_point(geometry.x, geometry.y),
                              'EPSG:4326', 'EPSG:5072', true) as geometry
          FROM points.parquet;")
dbExecute(con,
          "CREATE TABLE geo_buff AS
          SELECT id, mygroup, value,
          st_aswkb(st_transform(st_buffer(geometry, 100),
                                'EPSG:5072', 'EPSG:4326', true)) as geometry
          FROM geo_pts;"
          )
# Query Spatial into sf
dbGetQuery(con, "SELECT * FROM geo_buff;") |> 
  st_as_sf(crs = 4326)
dbDisconnect(con)}

microbenchmark::microbenchmark(xy_sf_buff(), xy_duck_buff(), times = 5)
