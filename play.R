library(arrow)
library(duckdb)
library(dplyr)
library(duckplyr)
library(sf)
library(geoarrow)
library(DBI)
library(tictoc)

# Create test and write to parquet
num_pts <- 1
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
as_tibble(xy_sf) |>
  group_by(mygroup) |>
  write_dataset("points")

single_file <- function(){points_parquet <- open_dataset("points.parquet") |>
  # But can put in quite a bit of dplyr filtering here...
  filter(mygroup %in% c("a", "d")) |>
  select(mygroup, value, geometry) |>
  st_as_sf(sf_column_name = "geometry")}
partitioned <- function(){points_parquet_part <- open_dataset("points", partitioning = c("mygroup")) |>
  # But can put in quite a bit of dplyr filtering here...
  filter(mygroup %in% c("a", "d")) |>
  select(mygroup, value, geometry) |>
  st_as_sf(sf_column_name = "geometry")}

microbenchmark::microbenchmark(single_file(), partitioned(),times = 10)

#tic()
#points_gpkg <- st_read("points.gpkg", 
#                       query = "SELECT mygroup, value, geom 
#                       FROM points 
#                       WHERE mygroup = 'a' OR mygroup = 'd';")  
#toc()


# Create connection with duckdb
con <- dbConnect(duckdb())
# Install and load Spatial
dbExecute(con, "LOAD spatial;")
dbExecute(con,
          "CREATE TABLE geo_pts AS 
          SELECT id, mygroup, value, 
                 st_aswkb(st_point(geometry.x, geometry.y)) as geometry
          FROM points.parquet;")
#https://github.com/duckdb/duckdb-r/issues/117
dbExecute(con,
           "CREATE TABLE geo_buff AS
           SELECT id, mygroup, value, st_aswkb(st_buffer(geometry, 100)) AS geometry FROM geo_pts;")
x <- dbGetQuery(con,
           "SELECT * FROM geo_pts;") |> 
  st_as_sf(crs = 5072)

dbDisconnect(con)

