library(arrow)
library(dplyr)
library(sf)
library(geoarrow)
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


points_parquet <- open_dataset("points.parquet") |>
  filter(mygroup %in% c("a", "d")) |>
  select(mygroup, value, geometry) |>
  st_as_sf(sf_column_name = "geometry")

