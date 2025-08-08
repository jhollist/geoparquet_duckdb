library(duckdb)
library(sf)

# Create a DuckDB connection
con <- dbConnect(duckdb::duckdb(), "spatial_duckdb.db")

# Load spatial extensions
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")

# Read data into DuckDb from GeoPackage files using SQL
dbExecute(con, "CREATE TABLE lake AS SELECT * FROM st_read('lake.gpkg');")
dbExecute(con, "CREATE TABLE lines AS SELECT * FROM st_read('lines.gpkg');")
dbListTables(con)

# Check the structure of the tables
dbListFields(con, "lake")
dbListFields(con, "lines")

# Perform spatial intersection using st_intersection in duckdb
dbExecute(con, "CREATE TABLE lake_lines AS  
                SELECT *, st_aswkb(st_intersection(lake.geom, lines.geom)) AS geometry
                FROM lake, lines;")
dbListFields(con, "lake_lines")

# Pull lake_lines into R as an sf object
lake_lines_sf <- dbGetQuery(con, "SELECT * FROM lake_lines;") %>%
  st_as_sf(crs = st_crs(5072))
