# --------------------------------------------
# Program: deleteDB.PractI.LollaM.R
# Author: Lolla,M
# Semester: Spring 2025
# Description: This script connects to a MySQL database and deletes (DROPs) all tables.
# --------------------------------------------

# Load required libraries
install.packages("DBI")
install.packages("RMySQL")
library(DBI)
library(RMySQL)

# Connect to MySQL database
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "cs5200_practicum",
                 host = "db4free.net",
                 port = 3306,
                 user = "mahimalolla",
                 password = "2ei#QFX.cUMCTQp")

print("Connected to MySQL database for deletion.")

# Drop tables if they exist
tables_to_drop <- c("Transactions", "Visits", "Servers", "Customers", "Restaurants")

for (table in tables_to_drop) {
  drop_query <- paste0("DROP TABLE IF EXISTS ", table, ";")
  dbSendStatement(con, drop_query)
  print(paste("Dropped table:", table))
}

# Close connection
dbDisconnect(con)
print("Disconnected from database after deleting tables.")
