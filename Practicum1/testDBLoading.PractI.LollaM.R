# --------------------------------------------
# Program: testDBLoading.PractI.LollaM.R
# Author: Lolla,M
# Semester: Spring 2025
# Description: This script tests the data loading process.
# --------------------------------------------

# Load required libraries
install.packages("DBI")
install.packages("RMySQL")
install.packages("readr")
install.packages("dplyr")


library(DBI)
library(RMySQL)
library(readr)
library(dplyr)

# Connect to MySQL database
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "cs5200_practicum",
                 host = "db4free.net",
                 port = 3306,
                 user = "mahimalolla",
                 password = "2ei#QFX.cUMCTQp")

print("Connected to MySQL database for verification.")

# Load the CSV file from URL
csv_url <- "https://raw.githubusercontent.com/mahimalolla/DBMS/refs/heads/main/restaurant-visits-139874%20(2).csv"
df.orig <- read_csv(csv_url)

# Map actual column names from df.orig
df.orig <- df.orig %>%
  rename(
    customer_name = CustomerName,
    loyalty_program = LoyaltyMember,
    restaurant_name = Restaurant,
    server_id = ServerEmpID,
    server_name = ServerName,
    visit_id = VisitID,
    visit_date = VisitDate,
    party_size = PartySize,
    food_bill = FoodBill,
    alcohol_bill = AlcoholBill,
    tip_amount = TipAmount
  ) %>%
  mutate(
    total_bill = food_bill + alcohol_bill + tip_amount  # Compute total bill
  )
# ------------------- Data Validation -------------------

print("Columns in df.orig:")
print(colnames(df.orig))


# 1. Count the number of unique values in CSV for each table
csv_unique_customers <- df.orig %>% select(customer_name) %>% distinct() %>% nrow()
csv_unique_restaurants <- df.orig %>% select(restaurant_name) %>% distinct() %>% nrow()
csv_unique_servers <- df.orig %>% select(server_name) %>% distinct() %>% nrow()
csv_total_visits <- nrow(df.orig)

# 2. Count the number of unique values in MySQL for each table
db_unique_customers <- dbGetQuery(con, "SELECT COUNT(*) AS total FROM Customers;")$total
db_unique_restaurants <- dbGetQuery(con, "SELECT COUNT(*) AS total FROM Restaurants;")$total
db_unique_servers <- dbGetQuery(con, "SELECT COUNT(*) AS total FROM Servers;")$total
db_total_visits <- dbGetQuery(con, "SELECT COUNT(*) AS total FROM Visits;")$total

# Print comparisons
print("Data Count Validation:")
print(paste("Unique Customers - CSV:", csv_unique_customers, "| Database:", db_unique_customers))
print(paste("Unique Restaurants - CSV:", csv_unique_restaurants, "| Database:", db_unique_restaurants))
print(paste("Unique Servers - CSV:", csv_unique_servers, "| Database:", db_unique_servers))
print(paste("Total Visits - CSV:", csv_total_visits, "| Database:", db_total_visits))


dbExecute(con, "ALTER TABLE Visits MODIFY alcohol_bill DECIMAL(12,4);")
print("Modified alcohol_bill column to DECIMAL(12,4)")

dbExecute(con, "UPDATE Visits SET alcohol_bill = NULL;")
print("Reset alcohol_bill values.")

for (i in 1:nrow(df.orig)) {
  query <- sprintf("UPDATE Visits 
                    SET alcohol_bill = %.4f 
                    WHERE visit_id = %d;",
                   df.orig$alcohol_bill[i], df.orig$visit_id[i])
  dbExecute(con, query)
}
print("Alcohol revenue values reinserted.")


# ------------------- Revenue Validation -------------------

# 3. Sum of food, alcohol, and tip amounts in CSV
csv_total_food <- sum(df.orig$food_bill, na.rm = TRUE)
csv_total_alcohol <- sum(df.orig$alcohol_bill, na.rm = TRUE)
csv_total_tip <- sum(df.orig$tip_amount, na.rm = TRUE)
csv_total_revenue <- csv_total_food + csv_total_alcohol + csv_total_tip

# 4. Sum of food, alcohol, and tip amounts in MySQL
# Increase decimal precision for better matching

# Fetch revenue totals from MySQL
db_totals <- dbGetQuery(con, "SELECT 
                                  ROUND(SUM(food_bill), 4) AS total_food, 
                                  ROUND(SUM(alcohol_bill), 4) AS total_alcohol, 
                                  ROUND(SUM(tip_amount), 4) AS total_tip 
                              FROM Visits;")

# Round values for accurate comparison
csv_total_food <- round(csv_total_food, 4)
csv_total_alcohol <- round(csv_total_alcohol, 4)
csv_total_tip <- round(csv_total_tip, 4)
csv_total_revenue <- csv_total_food + csv_total_alcohol + csv_total_tip

db_total_food <- round(db_totals$total_food, 4)
db_total_alcohol <- round(db_totals$total_alcohol, 4)
db_total_tip <- round(db_totals$total_tip, 4)
db_total_revenue <- db_total_food + db_total_alcohol + db_total_tip

# Compare values
print(paste("Total Food Revenue - CSV:", csv_total_food, "| Database:", db_total_food))
print(paste("Total Alcohol Revenue - CSV:", csv_total_alcohol, "| Database:", db_total_alcohol))
print(paste("Total Tip Amount - CSV:", csv_total_tip, "| Database:", db_total_tip))
print(paste("Total Revenue - CSV:", csv_total_revenue, "| Database:", db_total_revenue))

# ------------------- Final Check -------------------
if (csv_unique_customers == db_unique_customers &&
    csv_unique_restaurants == db_unique_restaurants &&
    csv_unique_servers == db_unique_servers &&
    csv_total_visits == db_total_visits &&
    round(csv_total_revenue, 4) == round(db_total_revenue, 4)) {
  
  print("All data validation checks PASSED! Data correctly loaded into the database.")
} else {
  print("Data validation FAILED! There are discrepancies between the CSV and database.")
}

# Close connection
dbDisconnect(con)
print("Disconnected from database after testing.")
