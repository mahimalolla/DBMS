# --------------------------------------------
# Program: createDB.PractI.LollaM.R
# Author: Lolla,M
# Semester: Spring 2025 
# Description: This script connects to a MySQL database 
# and creates the normalized schema for the restaurant visits dataset.
# --------------------------------------------

install.packages("DBI")      # Database Interface
install.packages("RMySQL")   # MySQL Connector
library(DBI)
library(RMySQL)

# Database Connection
con <- dbConnect(RMySQL::MySQL(),
                 dbname = "cs5200_practicum",
                 host = "db4free.net",  
                 port = 3306,
                 user = "mahimalolla",
                 password = "2ei#QFX.cUMCTQp")

# Print success message
print("Connected to MySQL database successfully!")


#Creating Tables
sql_customers <- "
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(255) NOT NULL,
    loyalty_program BOOLEAN DEFAULT FALSE
);"
dbSendStatement(con, sql_customers)
print("Customers table created successfully!")

sql_restaurants <- "
CREATE TABLE IF NOT EXISTS Restaurants (
    restaurant_id INT PRIMARY KEY AUTO_INCREMENT,
    restaurant_name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL
);"
dbSendStatement(con, sql_restaurants)
print("Restaurants table created successfully!")

sql_servers <- "
CREATE TABLE IF NOT EXISTS Servers (
    server_id INT PRIMARY KEY AUTO_INCREMENT,
    server_name VARCHAR(255) NOT NULL
);"
dbSendStatement(con, sql_servers)
print("Servers table created successfully!")

sql_transactions <- "
CREATE TABLE IF NOT EXISTS Transactions (
    visit_id INT PRIMARY KEY,
    total_bill DECIMAL(10,2) CHECK (total_bill >= 0),
    FOREIGN KEY (visit_id) REFERENCES Visits(visit_id) ON DELETE CASCADE
);"
dbSendStatement(con, sql_transactions)
print("Transactions table created successfully!")


sql_visits <- "
CREATE TABLE IF NOT EXISTS Visits (
    visit_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    restaurant_id INT,
    visit_date DATE NOT NULL,
    party_size INT DEFAULT 1 CHECK (party_size > 0),
    server_id INT,
    food_bill DECIMAL(10,2) CHECK (food_bill >= 0),
    alcohol_bill DECIMAL(10,2) CHECK (alcohol_bill >= 0),
    tip_amount DECIMAL(10,2) CHECK (tip_amount >= 0),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (restaurant_id) REFERENCES Restaurants(restaurant_id) ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES Servers(server_id) ON DELETE SET NULL
);"
dbSendStatement(con, sql_visits)
print("Visits table created successfully!")

# Check if tables exist
tables <- dbListTables(con)
print(tables)

# Close connection
dbDisconnect(con)
print("Disconnected from database successfully!")


#Verification of the Database 
#Once you reconnect to the DB, let's check the table schema for each table.
# Query table structure
df_customers <- dbGetQuery(con, "DESCRIBE Customers;")
df_restaurants <- dbGetQuery(con, "DESCRIBE Restaurants;")
df_servers <- dbGetQuery(con, "DESCRIBE Servers;")
df_visits <- dbGetQuery(con, "DESCRIBE Visits;")
df_transactions <- dbGetQuery(con, "DESCRIBE Transactions;")

# Print table structures
print("Customers Table Schema:")
print(df_customers)

print("Restaurants Table Schema:")
print(df_restaurants)

print("Servers Table Schema:")
print(df_servers)

print("Visits Table Schema:")
print(df_visits)

print("Transactions Table Schema:")
print(df_transactions)


#Next, let's print the foreign key relationships.
# Check foreign keys in the schema
df_foreign_keys <- dbGetQuery(con, "
SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
FROM information_schema.KEY_COLUMN_USAGE 
WHERE TABLE_SCHEMA = 'cs5200_practicum' AND REFERENCED_TABLE_NAME IS NOT NULL;
")

# Print foreign key relationships
print("Foreign Key Constraints in the Database:")
print(df_foreign_keys)

# Close connection after verification
dbDisconnect(con)
print("Disconnected from database successfully after verification!")
