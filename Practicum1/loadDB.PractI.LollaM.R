# --------------------------------------------
# Program: loadDB.PractI.LollaM.R
# Author: Lolla,M
# Semester: Spring 2025
# Description: This script loads restaurant visit data into MySQL database.
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

print("Connected to MySQL database for data loading.")

# Load the CSV file from URL
csv_url <- "https://raw.githubusercontent.com/mahimalolla/DBMS/refs/heads/main/restaurant-visits-139874%20(2).csv"
df.orig <- read_csv(csv_url)

# Display first few rows to check structure
print(head(df.orig))

# Check column names
print(colnames(df.orig))

# Rename columns to match MySQL table structure
df.orig <- df.orig %>%
  rename(
    customer_name = CustomerName, 
    loyalty_program = LoyaltyMember,
    restaurant_name = Restaurant,
    server_name = ServerName,
    visit_date = VisitDate,
    party_size = PartySize,
    food_bill = FoodBill,
    alcohol_bill = AlcoholBill,
    tip_amount = TipAmount
  )

# Get actual column names in df.orig
existing_columns <- colnames(df.orig)

# Define columns to check, only keeping the ones that exist in df.orig
columns_to_check <- intersect(c("food_bill", "alcohol_bill", "tip_amount", 
                                "visit_date", "party_size", "server_name", 
                                "restaurant_name", "customer_name", "loyalty_program"), 
                              existing_columns)

# Count NA values in each selected column
na_counts <- colSums(is.na(df.orig[, columns_to_check, drop = FALSE]))
print("Count of missing (NA) values in selected columns:")
print(na_counts)

# Count empty strings in character columns
empty_counts <- sapply(df.orig[, columns_to_check, drop = FALSE], function(x) sum(x == "", na.rm = TRUE))
print("Count of empty values in selected columns:")
print(empty_counts)




# Handle missing values (replace sentinel values)
df.orig <- df.orig %>%
  mutate(
    # Replace missing customer names with 'Unknown'
    customer_name = ifelse(is.na(customer_name) | customer_name == "", "Unknown", customer_name),
    
    # Replace missing restaurant names with 'Unknown Restaurant'
    restaurant_name = ifelse(is.na(restaurant_name) | restaurant_name == "", "Unknown Restaurant", restaurant_name),
    
    # Replace missing server names with 'Unknown Server'
    server_name = ifelse(is.na(server_name) | server_name == "", "Unknown Server", server_name),
    
    # Handle missing party_size (99 means unknown)
    party_size = ifelse(is.na(party_size) | party_size == 99, 1, party_size),  # Assume minimum size 1
    
    # Fix invalid visit_date (Replace '0000-00-00' with today's date)
    visit_date = ifelse(is.na(visit_date) | visit_date == "0000-00-00", as.character(Sys.Date()), visit_date),
    
    # Handle missing food, alcohol, and tip amounts (replace with 0.00)
    food_bill = ifelse(is.na(food_bill), 0.00, food_bill),
    alcohol_bill = ifelse(is.na(alcohol_bill), 0.00, alcohol_bill),
    tip_amount = ifelse(is.na(tip_amount), 0.00, tip_amount),
    
    # Convert loyalty_program to integer (TRUE → 1, FALSE → 0)
    loyalty_program = as.integer(loyalty_program)
  )

print("Missing values handled.")


print(head(df.orig[, c("customer_name", "loyalty_program")])) # Check first few rows

dbGetQuery(con, "SHOW CREATE TABLE Customers;")

# ---------------------- Create Customers Table ---------------------- #
dbSendStatement(con, "
CREATE TABLE Customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL UNIQUE,
    loyalty_program BOOLEAN DEFAULT 0
);
")

# ---------------------- Create Restaurants Table ---------------------- #
query_restaurants <- "
CREATE TABLE Restaurants (
    restaurant_id INT AUTO_INCREMENT PRIMARY KEY,
    restaurant_name VARCHAR(255) NOT NULL UNIQUE,
    location VARCHAR(255) DEFAULT 'Unknown'
);
"
dbSendStatement(con, query_restaurants)
print("Restaurants table created successfully.")

# ---------------------- Create Servers Table ---------------------- #
query_servers <- "
CREATE TABLE Servers (
    server_id INT AUTO_INCREMENT PRIMARY KEY,
    server_name VARCHAR(255) NOT NULL UNIQUE
);
"
dbSendStatement(con, query_servers)
print("Servers table created successfully.")

# ---------------------- Create Visits Table ---------------------- #
query_visits <- "
CREATE TABLE Visits (
    visit_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    restaurant_id INT NOT NULL,
    visit_date DATE DEFAULT NULL,
    party_size INT DEFAULT NULL,
    server_id INT NOT NULL,
    food_bill DECIMAL(10,2) DEFAULT 0.00,
    alcohol_bill DECIMAL(10,2) DEFAULT 0.00,
    tip_amount DECIMAL(10,2) DEFAULT 0.00,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (restaurant_id) REFERENCES Restaurants(restaurant_id),
    FOREIGN KEY (server_id) REFERENCES Servers(server_id)
);
"
dbSendStatement(con, query_visits)
print("Visits table created successfully.")

print(colnames(df.orig))

print(head(df.orig[, c("customer_id", "restaurant_id", "visit_date", "party_size", "server_id", "food_bill", "alcohol_bill", "tip_amount")], 10))


# ---------------------- Create Transactions Table ---------------------- #
query_transactions <- "
CREATE TABLE Transactions (
    visit_id INT PRIMARY KEY,
    total_bill DECIMAL(10,2) DEFAULT 0.00,
    FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);
"
dbSendStatement(con, query_transactions)
print("Transactions table created successfully.")

# Batch size (adjust based on performance)
batch_size <- 1000

# Split data into chunks and insert
for (i in seq(1, nrow(df.orig), by = batch_size)) {
  
  batch <- df.orig[i:min(i+batch_size-1, nrow(df.orig)), ]
  
  values <- paste0(
    "('", gsub("'", "''", batch$customer_name), "', ", as.integer(batch$loyalty_program), ")",
    collapse = ", "
  )
  
  query <- sprintf("INSERT INTO Customers (customer_name, loyalty_program) 
                    VALUES %s 
                    ON DUPLICATE KEY UPDATE loyalty_program = VALUES(loyalty_program);",
                   values)
  
  dbExecute(con, query)  # Faster than dbSendStatement()
  
  print(paste("Inserted batch:", i, "to", min(i+batch_size-1, nrow(df.orig))))
}

print("Customers data batch-inserted successfully.")



# ---------------------- Insert Data into Restaurants Table ---------------------- #
for (i in seq(1, nrow(df.orig), by = batch_size)) {
  
  batch <- df.orig[i:min(i+batch_size-1, nrow(df.orig)), ]
  
  values <- paste0(
    "('", gsub("'", "''", batch$restaurant_name), "', 'Unknown')",
    collapse = ", "
  )
  
  query <- sprintf("INSERT INTO Restaurants (restaurant_name, location) 
                    VALUES %s 
                    ON DUPLICATE KEY UPDATE restaurant_name = VALUES(restaurant_name);",
                   values)
  
  dbExecute(con, query)
  
  print(paste("Inserted batch:", i, "to", min(i+batch_size-1, nrow(df.orig))))
}

print("Restaurants data batch-inserted successfully.")



# ---------------------- Insert Data into Servers Table ---------------------- #
batch_size <- 1000  # Adjust batch size for performance

# Extract unique server names and convert to a data frame
server_df <- data.frame(server_name = unique(df.orig$server_name), stringsAsFactors = FALSE)

for (i in seq(1, nrow(server_df), by = batch_size)) {
  
  batch <- server_df[i:min(i+batch_size-1, nrow(server_df)), , drop = FALSE]  # Keep as data frame
  
  values <- paste0(
    "('", gsub("'", "''", batch$server_name), "')",
    collapse = ", "
  )
  
  query <- sprintf("INSERT INTO Servers (server_name) 
                    VALUES %s 
                    ON DUPLICATE KEY UPDATE server_name = VALUES(server_name);",
                   values)
  
  dbExecute(con, query)
  
  print(paste("Inserted batch:", i, "to", min(i+batch_size-1, nrow(server_df))))
}

print("Servers data batch-inserted successfully.")




# ---------------------- Insert Data into Visits Table ---------------------- #
for (i in seq(1, nrow(df.orig), by = batch_size)) {
  
  batch <- df.orig[i:min(i+batch_size-1, nrow(df.orig)), ]
  
  # Map Customer IDs
  customer_map <- dbGetQuery(con, "SELECT customer_id, customer_name FROM Customers;")
  batch <- merge(batch, customer_map, by = "customer_name", all.x = TRUE)
  
  # Map Restaurant IDs
  restaurant_map <- dbGetQuery(con, "SELECT restaurant_id, restaurant_name FROM Restaurants;")
  batch <- merge(batch, restaurant_map, by = "restaurant_name", all.x = TRUE)
  
  # Map Server IDs
  server_map <- dbGetQuery(con, "SELECT server_id, server_name FROM Servers;")
  batch <- merge(batch, server_map, by = "server_name", all.x = TRUE)
  
  # Remove rows where required fields are missing
  batch <- batch[!is.na(batch$customer_id) & !is.na(batch$restaurant_id) & !is.na(batch$server_id), ]
  
  if (nrow(batch) == 0) {
    next  # Skip empty batch
  }
  
  values <- paste0(
    "(", batch$customer_id, ", ", batch$restaurant_id, ", '", batch$visit_date, "', ",
    batch$server_id, ", ", batch$food_bill, ", ", batch$alcohol_bill, ", ", batch$tip_amount, ")",
    collapse = ", "
  )
  
  query <- sprintf("INSERT INTO Visits (customer_id, restaurant_id, visit_date, server_id, food_bill, alcohol_bill, tip_amount) 
                    VALUES %s;", values)
  
  dbExecute(con, query)
  
  print(paste("Inserted batch:", i, "to", min(i+batch_size-1, nrow(df.orig))))
}

print("Visits data batch-inserted successfully.")


#Debug Step - can be ignored 
print("Columns in batch before merging with visit_map:")
print(colnames(batch))

print("Columns in visit_map before merging:")
print(colnames(visit_map))

 
# Rename visit_id.x to visit_id
colnames(batch)[colnames(batch) == "visit_id.x"] <- "visit_id"

# Drop visit_id.y since it is entirely NA
batch <- batch[, !colnames(batch) %in% c("visit_id.y")]

print("Columns in batch before merging with visit_map:")
print(colnames(batch))
# Map Customer IDs
customer_map <- dbGetQuery(con, "SELECT customer_id, customer_name FROM Customers;")
batch <- merge(batch, customer_map, by = "customer_name", all.x = TRUE)

# Map Restaurant IDs
restaurant_map <- dbGetQuery(con, "SELECT restaurant_id, restaurant_name FROM Restaurants;")
batch <- merge(batch, restaurant_map, by = "restaurant_name", all.x = TRUE)

# Ensure visit_date is formatted correctly
batch$visit_date <- as.character(batch$visit_date)  # Convert to ensure it matches SQL format


# Dynamically fetch correct column names from batch
correct_cols <- c("visit_id", "customer_id", "restaurant_id", "visit_date", "food_bill", "alcohol_bill", "tip_amount")

# Ensure all columns exist before selecting
correct_cols <- correct_cols[correct_cols %in% colnames(batch)]

# Now safely select only existing columns
print("Selecting correct columns for debugging:")
print(head(batch[, correct_cols]))

print("Checking for duplicate visit_id in batch before inserting:")
print(table(duplicated(batch$visit_id)))


# Fix: Remove Already Existing `visit_id`s Before Inserting
existing_visits <- dbGetQuery(con, "SELECT visit_id FROM Transactions;")
batch <- batch[!batch$visit_id %in% existing_visits$visit_id, ]

# ---------------------- Insert Data into Transactions Table ---------------------- #
batch_size <- 1000  # Adjust batch size for performance

for (i in seq(1, nrow(df.orig), by = batch_size)) {
  
  batch <- df.orig[i:min(i+batch_size-1, nrow(df.orig)), ]
  
  # Fetch and map Customer IDs
  customer_map <- dbGetQuery(con, "SELECT customer_id, customer_name FROM Customers;")
  batch <- merge(batch, customer_map, by = "customer_name", all.x = TRUE)
  
  # Fetch and map Restaurant IDs
  restaurant_map <- dbGetQuery(con, "SELECT restaurant_id, restaurant_name FROM Restaurants;")
  batch <- merge(batch, restaurant_map, by = "restaurant_name", all.x = TRUE)
  
  # Fetch Visit IDs from Visits table
  visit_map <- dbGetQuery(con, "SELECT visit_id, customer_id, restaurant_id, visit_date FROM Visits;")
  batch <- merge(batch, visit_map, by = c("customer_id", "restaurant_id", "visit_date"), all.x = TRUE)
  
  # Fix column names if needed
  if ("visit_id.x" %in% colnames(batch)) {
    colnames(batch)[colnames(batch) == "visit_id.x"] <- "visit_id"
  }
  batch <- batch[, !colnames(batch) %in% c("visit_id.y")]  # Remove duplicate visit_id
  
  # Ensure visit_id exists before inserting (avoid NULL foreign key errors)
  batch <- batch[!is.na(batch$visit_id), ]
  
  # Fix: Remove Already Existing `visit_id`s Before Inserting
  existing_visits <- dbGetQuery(con, "SELECT visit_id FROM Transactions;")
  batch <- batch[!batch$visit_id %in% existing_visits$visit_id, ]
  
  # Remove duplicate visit_id inside the batch
  batch <- batch[!duplicated(batch$visit_id), ]
  
  # If batch is empty after filtering, skip
  if (nrow(batch) == 0) {
    print("No new transactions to insert. Skipping batch.")
    next
  }
  
  # Calculate total_bill
  batch$total_bill <- ifelse(is.na(batch$food_bill), 0.00, batch$food_bill) + 
    ifelse(is.na(batch$alcohol_bill), 0.00, batch$alcohol_bill) + 
    ifelse(is.na(batch$tip_amount), 0.00, batch$tip_amount)
  
  # Insert Transactions data (only new `visit_id`s)
  values <- paste0(
    "(", batch$visit_id, ", ", batch$total_bill, ")",
    collapse = ", "
  )
  
  query <- sprintf("INSERT INTO Transactions (visit_id, total_bill) 
                    VALUES %s;", values)
  
  dbExecute(con, query)
  
  print(paste("Inserted batch:", i, "to", min(i+batch_size-1, nrow(df.orig))))
}

print("Transactions data batch-inserted successfully.")



#----------------Verification Check for all tables------------------
# Check total counts for each table
print(dbGetQuery(con, "SELECT COUNT(*) AS total_customers FROM Customers;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS total_restaurants FROM Restaurants;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS total_servers FROM Servers;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS total_visits FROM Visits;"))
print(dbGetQuery(con, "SELECT COUNT(*) AS total_transactions FROM Transactions;"))



# Close connection
dbDisconnect(con)
print("Disconnected from database after loading data.")