# --------------------------------------------
# Program: configBusinessLogic.PractI.LollaM.R
# Author: Lolla,M
# Semester: Spring 2025
# Description: This script creates MySQL stored procedures for handling visits in the database.
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

print("Connected to MySQL database.")

# ---------------------- CREATE `storeVisit` STORED PROCEDURE ---------------------- #
query_storeVisit <- "
CREATE PROCEDURE storeVisit(
    IN p_customer_id INT,
    IN p_restaurant_id INT,
    IN p_visit_date DATE,
    IN p_party_size INT,
    IN p_server_id INT,
    IN p_food_bill DECIMAL(10,2),
    IN p_alcohol_bill DECIMAL(10,2),
    IN p_tip_amount DECIMAL(10,2)
)
BEGIN
    DECLARE new_visit_id INT;

    -- Insert into Visits table
    INSERT INTO Visits (customer_id, restaurant_id, visit_date, party_size, server_id, food_bill, alcohol_bill, tip_amount)
    VALUES (p_customer_id, p_restaurant_id, p_visit_date, p_party_size, p_server_id, p_food_bill, p_alcohol_bill, p_tip_amount);
    
    -- Retrieve last inserted visit ID
    SET new_visit_id = LAST_INSERT_ID();

    -- Insert into Transactions table
    INSERT INTO Transactions (visit_id, total_bill)
    VALUES (new_visit_id, p_food_bill + p_alcohol_bill + p_tip_amount);
END;
"

dbSendStatement(con, query_storeVisit)
print("Stored procedure `storeVisit` created successfully.")

# ---------------------- CREATE `storeNewVisit` STORED PROCEDURE ---------------------- #
query_storeNewVisit <- "
CREATE PROCEDURE storeNewVisit(
    IN p_customer_name VARCHAR(255),
    IN p_restaurant_name VARCHAR(255),
    IN p_location VARCHAR(255),
    IN p_visit_date DATE,
    IN p_party_size INT,
    IN p_server_name VARCHAR(255),
    IN p_food_bill DECIMAL(10,2),
    IN p_alcohol_bill DECIMAL(10,2),
    IN p_tip_amount DECIMAL(10,2)
)
BEGIN
    DECLARE v_customer_id INT;
    DECLARE v_restaurant_id INT;
    DECLARE v_server_id INT;
    DECLARE new_visit_id INT;

    -- Check if Customer Exists
    SELECT customer_id INTO v_customer_id FROM Customers WHERE customer_name = p_customer_name LIMIT 1;
    IF v_customer_id IS NULL THEN
        INSERT INTO Customers (customer_name, loyalty_program) VALUES (p_customer_name, 0);
        SET v_customer_id = LAST_INSERT_ID();
    END IF;

    -- Check if Restaurant Exists
    SELECT restaurant_id INTO v_restaurant_id FROM Restaurants WHERE restaurant_name = p_restaurant_name AND location = p_location LIMIT 1;
    IF v_restaurant_id IS NULL THEN
        INSERT INTO Restaurants (restaurant_name, location) VALUES (p_restaurant_name, p_location);
        SET v_restaurant_id = LAST_INSERT_ID();
    END IF;

    -- Check if Server Exists
    SELECT server_id INTO v_server_id FROM Servers WHERE server_name = p_server_name LIMIT 1;
    IF v_server_id IS NULL THEN
        INSERT INTO Servers (server_name) VALUES (p_server_name);
        SET v_server_id = LAST_INSERT_ID();
    END IF;

    -- Insert into Visits table
    INSERT INTO Visits (customer_id, restaurant_id, visit_date, party_size, server_id, food_bill, alcohol_bill, tip_amount)
    VALUES (v_customer_id, v_restaurant_id, p_visit_date, p_party_size, v_server_id, p_food_bill, p_alcohol_bill, p_tip_amount);
    
    -- Retrieve last inserted visit ID
    SET new_visit_id = LAST_INSERT_ID();

    -- Insert into Transactions table
    INSERT INTO Transactions (visit_id, total_bill)
    VALUES (new_visit_id, p_food_bill + p_alcohol_bill + p_tip_amount);
END;
"

dbSendStatement(con, query_storeNewVisit)
print("Stored procedure `storeNewVisit` created successfully.")

# ---------------------- TEST `storeVisit` PROCEDURE ---------------------- #
test_storeVisit <- "
CALL storeVisit(1, 1, '2025-03-10', 4, 2, 50.00, 20.00, 10.00);
"
dbSendStatement(con, test_storeVisit)
print("Test for `storeVisit` executed successfully.")

# ---------------------- TEST `storeNewVisit` PROCEDURE ---------------------- #
test_storeNewVisit <- "
CALL storeNewVisit('John Doe', 'McDonalds', 'Boston', '2025-03-10', 4, 'Alice', 50.00, 20.00, 10.00);
"
dbSendStatement(con, test_storeNewVisit)
print("Test for `storeNewVisit` executed successfully.")

# Close connection
dbDisconnect(con)
print("Disconnected from database after configuring business logic.")

