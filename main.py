# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

print(pd.read_sql("""SELECT * FROM sqlite_master""", conn))


# STEP 1 — Employees in Boston
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)

print("\nSTEP 1:\n", df_boston)


# STEP 2 — Offices with Zero Employees
df_zero_emp = pd.read_sql("""
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
GROUP BY o.officeCode
HAVING COUNT(e.employeeNumber) = 0
""", conn)

print("\nSTEP 2:\n", df_zero_emp)


# STEP 3 — All Employees + Office Location
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName
""", conn)

print("\nSTEP 3:\n", df_employee)


# STEP 4 — Customers Without Orders
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName
""", conn)

print("\nSTEP 4:\n", df_contacts)


# STEP 5 — Payments (CAST for correct sorting)
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

print("\nSTEP 5:\n", df_payment)


# STEP 6 — Employees with High Credit Customers
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName,
       COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
LIMIT 4
""", conn)

print("\nSTEP 6:\n", df_credit)


# STEP 7 — Product Sales Analysis
df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productName
ORDER BY totalunits DESC
""", conn)

print("\nSTEP 7:\n", df_product_sold)


# STEP 8 — Number of Customers per Product
df_total_customers = pd.read_sql("""
SELECT p.productName, p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
""", conn)

print("\nSTEP 8:\n", df_total_customers)


# STEP 9 — Customers per Office
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
""", conn)

print("\nSTEP 9:\n", df_customers)


# STEP 10 — Subquery (Customers with < 20 Orders)
df_under_20 = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
WHERE e.employeeNumber IN (
    SELECT DISTINCT c.salesRepEmployeeNumber
    FROM customers c
    JOIN orders o ON c.customerNumber = o.customerNumber
    GROUP BY c.customerNumber
    HAVING COUNT(o.orderNumber) < 20
)
""", conn)

print("\nSTEP 10:\n", df_under_20)


# Close connection
conn.close()