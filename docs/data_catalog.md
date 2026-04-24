# Data Catalog for Gold Layer

## Overview  
The Gold Layer represents business-ready data designed for analysis and reporting. It includes **dimension tables** and **fact tables** that support key business metrics.

---

## 1. gold.dim_customers  
**Purpose:** Contains customer information along with demographic and location details.  

### Columns:

| Column Name      | Data Type     | Description                                                                 |
|------------------|---------------|-----------------------------------------------------------------------------|
| customer_key     | INT           | A unique surrogate key for each customer record in this table.              |
| customer_id      | INT           | A unique numeric ID assigned to each customer.                              |
| customer_number  | NVARCHAR(50)  | A text-based identifier used to track and reference the customer.           |
| first_name       | NVARCHAR(50)  | The customer’s first name.                                                  |
| last_name        | NVARCHAR(50)  | The customer’s last name.                                                   |
| country          | NVARCHAR(50)  | The country where the customer lives (e.g., 'Australia').                   |
| marital_status   | NVARCHAR(50)  | The customer’s marital status (e.g., 'Married', 'Single').                  |
| gender           | NVARCHAR(50)  | The customer’s gender (e.g., 'Male', 'Female', 'N/A').                      |
| birthdate        | DATE          | The customer’s date of birth (format: YYYY-MM-DD).                          |
| create_date      | DATE          | The date when the customer record was created.                              |

---

## 2. gold.dim_products  
**Purpose:** Stores details about products and their attributes.  

### Columns:

| Column Name         | Data Type     | Description                                                                 |
|---------------------|---------------|-----------------------------------------------------------------------------|
| product_key         | INT           | A unique surrogate key for each product record.                             |
| product_id          | INT           | A unique ID used to identify the product internally.                        |
| product_number      | NVARCHAR(50)  | A code used to represent and track the product.                             |
| product_name        | NVARCHAR(50)  | The name of the product, including details like type, color, or size.       |
| category_id         | NVARCHAR(50)  | An ID that links the product to its category.                               |
| category            | NVARCHAR(50)  | The main category of the product (e.g., Bikes, Components).                 |
| subcategory         | NVARCHAR(50)  | A more specific group within the category.                                  |
| maintenance_required| NVARCHAR(50)  | Shows if the product needs maintenance ('Yes' or 'No').                     |
| cost                | INT           | The base cost of the product.                                               |
| product_line        | NVARCHAR(50)  | The product line or series (e.g., Road, Mountain).                          |
| start_date          | DATE          | The date when the product became available.                                 |

---

## 3. gold.fact_sales  
**Purpose:** Stores sales transaction data for analysis.  

### Columns:

| Column Name     | Data Type     | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | A unique ID for each sales order (e.g., 'SO54496').                         |
| product_key     | INT           | Links the order to a product in the product table.                          |
| customer_key    | INT           | Links the order to a customer in the customer table.                        |
| order_date      | DATE          | The date when the order was placed.                                         |
| shipping_date   | DATE          | The date when the order was shipped.                                        |
| due_date        | DATE          | The date when payment is due.                                               |
| sales_amount    | INT           | Total value of the sale for that item.                                      |
| quantity        | INT           | Number of units ordered.                                                    |
| price           | INT           | Price per unit of the product.                                              |
