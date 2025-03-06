# Ecommerce Dataset: Exploratory Data Analysis (EDA) and Cohort Analysis in SQL

## Overview
This project focuses on analyzing TheLook's ecommerce dataset, which includes customers, products, orders, logistics, web events, and digital marketing campaigns. The objective is to derive insights from the dataset using SQL queries and perform exploratory data analysis (EDA) along with cohort analysis to understand customer behavior.

## Dataset Description
The dataset comprises the following key tables:
- **Orders**: Contains records of customer orders.
- **Order_Items**: Details the items purchased in each order.
- **Products**: Contains product details, including price, brand, and category.
- **Users**: Contains customer demographic details.

## Objectives
1. Monthly customer and order analysis.
2. Average order value (AOV) and unique customer trends.
3. Customer segmentation based on age.
4. Identifying the top-performing products each month.
5. Category-wise revenue trends.
6. Creating a dataset for dashboard visualization.
7. Conducting a retention cohort analysis.

---
## SQL Queries and Insights

### 1. Monthly Customer and Order Analysis
**Query:**
```sql
SELECT
  FORMAT_DATE('%Y-%m', b.delivered_at) AS month_year,
  COUNT(b.order_id) AS total_order,
  COUNT(DISTINCT b.user_id) AS total_user
FROM bigquery-public-data.thelook_ecommerce.order_items AS b
WHERE
  FORMAT_DATE('%Y-%m', b.created_at) BETWEEN '2019-01' AND '2022-04'
  AND b.status='Complete'
GROUP BY 1
ORDER BY 1;
```
**Insights:**
- Order volume and customer count generally increased over time.
- Peak sales were observed in Q4 (October - December), likely due to holiday promotions.
- January 2022 saw an unusual surge in sales, possibly due to a marketing campaign.

### 2. Average Order Value (AOV) & Unique Customers per Month
**Query:**
```sql
SELECT
  FORMAT_DATE('%Y-%m', b.created_at) AS month_year,
  ROUND(SUM(b.sale_price)/COUNT(b.sale_price),2) AS average_order_value,
  COUNT(DISTINCT b.user_id) AS distinct_user
FROM bigquery-public-data.thelook_ecommerce.order_items AS b
WHERE
  FORMAT_DATE('%Y-%m', b.created_at) BETWEEN '2019-01' AND '2022-04'
GROUP BY 1
ORDER BY 1;
```
**Insights:**
- AOV fluctuated over time but remained relatively stable.
- Customer base expanded, indicating effective marketing strategies.

### 3. Customer Age Segmentation
**Query:**
```sql
WITH CTE AS (
    SELECT *,
           DENSE_RANK() OVER(PARTITION BY gender ORDER BY age) AS stt
    FROM bigquery-public-data.thelook_ecommerce.users),
CTE_2 AS (
    SELECT *,
           MIN(stt) OVER(PARTITION BY gender) AS min_stt,
           MAX(stt) OVER(PARTITION BY gender) AS max_stt
    FROM CTE)
SELECT first_name, last_name, gender, age,
       (CASE WHEN stt=min_stt THEN 'youngest' ELSE 'oldest' END) AS tag,
       COUNT(*) OVER(PARTITION BY gender, age)
FROM CTE_2 AS a
JOIN bigquery-public-data.thelook_ecommerce.order_items AS b ON a.id = b.id
WHERE stt IN (max_stt, min_stt) 
  AND FORMAT_DATE('%Y-%m', b.created_at) BETWEEN '2019-01' AND '2022-04'
ORDER BY gender, age;
```
**Insights:**
- Youngest customer: 12 years old (569 female users, 546 male users).
- Oldest customer: 70 years old (525 female users, 529 male users).

### 4. Top 5 Products Each Month by Profit
**Query:**
```sql
WITH CTE AS (
    SELECT FORMAT_DATE('%Y-%m', a.created_at) AS month_year, a.product_id, a.product_name,
           SUM(b.sale_price) - SUM(a.cost) AS profit
    FROM bigquery-public-data.thelook_ecommerce.inventory_items a
    JOIN bigquery-public-data.thelook_ecommerce.order_items b ON a.product_id = b.product_id
    WHERE b.status = 'Complete'
    GROUP BY month_year, a.product_id, a.product_name),
CTE_2 AS (
    SELECT *, 
           DENSE_RANK() OVER(PARTITION BY month_year ORDER BY profit DESC) AS rank_per_month
    FROM CTE)
SELECT *
FROM CTE_2
WHERE rank_per_month <= 5;
```

### 5. Daily Revenue per Category (Last 3 Months)
**Query:**
```sql
SELECT FORMAT_DATE('%Y-%m-%d', a.delivered_at) AS dates, b.category,
       ROUND(SUM(a.sale_price),2) AS revenue
FROM bigquery-public-data.thelook_ecommerce.order_items a
JOIN bigquery-public-data.thelook_ecommerce.products b ON a.product_id = b.id
WHERE a.status = 'Complete' AND a.delivered_at BETWEEN '2022-01-15' AND '2022-04-15'
GROUP BY b.category, dates
ORDER BY dates;
```

---
## Retention Cohort Analysis
**Query:**
```sql
WITH user_index AS (
  SELECT user_id, FORMAT_DATE('%Y-%m', first_purchase) AS cohort_month, amount,
         (EXTRACT(year FROM created_at) - EXTRACT(year FROM first_purchase)) * 12 + 
         (EXTRACT(month FROM created_at) - EXTRACT(month FROM first_purchase)) + 1 AS index
  FROM (
    SELECT user_id, created_at,
           MIN(created_at) OVER(PARTITION BY user_id) AS first_purchase,
           ROUND(sale_price,2) AS amount
    FROM bigquery-public-data.thelook_ecommerce.order_items
  ) AS a),
xxx AS (
  SELECT cohort_month, index,
         COUNT(DISTINCT user_id) AS user_count,
         ROUND(SUM(amount),2) AS revenue
  FROM user_index
  GROUP BY cohort_month, index
  ORDER BY index)
SELECT cohort_month,
       ROUND(100.00 * m1/m1,2) || '%' AS m1,
       ROUND(100.00 * m2/m1,2) || '%' AS m2,
       ROUND(100.00 * m3/m1,2) || '%' AS m3,
       ROUND(100.00 * m4/m1,2) || '%' AS m4
FROM (
  SELECT cohort_month,
         SUM(CASE WHEN index = 1 THEN user_count ELSE 0 END) AS m1,
         SUM(CASE WHEN index = 2 THEN user_count ELSE 0 END) AS m2,
         SUM(CASE WHEN index = 3 THEN user_count ELSE 0 END) AS m3,
         SUM(CASE WHEN index = 4 THEN user_count ELSE 0 END) AS m4
  FROM xxx
  GROUP BY cohort_month
) AS customer_cohort;
```

**Insights:**
- New customer acquisition increased over time.
- Retention rates remained low (<10%) until Q4 2023, where they improved slightly.
- Suggests a need for better retention strategies like loyalty programs and personalized offers.

