--1.Thống kê tổng số lượng người mua và số lượng đơn hàng đã hoàn thành mỗi tháng ( Từ 1/2019-4/2022)

SELECT
  FORMAT_DATE( '%Y-%m',b.delivered_at) AS month_year,
  COUNT(b.order_id) AS total_order,
  COUNT(DISTINCT b.user_id) AS total_user,
FROM bigquery-public-data.thelook_ecommerce.order_items AS b
WHERE
  FORMAT_DATE( '%Y-%m',b.created_at) BETWEEN '2019-01' AND '2022-04'
  AND b.status='Complete'
GROUP BY  1
ORDER BY  1

	/*--> Insight: 
    - Nhìn chung số lượng người mua hàng và đơn hàng tiêu thụ đã hoàn thành tăng dần theo mỗi tháng và năm   
    - Giai đoạn 2019-tháng 1 2022: người mua hàng có xu hướng mua sắm nhiều hơn vào ba tháng cuối năm (10-12) và tháng 1 năm kế tiếp do nhu cầu mua sắm cuối/đầu năm tăng 
           và nhiều chương trình khuyến mãi/giảm giá cuối năm           
    - Giai đoạn bốn tháng đầu năm 2022: ghi nhận tỷ lệ lượng người mua tăng mạnh so với ba tháng cuối năm 2021, khả năng do TheLook triển khai chương trình khuyến mãi mới nhằm 
      kích cầu mua sắm các tháng đầu năm
    - Tháng 7 2021 ghi nhận lượng mua hàng tăng bất thường, trái ngược với lượng mua giảm sút so với cùng kì năm 2020, có thể do TheLook triển khai campaign đặc biệt cải thiện tình hình 
      doanh số cho riêng tháng 7.
*/


--2.Thống kê giá trị đơn hàng trung bình và tổng số người dùng khác nhau mỗi tháng ( Từ 1/2019-4/2022)
    --insight: distinct_user tăng dần theo thời gian, average_order_value lúc tăng lúc giảm


SELECT
  FORMAT_DATE( '%Y-%m',b.created_at) AS month_year,
  ROUND(SUM(b.sale_price)/COUNT(b.sale_price),2) AS average_order_value,
  COUNT(DISTINCT b.user_id) AS distinct_user,
  
FROM bigquery-public-data.thelook_ecommerce.order_items AS b
WHERE
  FORMAT_DATE( '%Y-%m',b.created_at) BETWEEN '2019-01' AND '2022-04'
GROUP BY  1
ORDER BY  1
--------



--3. Nhóm khách hàng theo độ tuổi
WITH CTE AS(SELECT *,
                 DENSE_RANK() OVER(PARTITION BY gender ORDER BY age) AS stt
             FROM bigquery-public-data.thelook_ecommerce.users),

 CTE_2 AS(SELECT *,
                 MIN(stt) OVER(PARTITION BY gender ) AS min_stt ,
                 MAX(stt) OVER(PARTITION BY gender )  AS max_stt
         FROM CTE)
SELECT first_name, last_name, gender, age,
       (CASE WHEN stt=min_stt THEN 'youngest' ELSE 'oldest'END) AS c,
        COUNT(*) OVER(PARTITION BY gender, age)
FROM CTE_2 AS a
 JOIN bigquery-public-data.thelook_ecommerce.order_items AS b ON a.id=b.id
 WHERE stt IN (max_stt,min_stt) AND FORMAT_DATE( '%Y-%m',b.created_at) BETWEEN '2019-01' AND '2022-04'
 ORDER BY gender , age 

 /* insight: Male: trẻ nhất là 12t, số lượng: 220
                    lớn nhất 70t, số lượng: 250
            Female: trẻ nhất là 12t, số lượng: 211
                    lớn nhất 70t, số lượng: 218 */

    --------
     WITH female_age AS (
  SELECT min(age) as min_age, max(age) as max_age
  FROM bigquery-public-data.thelook_ecommerce.users
  WHERE gender='F' AND FORMAT_DATE( '%Y-%m',created_at) BETWEEN '2019-01' AND '2022-04'
 ),
 male_age AS (
  SELECT min(age) as min_age, max(age) as max_age
  FROM bigquery-public-data.thelook_ecommerce.users
  WHERE gender='M' AND FORMAT_DATE( '%Y-%m',created_at) BETWEEN '2019-01' AND '2022-04'
 ),
 young_old_group AS (
    (SELECT m1.first_name, m1.last_name, m1.gender, m1.age
    FROM bigquery-public-data.thelook_ecommerce.users m1
    JOIN female_age m2 ON m1.age=m2.min_age OR m1.age=m2.max_age
    WHERE m1.gender='F' AND FORMAT_DATE( '%Y-%m',m1.created_at) BETWEEN '2019-01' AND '2022-04'
    )
    UNION ALL
    (
      SELECT n1.first_name, n1.last_name, n1.gender, n1.age
      FROM  bigquery-public-data.thelook_ecommerce.users n1
      JOIN male_age n2 ON n1.age=n2.min_age OR n1.age=n2.max_age
      WHERE n1.gender='M' AND FORMAT_DATE( '%Y-%m',n1.created_at) BETWEEN '2019-01' AND '2022-04'
    )
 ),
 age_tag AS (
  SELECT *,
      CASE 
          WHEN age IN (select min(age) from bigquery-public-data.thelook_ecommerce.users 
                        where gender='F' and FORMAT_DATE( '%Y-%m',created_at) BETWEEN '2019-01' AND '2022-04')
                THEN 'youngest'
          WHEN age IN (select min(age) from bigquery-public-data.thelook_ecommerce.users 
                        where gender='M' and FORMAT_DATE( '%Y-%m',created_at) BETWEEN '2019-01' AND '2022-04')
                THEN 'youngest'
          ELSE 'oldest'
      END as tag
  FROM young_old_group
 )
 SELECT gender, tag,count(*)
 FROM age_tag
 GROUP BY gender,tag

 -- Insight: trong giai đoạn Từ 1/2019-4/2022
 --      - Giới tính Female: lớn tuổi nhất là 70 tuổi (525 người người dùng); nhỏ tuổi nhất là 12 tuổi (569 người dùng)
 --      - Giới tính Male: lớn tuổi nhất là 70 tuổi (529 người người dùng); nhỏ tuổi nhất là 12 tuổi (546 người dùng)



--4.Thống kê top 5 sản phẩm có lợi nhuận cao nhất từng tháng (xếp hạng cho từng sản phẩm). 
    
 WITH CTE AS (SELECT FORMAT_DATE( '%Y-%m',a.created_at) AS month_year, a.product_id, a.product_name,
                  SUM(b.sale_price)-sum(a.cost)AS profit
                  
            FROM bigquery-public-data.thelook_ecommerce.inventory_items a
            JOIN bigquery-public-data.thelook_ecommerce.order_items b ON a.product_id=b.product_id
            WHERE b.status='Complete'
            GROUP BY month_year, a.product_id, a.product_name
            ),
    CTE_2 AS (SELECT *, 
                      DENSE_RANK() OVER(PARTITION BY month_year ORDER BY profit DESC ) AS rank_per_month
              FROM CTE
              ORDER BY month_year)
SELECT *
FROM CTE_2
WHERE rank_per_month<=5

--5.Thống kê tổng doanh thu theo ngày của từng danh mục sản phẩm (category) trong 3 tháng qua ( giả sử ngày hiện tại là 15/4/2022)
    

SELECT FORMAT_DATE( '%Y-%m-%d', a.delivered_at) AS dates,b.category,
              ROUND(sum(a.sale_price),2) as revenue
FROM bigquery-public-data.thelook_ecommerce.order_items a
Join bigquery-public-data.thelook_ecommerce.products as b on a.product_id=b.id
WHERE  a.status='Complete' and a.delivered_at BETWEEN '2022-01-15'  AND '2022-04-15'
GROUP BY b.category,dates
ORDER BY dates

    ------------------------------------------------------------------------------
--III/
--1/ sử dụng câu lệnh SQL để tạo ra 1 dataset như mong muốn và lưu dataset đó vào VIEW đặt tên là vw_ecommerce_analyst

WITH CTE AS(
        SELECT   FORMAT_DATE( '%Y-%m',b.created_at) AS month_year, c.category as Product_category,
                round(SUM(b.sale_price),2) AS TPV,
                COUNT(b.product_id) AS TPO,
                round(SUM(c.cost),2) AS total_cost
        FROM bigquery-public-data.thelook_ecommerce.orders as a
        JOIN bigquery-public-data.thelook_ecommerce.order_items as b ON a.order_id=b.order_id
        JOIN bigquery-public-data.thelook_ecommerce.products as c ON c.id=b.product_id
        WHERE b.status='Complete'
        GROUP BY month_year, Product_category
        ORDER BY Product_category,month_year)

SELECT month_year,Product_category,TPV, TPO,
    round(100*(TPV-(LAG(TPV) OVER(PARTITION BY Product_category ORDER BY month_year)))/(LAG(TPV) OVER(PARTITION BY Product_category ORDER BY month_year)),2)||'%' AS Revenue_growth,
    round(100*(TPO-(LAG(TPO) OVER(PARTITION BY Product_category ORDER BY month_year)))/(LAG(TPO) OVER(PARTITION BY Product_category ORDER BY month_year)),2)||'%' AS Order_growth,
    total_cost,
    round(TPV-total_cost,2) AS total_profit,
    round((TPV-total_cost)/total_cost,2) AS profit_to_cost_ratio
FROM CTE
ORDER BY Product_category,month_year


 --TẠO COHORT-------------------------------------------------------------------------

WITH user_index AS 
 (SELECT user_id, FORMAT_DATE('%Y-%m',first_purchase) AS  cohort_month, amount,
 (EXTRACT(year FROM created_at)-EXTRACT(year FROM first_purchase))*12
			+ (EXTRACT(month FROM created_at)-EXTRACT(month FROM first_purchase)) +1 AS index
 FROM ( SELECT user_id, created_at,
 Min(created_at) over(partition by user_id) AS first_purchase,
 round(sale_price,2) as amount
 FROM bigquery-public-data.thelook_ecommerce.order_items) AS a),
 xxx AS (
  SELECT cohort_month, index,
  COUNT(distinct user_id) as user_count,
  round(sum(amount),2) AS revenue
  FROM user_index
 GROUP BY cohort_month, index
 ORDER BY index),

 ------Customer cohort ------
 Customer_cohort AS (
 SELECT 
cohort_month,
  SUM(case when index=1 then user_count else 0 end) as m1,
  SUM(case when index=2 then user_count else 0 end) as m2,
  SUM(case when index=3 then user_count else 0 end) as m3,
  SUM(case when index=4 then user_count else 0 end) as m4
from xxx
Group by cohort_month
Order by cohort_month),

---retention cohort-----
Retention_cohort AS (
SELECT cohort_month,
  round(100.00* m1/m1,2) || '%' as m1,
  round(100.00* m2/m1,2) || '%' as m2,
  round(100.00* m3/m1,2) || '%' as m3,
  round(100.00* m4/m1,2) || '%' as m4
FROM customer_cohort),

------CHURN COHORT--
Churn_cohort  AS (
SELECT cohort_month,
  round(100-100*m1/m1,2) ||'%' AS m1,
  round(100-100*m2/m1,2) ||'%' AS m2,
  round(100-100*m3/m1,2) ||'%' AS m3,
  round(100-100*m4/m1,2) ||'%' AS m4
FROM customer_cohort
)

-----cohort chart:    https://docs.google.com/spreadsheets/d/1boZvK7um_qkGgtmL3kzkEGgEjEEIbfuGkTcaO0OaJpk/edit#gid=1988940515
/*
insight:
+ Số lượng khách hàng mới tăng nhanh theo thời gian, có thể là do The Look thực hiện việc quảng bá, marketing tốt
+ tuy nhiên số lượng khách hàng quay lại vào tháng kế tiếp rất thấp, cụ thể:
  - trong giai đoạn 1/2019 - 9/2023: tỉ lệ quay lại dao động khoảng 0-->10%
  - giai đoạn T10,11,12 của 2023: tỉ lệ quay lại trên mức 10%
--> giải pháp: đưa ra chiến lược(cải thiện sản phẩm, chất lượng phục vụ...) nhằm giữ chân khách hàng cũ ,
từ đó tiết kiệm được chi phí marketing cũng như tăng lợi nhuận
*/















