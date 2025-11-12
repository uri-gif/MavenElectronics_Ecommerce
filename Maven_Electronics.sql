-- Adhoc request for SALES AND OPERATION Team --
use maven_portfolio;

-- 1.Total Revenue by Product Category --
alter table products
rename column `Unit Cost USD` to UnitCost_USD,
rename column `Unit Price USD` to UnitPrice_USD;
	
select * from products;
select p.Category ,
       sum(p.Unitprice_usd) as Total_price,
       sum(s.Quantity) as Total_quantity
from products as p 
left join  sales as s
on p.ProductKey= s.Productkey
group by 1
order by Total_price desc;

-- 2. Top 10 Best-Selling Products --

select * from products;
select p.Product_name ,
       sum(p.Unitprice_usd) as Total_price,
       sum(s.Quantity) as Total_quantity
from products as p 
left join  sales as s
on p.ProductKey= s.Productkey
group by 1
order by Total_price desc
limit 10;

-- 3. Average sales amount per store for the last 6 months. --
        -- convert date to date format --
 update calendar 
 set `Date`= str_to_date(`Date`, "%m/%d/%Y");
 alter table calendar
 modify column`Date` date;

		-- convert order date to date format in sales table --
update sales 
set `Order Date`=STR_TO_DATE(`Order Date`, "%m/%d/%Y");

alter table sales 
modify column `Order date` date;
                   -- get the latest date -- 
select max(`Order Date`) from sales ;
 
WITH monthly_6_sales AS (
  SELECT 
    s.ProductKey,
    s.StoreKey,
    s.`Order Date`,
    MONTHNAME(s.`Order Date`) AS `Month`,
    SUM(s.Quantity) AS Total_quantity,
    SUM(p.UnitCost_USD) AS Total_price,
    p.Category
  FROM sales AS s
  LEFT JOIN products AS p  
    ON s.ProductKey = p.ProductKey
  WHERE s.`Order Date` >= DATE_SUB('2021-02-20', INTERVAL 6 MONTH)
  GROUP BY s.ProductKey, s.StoreKey, s.`Order Date`, p.Category
)
SELECT 
  s.StoreKey,
  m.`Month`,
  SUM(m.Total_quantity) AS Total_quantity,
  AVG(m.Total_price) AS Avg_price,
  s.Country
FROM monthly_6_sales AS m
LEFT JOIN stores AS s 
  ON m.StoreKey = s.StoreKey
GROUP BY s.StoreKey, m.`Month`, s.Country
ORDER BY MAX(m.`Order Date`) DESC;

-- 4.Inactive customers who haven't made purchase for the last 12 momths --

  WITH last_purchase AS (
  SELECT 
      c.CustomerKey,
      c.`Name`,
      MAX(s.`Order Date`) AS LastPurchaseDate
  FROM customers AS c
  LEFT JOIN sales AS s
      ON c.CustomerKey = s.CustomerKey
  GROUP BY c.CustomerKey
)
SELECT 
    CustomerKey,
    `Name`,
    LastPurchaseDate
FROM last_purchase
WHERE LastPurchaseDate < DATE_SUB('2021-02-20', INTERVAL 12 MONTH)
OR 
   LastPurchaseDate IS NULL;

-- 5.the total Product Profit Margin for the recent year  --
SELECT 
    p.Product_name,
    p.ProductKey,
    SUM((p.UnitPrice_USD - p.UnitCost_USD) * s.Quantity) AS TotalProfit,
    SUM(p.UnitPrice_USD * s.Quantity) AS TotalRevenue,
    (SUM((p.UnitPrice_USD - p.UnitCost_USD) * s.Quantity) 
      / SUM(p.UnitPrice_USD * s.Quantity)) * 100 AS ProfitMargin_Percent
FROM sales s
JOIN products p 
    ON s.ProductKey = p.ProductKey
    where  s.`Order date` <= date_sub( '2021-02-20', interval 12 month)
GROUP BY p.Product_name, p.ProductKey
;

-- 6. Sales Distribution by Day of Week --
                 -- average revenue by day of the week to identify peak shopping days --
 select * from calendar;
 
 with daily_avg_rev as(
 select
        dayname(s.`Order date`) as Days,
        date(s.`Order date`),
         sum(s.Quantity * p.UnitPrice_USD) as Total_revenue
	from sales as s
    left join products as p
on s.ProductKey= p.ProductKey
   where  s.`Order date` >= date_sub( '2021-02-20', interval 12 month)
   group by 1,2
order by 1 desc
)
select  avg(Total_revenue) as h,
       Days
from daily_avg_rev
group by 2

order by 1 desc;
  
-- 7. Percentage of total sales of Online vs. In-Store Sales for the pastnone year

select 
 case when s.StoreKey= 0 then  'online' else 'physical' end as storetype,
 round(sum(s.Quantity * p.UnitPrice_USD),2) as total_sales,
 round(
	(sum(s.Quantity * p.UnitPrice_USD)* 100)/
      sum(sum(s.Quantity * p.UnitPrice_USD)) over (),2)
       as perc_sales
from sales as s
      left join products as p
on s.ProductKey=p.ProductKey
 where  s.`Order date` >= date_sub( '2021-02-20', interval 12 month)
group by storetype ;

-- 8. Monthly Sales Growth Rate --
-- Display total monthly revenue and the month-over-month growth percentage for last year. --
 

      WITH tov AS (
    SELECT 
        MONTHNAME(s.`Order date`) AS month_name,
        MONTH(s.`Order date`) AS month_num,
        SUM(s.Quantity * p.UnitCost_USD) AS sales
    FROM sales AS s 
    LEFT JOIN products AS p
        ON s.ProductKey = p.ProductKey 
         where s.`Order date`  >= date_sub( '2021-02-20', interval 12 month)
    GROUP BY MONTHNAME(s.`Order date`), MONTH(s.`Order date`)
)
SELECT 
    month_name,
    ROUND(sales, 2) AS total_sales,
    ROUND(sales - LAG(sales) OVER (ORDER BY month_num), 2) AS diff_from_prev_month,
      ROUND(
      (sales - LAG(sales) OVER (ORDER BY month_num))/
      (lag(sales) OVER (ORDER BY month_num)) *100, 2)
       as growth_percent 
FROM tov
ORDER BY month_num;

-- 9. For each store, find the top 3 best-selling products based on total revenue--

with top_pdt as
(select (s.Quantity * p.UnitCost_USD) as sales,
        s.StoreKey,
        p.Product_name,
        row_number()over(partition by s.StoreKey order by (s.Quantity * p.UnitCost_USD) desc) as PriceRank 
from sales as s left join
products as p on
s.ProductKey=p.ProductKey
) 
select 
      product_name,
      sales,
      StoreKey,
       PriceRank 
      from top_pdt
where PriceRank <= '3';

        