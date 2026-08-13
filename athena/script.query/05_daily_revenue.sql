SELECT order_date, COUNT(DISTINCT order_id) AS total_orders,
SUM(revenue) AS total_revenue FROM customer_orders
GROUP BY order_date ORDER BY order_date;
