SELECT product_id, product_name, SUM(quantity) AS total_quantity,
SUM(revenue) AS total_revenue FROM customer_orders
GROUP BY product_id, product_name ORDER BY total_revenue DESC LIMIT 10;
