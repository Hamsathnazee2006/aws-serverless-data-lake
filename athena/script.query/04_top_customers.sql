SELECT customer_id, customer_name, COUNT(DISTINCT order_id) AS total_orders,
SUM(revenue) AS total_spent FROM customer_orders
GROUP BY customer_id, customer_name ORDER BY total_spent DESC LIMIT 10;
