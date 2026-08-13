SELECT COUNT(DISTINCT order_id) AS total_orders,
SUM(quantity) AS total_quantity, SUM(revenue) AS total_revenue
FROM customer_orders;
