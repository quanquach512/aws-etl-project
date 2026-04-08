
-- 1. Revenue by category
SELECT
    p.category,
    SUM(t.total_amount) AS revenue
FROM fact_transactions t
JOIN dim_products p
    ON t.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;


-- 2. Top products by revenue
SELECT
    p.product_name,
    SUM(t.quantity) AS units_sold,
    SUM(t.total_amount) AS revenue
FROM fact_transactions t
JOIN dim_products p
    ON t.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;


-- 3. Top 3 products within each category
WITH product_revenue AS (
    SELECT
        p.category,
        p.product_name,
        SUM(t.total_amount) AS revenue
    FROM fact_transactions t
    JOIN dim_products p
        ON t.product_id = p.product_id
    GROUP BY p.category, p.product_name
)
SELECT *
FROM (
    SELECT
        *,
        RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rnk
    FROM product_revenue
) ranked
WHERE rnk <= 3;