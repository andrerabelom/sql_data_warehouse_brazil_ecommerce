SELECT
  order_id,
  SUM(payment_value) AS total_payment_value,
  MAX(payment_installments) AS max_payment_installments,
  FIRST(payment_type) AS first_occuring_payment_type
FROM (
  SELECT
    order_id,
    payment_value,
    payment_installments,
    payment_type,
    ROW_NUMBER() OVER (PARTITION BY order_id, payment_type ORDER BY payment_value DESC) AS rn,
    COUNT(*) OVER (PARTITION BY order_id, payment_type) AS payment_type_count
  FROM brazilian_ecommerce.silver.payments
)
WHERE rn = 1
GROUP BY order_id
