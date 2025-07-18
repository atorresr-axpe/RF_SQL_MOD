SELECT
    id AS payments_id,
    orderid AS order_id,
    paymentmethod AS payment_method,
    status AS payment_status,
    amount AS payment_amount,
    ROUND(amount / 100.0, 2) AS payment_round_amount,
    created
FROM 
    {{source('stripe','payments')}}
