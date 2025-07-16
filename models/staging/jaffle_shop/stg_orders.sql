SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status AS order_status,
    ROW_NUMBER() OVER 
        (PARTITION BY user_id ORDER BY order_date, id) AS user_order_seq
FROM 
    {{source('jaffle_shop','orders')}}
