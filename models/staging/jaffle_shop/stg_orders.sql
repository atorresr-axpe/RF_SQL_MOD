SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status AS order_status,

    -- Secuencia de pedidos por cliente, ordenados por fecha e ID
    ROW_NUMBER() OVER (
        PARTITION BY user_id 
        ORDER BY order_date, id
    ) AS user_order_seq,

    -- Fecha del pedido solo si no fue devuelto
    CASE 
        WHEN status NOT IN ('returned', 'return_pending') 
        THEN order_date 
    END AS valid_order_date

FROM 
    {{ source('jaffle_shop', 'orders') }}

