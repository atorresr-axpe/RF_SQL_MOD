-- STAGING
WITH 

customers as (
    select * from {{ref('stg_customers')}}
),

-- INTERMEDIATE

orders as (
    select * from {{ref('int_orders')}}
),

----------

customer_orders AS (
    SELECT

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        MIN(orders.order_date) OVER (
            PARTITION BY orders.customer_id
        ) AS customer_first_order_date,

        MIN(orders.valid_order_date) OVER (
            PARTITION BY orders.customer_id
        ) AS customer_first_non_returned_order_date,

        MAX(orders.valid_order_date) OVER (
            PARTITION BY orders.customer_id
        ) AS customer_most_recent_non_returned_order_date,

        COUNT(*) OVER (
            PARTITION BY orders.customer_id
        ) AS customer_order_count,

        COALESCE(
            COUNT(CASE 
                WHEN orders.order_status != 'returned' THEN 1
            END) OVER (PARTITION BY orders.customer_id), 
            0
        ) AS customer_non_returned_order_count,

        SUM(CASE 
            WHEN orders.valid_order_date IS NOT NULL THEN orders.order_value_dollars
            ELSE 0 
        END) over (partition by orders.customer_id) AS customer_total_lifetime_value

    FROM 
        orders
    INNER JOIN 
        customers ON orders.customer_id = customers.customer_id
),

array_int_cte as (
select
    orders.customer_id,
    ARRAY_AGG(DISTINCT orders.order_id) as customer_orders_id
from
    orders
group by
    orders.customer_id
),

add_avg_non_returned_order_value as (
select
    customer_orders.*,
    customer_orders.customer_total_lifetime_value / nullif(customer_orders.customer_non_returned_order_count,0) as customer_avg_non_returned_order_value,
    array_int_cte.customer_orders_id
from
    customer_orders left join array_int_cte
    on customer_orders.customer_id = array_int_cte.customer_id
),


-- MART

final as (
    select
        order_id,
        customer_id,
        surname,
        givenname,
        customer_first_order_date as first_order_date,
        customer_order_count as order_count,
        customer_total_lifetime_value as total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status
    from 
        add_avg_non_returned_order_value
)

-- Simple select statement

select * from final