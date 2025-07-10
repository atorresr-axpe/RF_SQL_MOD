-- Import CTEs 

WITH 

base_customers AS (
select * from  {{source('jaffle_shop','customers')}}
),

base_orders as (
select * from  {{source('jaffle_shop','orders')}}
),

payments as (
select * from  {{source('stripe','payments')}}
),

-- Logical CTEs

customers as (
    select 
        first_name || ' ' || last_name as name, 
        * 
      from base_customers
),

orders as (
    select 
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        *
      from base_orders
),

customer_order_history as (
    SELECT 
        b.id AS customer_id,
        b.name AS full_name,
        b.last_name AS surname,
        b.first_name AS givenname,

        MIN(order_date) AS first_order_date,

        MIN(CASE 
            WHEN a.status NOT IN ('returned', 'return_pending') 
            THEN order_date 
        END) AS first_non_returned_order_date,

        MAX(CASE 
            WHEN a.status NOT IN ('returned', 'return_pending') 
            THEN order_date 
        END) AS most_recent_non_returned_order_date,

        COALESCE(MAX(user_order_seq), 0) AS order_count,

        COALESCE(
            COUNT(CASE WHEN a.status != 'returned' 
                THEN 1 
            END), 0
        ) AS non_returned_order_count,

        SUM(CASE 
            WHEN a.status NOT IN ('returned', 'return_pending') 
            THEN ROUND(c.amount / 100.0, 2) 
            ELSE 0 
        END) AS total_lifetime_value,

        SUM(CASE 
            WHEN a.status NOT IN ('returned', 'return_pending') 
            THEN ROUND(c.amount / 100.0, 2) 
            ELSE 0 
        END) / NULLIF(
            COUNT(CASE 
                    WHEN a.status NOT IN ('returned', 'return_pending') 
                    THEN 1 
                END), 0
        ) AS avg_non_returned_order_value,
                                        
        ARRAY_AGG(DISTINCT a.id) AS order_ids

    FROM 
        orders a
    JOIN 
        customers b ON a.user_id = b.id
    LEFT OUTER JOIN 
        payments c ON a.id = c.orderid

    WHERE 
        a.status NOT IN ('pending') 
        AND c.status != 'fail'

    GROUP BY 
        b.id, b.name, b.last_name, b.first_name
),

-- Final CTEs

final AS (
    SELECT 
        orders.id AS order_id,
        orders.user_id AS customer_id,
        last_name AS surname,
        first_name AS givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        ROUND(amount / 100.0, 2) AS order_value_dollars,
        orders.status AS order_status,
        payments.status AS payment_status

    FROM 
        orders 

    JOIN 
        customers ON orders.user_id = customers.id

    JOIN 
        customer_order_history ON orders.user_id = customer_order_history.customer_id

    LEFT OUTER JOIN 
        payments ON orders.id = payments.orderid

    WHERE 
        payments.status != 'fail'
)

-- Simple select statement

select * from final