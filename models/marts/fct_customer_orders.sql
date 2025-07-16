-- STAGING
WITH customers as (
    select * from {{ref('stg_customers')}}
),

orders as (
    select * from {{ref('stg_orders')}}
),

payments as (
    select * from {{ref('stg_payments')}}
),

-- MARTS

customer_order_history as (
    SELECT 
        b.customer_id,
        b.full_name,
        b.surname,
        b.givenname,

        MIN(order_date) AS first_order_date,

        MIN(CASE 
            WHEN a.order_status NOT IN ('returned', 'return_pending') 
            THEN order_date 
        END) AS first_non_returned_order_date,

        MAX(CASE 
            WHEN a.order_status NOT IN ('returned', 'return_pending') 
            THEN order_date 
        END) AS most_recent_non_returned_order_date,

        COALESCE(MAX(user_order_seq), 0) AS order_count,

        COALESCE(
            COUNT(CASE WHEN a.order_status != 'returned' 
                THEN 1 
            END), 0
        ) AS non_returned_order_count,

        SUM(CASE 
            WHEN a.order_status NOT IN ('returned', 'return_pending') 
            THEN c.payment_round_amount
            ELSE 0 
        END) AS total_lifetime_value,

        SUM(CASE 
            WHEN a.order_status NOT IN ('returned', 'return_pending') 
            THEN c.payment_round_amount
            ELSE 0 
        END) / NULLIF(
            COUNT(CASE 
                    WHEN a.order_status NOT IN ('returned', 'return_pending') 
                    THEN 1 
                END), 0
        ) AS avg_non_returned_order_value,
                                        
        ARRAY_AGG(DISTINCT a.order_id) AS order_ids

    FROM 
        orders a
    JOIN 
        customers b ON a.customer_id = b.customer_id
    LEFT OUTER JOIN 
        payments c ON a.order_id = c.order_id

    WHERE 
        a.order_status NOT IN ('pending') 
        AND c.payment_status != 'fail'

    GROUP BY 
        b.customer_id, b.full_name, b.surname, b.givenname
),

-- Final CTEs

final AS (
    SELECT 
        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        customer_order_history.first_order_date,
        customer_order_history.order_count,
        customer_order_history.total_lifetime_value,
        payments.payment_round_amount AS order_value_dollars,
        orders.order_status,
        payments.payment_status

    FROM 
        orders 

    JOIN 
        customers ON orders.customer_id = customers.customer_id

    JOIN 
        customer_order_history ON orders.customer_id = customer_order_history.customer_id

    LEFT OUTER JOIN 
        payments ON orders.order_id = payments.order_id

    WHERE 
        payments.payment_status != 'fail'
)

-- Simple select statement

select * from final