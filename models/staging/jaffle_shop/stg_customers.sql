SELECT
    id AS customer_id,
    last_name AS surname,
    first_name AS givenname,
    first_name || ' ' || last_name AS full_name
FROM 
    {{source('jaffle_shop','customers')}}