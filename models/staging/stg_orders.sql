SELECT 
    -- from raw orders
    {{ dbt_utils.surrogate_key(['o.orderid', 'c.customerid', 'p.productid']) }} AS skorders,
    o.orderid,
    o.orderdate,
    o.shipdate,
    o.shipmode, 
    o.ordersellingprice - o.ordercostprice AS OrderProfit,
    o.ordercostprice,
    o.ordersellingprice,
    -- from raw customer
    c.customerid,
    c.customername,
    c.segment,
    c.country,
    -- from raw product
    p.productid,
    p.category,
    p.productname,
    p.subcategory,
    {{ markup('ordersellingprice', 'ordercostprice') }} AS markup,
    d.delivery_team
FROM {{ ref('raw_orders') }} AS o
    LEFT JOIN {{ ref('raw_customer') }} AS c
        ON o.customerid = c.customerid
    LEFT JOIN {{ ref('raw_product') }} AS p
        ON o.productid = p.productid
    LEFT JOIN {{ ref('delivery_team') }} AS d
        ON o.shipmode = d.shipmode
{{ limit_data_in_dev('orderdate') }}