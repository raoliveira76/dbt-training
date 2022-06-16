SELECT 
    -- from raw orders
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
    {{ markup('ordersellingprice', 'ordercostprice') }} AS markup
FROM {{ ref('raw_orders') }} AS o
    LEFT JOIN {{ ref('raw_customer') }} AS c
        ON o.customerid = c.customerid
    LEFT JOIN {{ ref('raw_product') }} AS p
        ON o.productid = p.productid
{{ limit_data_in_dev('orderdate') }}