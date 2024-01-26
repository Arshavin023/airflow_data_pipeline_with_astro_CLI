                        
INSERT INTO azeeolow8759_analytics.agg_public_holiday (
    ingestion_date,
    tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar, tt_order_hol_apr, tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul, 
    tt_order_hol_aug, tt_order_hol_sep, tt_order_hol_oct, tt_order_hol_nov, tt_order_hol_dec)
                            
SELECT
    CURRENT_DATE::DATE AS ingestion_date,
    CAST(SUM(CASE WHEN month_of_the_year_num = 1 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_jan,
    CAST(SUM(CASE WHEN month_of_the_year_num = 2 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_feb,
    CAST(SUM(CASE WHEN month_of_the_year_num = 3 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_mar,
    CAST(SUM(CASE WHEN month_of_the_year_num = 4 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_apr,
    CAST(SUM(CASE WHEN month_of_the_year_num = 5 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_may,
    CAST(SUM(CASE WHEN month_of_the_year_num = 6 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_jun,
    CAST(SUM(CASE WHEN month_of_the_year_num = 7 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_jul,
    CAST(SUM(CASE WHEN month_of_the_year_num = 8 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_aug,
    CAST(SUM(CASE WHEN month_of_the_year_num = 9 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_sep,
    CAST(SUM(CASE WHEN month_of_the_year_num = 10 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_oct,
    CAST(SUM(CASE WHEN month_of_the_year_num = 11 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_nov,
    CAST(SUM(CASE WHEN month_of_the_year_num = 12 THEN quantity ELSE 0 END) AS INT) AS tt_order_hol_dec
FROM
    azeeolow8759_staging.orders
LEFT JOIN
    if_common.dim_dates ON TO_DATE(order_date, 'YYYY-MM-DD') = calendar_dt
WHERE
    year_num = 2022
    AND day_of_the_week_num BETWEEN 1 AND 5
    AND working_day = 'false';



INSERT INTO azeeolow8759_analytics.agg_shipments (
    ingestion_date,
    tt_late_shipments,
    tt_undelivered_items)

SELECT
    CURRENT_DATE::DATE AS ingestion_date,
    COUNT(CASE WHEN (CAST(shipment_date AS date) - CAST(order_date AS date)) >= 6 AND DELIVERY_DATE IS NULL AND SHIPMENT_DATE >= ORDER_DATE THEN SHIPMENT_ID END) AS tt_late_shipments,
    COUNT(CASE WHEN DELIVERY_DATE IS NULL AND SHIPMENT_DATE IS NULL AND CAST ('2022-09-05' AS DATE) >= (CAST(order_date AS date) + INTERVAL '15 days') THEN SHIPMENT_ID END) AS tt_undelivered_items
FROM azeeolow8759_staging.SHIPMENT_DELIVERIES a
INNER JOIN azeeolow8759_staging.orders b ON a.order_id = b.order_id;

WITH MostOrderedDate AS (
    SELECT PRODUCT_ID, CAST(ORDER_DATE AS date)ORDER_DATE, ORDER_COUNT, working_day
    FROM (SELECT PRODUCT_ID, ORDER_DATE, COUNT(*) ORDER_COUNT,
    ROW_NUMBER () OVER (PARTITION BY PRODUCT_ID ORDER BY COUNT(*) DESC) ROW_NUM
    , working_day
    FROM azeeolow8759_staging.ORDERS a inner join if_common.dim_dates b
    on a.order_date::DATE = b.calendar_dt::DATE
    where day_of_the_week_num BETWEEN 1 AND 5
    GROUP BY 1,2,working_day
    ORDER BY PRODUCT_ID, COUNT(*) DESC)U
    WHERE ROW_NUM = 1 
),

ReviewPercentages AS (
    select product_name PRODUCT, a.product_id, 
    (CAST(SUM(CASE WHEN REVIEW=1 THEN 1 ELSE 0 END) AS FLOAT)/COUNT(*))*100  ONE_STAR_REVIEW,
    (CAST(SUM(CASE WHEN REVIEW=2 THEN 1 ELSE 0 END) AS FLOAT)/COUNT(*))*100 TWO_STAR_REVIEW,
    (CAST(SUM(CASE WHEN REVIEW=3 THEN 1 ELSE 0 END) AS FLOAT)/COUNT(*))*100 THREE_STAR_REVIEW,
    (CAST(SUM(CASE WHEN REVIEW=4 THEN 1 ELSE 0 END) AS FLOAT)/COUNT(*))*100 FOUR_STAR_REVIEW,
    (CAST(SUM(CASE WHEN REVIEW=5 THEN 1 ELSE 0 END) AS FLOAT)/COUNT(*))*100 FIVE_STAR_REVIEW,
    count(*) tt_review_points  from
    if_common.dim_products a inner join azeeolow8759_staging.reviews b
    on a.product_id = b.product_id
    group by product_name, a.product_id
    ORDER BY 6 DESC,5 DESC,4 DESC,3 DESC,2 DESC
),

ShipmentPercentages AS (
    SELECT
    c.product_name,c.product_id,
    CAST(COUNT(CASE WHEN b.shipment_date IS NOT NULL AND CAST(b.shipment_date AS date) < CAST(a.order_date AS date) + 6 AND b.delivery_date IS not NULL THEN  a.order_id END) AS FLOAT)/count(*)*100 AS tt_early_shipments,
    CAST(COUNT(CASE WHEN b.shipment_date IS NOT NULL AND (CAST(b.shipment_date AS date) >= CAST(a.order_date AS date) + 6 OR b.delivery_date IS NULL) THEN a.order_id  END)  AS FLOAT)/count(*)*100 AS tt_late_shipments
    from azeeolow8759_staging.orders a
    left join azeeolow8759_staging.SHIPMENT_DELIVERIES b
    ON a.order_id = b.order_id
    left join if_common.dim_products c on a.product_id = c.product_id
    group by 1,2
)
    
INSERT INTO azeeolow8759_analytics.best_performing_product
SELECT 
    current_date AS ingestion_date,
    review_percentages.product AS product_name,
    most_ordered.ORDER_DATE,
    most_ordered.working_day,
    COALESCE(review_percentages.tt_review_points, 0) AS tt_review_points,
    COALESCE(review_percentages.ONE_STAR_REVIEW, 0) AS pct_one_star_review,
    COALESCE(review_percentages.TWO_STAR_REVIEW, 0) AS pct_two_star_review,
    COALESCE(review_percentages.THREE_STAR_REVIEW, 0) AS pct_three_star_review,
    COALESCE(review_percentages.FOUR_STAR_REVIEW, 0) AS pct_four_star_review,
    COALESCE(review_percentages.FIVE_STAR_REVIEW, 0) AS pct_five_star_review,
    COALESCE(shipment_percentages.tt_early_shipments, 0) AS pct_early_shipments,
    COALESCE(shipment_percentages.tt_late_shipments, 0) AS pct_late_shipments
    
    FROM 
    MostOrderedDate most_ordered
    LEFT JOIN 
        ReviewPercentages review_percentages ON most_ordered.product_id = review_percentages.product_id
    LEFT JOIN 
        ShipmentPercentages shipment_percentages ON most_ordered.product_id = shipment_percentages.product_id
    ORDER BY 
        pct_five_star_review DESC, pct_four_star_review DESC, pct_three_star_review DESC, pct_two_star_review DESC,
pct_one_star_review DESC 
LIMIT 1;
