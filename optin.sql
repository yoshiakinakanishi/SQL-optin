
WITH
 data_open_id AS
 (
    SELECT DISTINCT
     open_id
     , member_id
    FROM
     common_memberprovision.open_id_mapping
    WHERE
     dt BETWEEN CONCAT('{{集計月：yyyy-mm}}', '-01') AND CONCAT('{{集計月：yyyy-mm}}', '-31')
    AND open_id IN ('YVYcs09z2q905aEB','poLecZ6RNGry0vpY')    
 )
 , data_purchase AS
 (
    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     common_purchase.v_view_purchase_detail
    WHERE
     dt BETWEEN CONCAT('{{集計月：yyyy-mm}}', '-01') AND CONCAT('{{集計月：yyyy-mm}}', '-31')
     AND member_id IN (SELECT member_id FROM data_open_id)
 )
 , data_point AS
 (
    SELECT
     account_id AS member_id
     , client_type AS basket_service_type
     , transaction_date AS purchase_date
     , user_device AS device
    FROM
     common_emoney.v_history_view
    WHERE
     dt BETWEEN CONCAT('{{集計月：yyyy-mm}}', '-01') AND CONCAT('{{集計月：yyyy-mm}}', '-31')
     AND account_id IN (SELECT member_id FROM data_open_id)
     AND transaction_type = 'use'
 )
 , union_purchase_point AS
 (
    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     data_purchase
    
    UNION ALL

    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     data_point
 )
 , data_purchase_point AS
 (
    SELECT
     member_id
     , basket_service_type
     , SUBSTR(MIN(purchase_date), 1, 10) AS purchase_dt
     , SUBSTR(MIN(purchase_date), 12, 8) AS purchase_time
     , device
    FROM
     union_purchase_point
    GROUP BY
     member_id
     , basket_service_type
     , device
 )
 , data_activity AS
 (
    SELECT
     member_id
     , i3_service_code
    FROM
     i3.activity
    WHERE
     dt BETWEEN CONCAT('{{集計月：yyyy-mm}}', '-01') AND CONCAT('{{集計月：yyyy-mm}}', '-31')
     AND action = 'view'
     AND option = 'page'
     AND member_id IN (SELECT member_id FROM data_open_id)
 )
 , shukei_activity AS
 (
    SELECT
     member_id
     , i3_service_code
     , COUNT(*) AS times
     , row_number() OVER(PARTITION BY member_id) AS num
    FROM
     data_activity
    GROUP BY
     member_id
     , i3_service_code
 )
 , data_target_service AS
 (
    SELECT DISTINCT
     member_id
     , LAST_VALUE(times) OVER(PARTITION BY member_id) AS max_page
     , FIRST_VALUE(num) OVER(PARTITION BY member_id) AS first_service
    FROM
     shukei_activity
 )
 , join_activty_service AS
 (
    SELECT
     t1.member_id
     , t1.i3_service_code
    FROM
     shukei_activity AS t1
      JOIN
       data_target_service AS t2
      ON
       t1.member_id = t2.member_id
       AND t1.times = t2.max_page
       AND t1.num = t2.first_service
 )
 , join_purchase_activity AS
 (
    SELECT
     t1.member_id
     , t1.basket_service_type
     , t1.purchase_dt
     , t1.purchase_time
     , t1.device
     , t2.i3_service_code
    FROM
     data_purchase_point AS t1
      LEFT JOIN
       join_activty_service AS t2
      ON
       t1.member_id = t2.member_id
 )
 , data_service AS
 (
    SELECT
     i3_service_code
     , purchase_service_type
     , row_number() OVER(PARTITION BY i3_service_code ORDER BY dt DESC) AS service_num
    FROM
     common_i3.site_info_v2
    WHERE
     dt BETWEEN CONCAT('{{集計月：yyyy-mm}}', '-01') AND CONCAT('{{集計月：yyyy-mm}}', '-31')
 )
 , shukei_service AS
 (
    SELECT
     i3_service_code
     , purchase_service_type
    FROM
     data_service
    WHERE
     service_num = 1
 )


SELECT
 t1.member_id
 , t1.basket_service_type
 , t1.purchase_dt
 , t1.purchase_time
 , t1.device
 , t2.purchase_service_type
FROM
 join_purchase_activity AS t1
  LEFT JOIN
   shukei_service AS t2
  ON
   t1.i3_service_code = t2.i3_service_code
ORDER BY
 t1.purchase_dt
 , t1.purchase_time
 , t1.member_id

LIMIT 1000
--NO_ALERT