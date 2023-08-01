-- ============================================================================
--        File : 03.3 flag_parcel_gaps.sql
--      Author : Alex Nielson (alexnielson@utah.gov)
-- Description : Checks for gaps in tax years 


-- ============================================================================
# Takes 3-5 minutes to complete


-- Section 1 ------------------------------------------------------------------

-- Uncomment and run this section each time you run the script or if it is 
--  first time.

-- DROP TABLE IF EXISTS central_dev.parcels_with_gaps;

-- CREATE TABLE central_dev.parcels_with_gaps 
-- (county STRING,
--  parcel_id STRING, 
--  year_impacted INT64);

-- Section 2 ------------------------------------------------------------------
-- Uncomment and run this section the first time you run the script. It does
-- not need to be recreated each time.

-- CREATE OR REPLACE PROCEDURE central_dev.find_parcel_gaps_for_county_year(county_name STRING, year_check INT64)
-- BEGIN
--   DECLARE county_p  STRING;
--   DECLARE year_test_p INT64;
--   SET county_p = county_name;
--   SET year_test_p = year_check;

--   INSERT INTO central_dev.parcels_with_gaps 

--   SELECT county, parcel_id, year_test_p AS year_impacted
--   FROM `ut-sao-tax-prod.central_dev.tax_roll`

--   WHERE parcel_id IN (SELECT parcel_id FROM `ut-sao-tax-prod.central_dev.tax_roll` 
--                       WHERE county = county_p AND year = year_test_p+1) 

--     AND parcel_id IN (SELECT parcel_id FROM `ut-sao-tax-prod.central_dev.tax_roll` 
--                       WHERE county = county_p AND year = year_test_p-1) 

--     AND parcel_id NOT IN (SELECT parcel_id FROM `ut-sao-tax-prod.central_dev.tax_roll` 
--                           WHERE county = county_p AND year = year_test_p)

--     AND year = year_test_p+1 
--     AND county = county_p;
-- END;

-- CALL central_dev.find_parcel_gaps_for_county_year("Salt Lake County", 2022)

-- Section 3 ------------------------------------------------------------------
-- run this section every time. 

DECLARE years_to_check ARRAY<INT64>;
DECLARE i INT64 DEFAULT 0;

SET years_to_check =  [2019, 2020, 2021, 2022];

FOR county_name_to_check IN (SELECT DISTINCT county FROM central_dev.tax_roll ORDER BY county)

DO
  SET i = 0;
  LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(years_to_check) THEN 
    LEAVE; 
  END IF;
  CALL central_dev.find_parcel_gaps_for_county_year(county_name_to_check.county, years_to_check[ORDINAL(i)]);
  END LOOP; 
END FOR;


