CREATE OR REPLACE TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` AS 
-- (
--   SELECT 
--     county, 
--     district,
--     year, 
--     parcel_id, 
--     property_type, 
--     acres, 
--     CAST(sq_feet AS FLOAT64) AS sq_feet, 
--     market, 
--     land, 
--     improvements, 
--     CAST(tax_exempt AS STRING) AS tax_exempt,
--     CAST(tax_exempt_type AS STRING) AS tax_exempt_type,
--     situs_city, 
--     year_built,
--     situs_address1,
--     NULL AS market_change_perc_2019,
--     NULL AS market_change_perc_2020,
--     NULL AS market_change_perc_2021,
--     NULL AS market_change_perc_2022,
--     NULL AS market_change_perc_2023


--   FROM `ut-sao-tax-prod.central_dev.tax_roll` 

-- )

-- UNION ALL ( 
  SELECT  
    county, 
    district,
    year, 
    parcel_id, 
    property_type, 
    acres, 
    sq_feet, 
    market, 
    land, 
    improvements, 
    CAST(tax_exempt AS STRING) AS tax_exempt,
    CAST(tax_exempt_type AS STRING) AS tax_exempt_type,
    situs_city, 
    year_built,
    situs_address1,
    market_change_perc_2019,
    market_change_perc_2020,
    market_change_perc_2021,
    market_change_perc_2022,
    market_change_perc_2023,
    geometry


  FROM `ut-sao-tax-prod.central_dev.tax_roll_stats` 

-- )


;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN neg_mrkt_val INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 neg_mrkt_val = (CASE 
          WHEN market < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN neg_lnd_val INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 neg_lnd_val = (CASE 
          WHEN land < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN neg_bldngs_val INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 neg_bldngs_val = (CASE 
          WHEN improvements < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN missing_market_val INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 missing_market_val = (CASE 
          WHEN market IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;



ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN future_build_date INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 future_build_date = (CASE 
          WHEN year_built > 2024 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN pre_utah_build_date INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 pre_utah_build_date = (CASE 
          WHEN year_built < 1847 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN no_prop_type INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 no_prop_type = (CASE          
          WHEN property_type IS NULL OR 
  TRIM(property_type) = "" OR TRIM(property_type) = "NULL" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN zero_sqr_ft INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 zero_sqr_ft = (CASE          
          WHEN sq_feet <= 0 and improvements > 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN zero_acres INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 zero_acres = (CASE          
          WHEN acres <= 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN missing_parcel_id INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 missing_parcel_id = (CASE          
          WHEN parcel_id IS NULL OR 
  TRIM(parcel_id) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN no_situs_city INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 no_situs_city = (CASE          
          WHEN situs_city IS NULL OR 
  TRIM(situs_city) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN no_situs_address INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 no_situs_address = (CASE          
          WHEN situs_address1 IS NULL OR 
  TRIM(situs_address1) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN no_dist_id INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 no_dist_id = (CASE          
          WHEN district IS NULL OR TRIM(district) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


--run these in a separate tabe to avoid an error
ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN big_2019_market_change INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 big_2019_market_change = (CASE          
          WHEN market_change_perc_2019 > 300 AND year_built!= 2018  THEN 1
          ELSE 0
        END) 
WHERE TRUE;



ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN big_2020_market_change INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 big_2020_market_change = (CASE          
          WHEN market_change_perc_2020 > 300 AND year_built!= 2019 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN big_2021_market_change INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 big_2021_market_change = (CASE          
          WHEN market_change_perc_2021 > 300 AND year_built!= 2020 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN big_2022_market_change INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 big_2022_market_change = (CASE          
          WHEN market_change_perc_2022 > 300 AND year_built!= 2021 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN big_2023_market_change INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 big_2023_market_change = (CASE          
          WHEN market_change_perc_2023 > 300 AND year_built!= 2022 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN missing_exempt_status INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 missing_exempt_status = (CASE          
          WHEN tax_exempt IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN missing_exempt_type INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 missing_exempt_type = (CASE          
          WHEN tax_exempt_type IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN missing_geometry INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 missing_geometry = (CASE          
          WHEN geometry IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN parcel_id_gap_2022 INT64;
ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN parcel_id_gap_2021 INT64;
ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN parcel_id_gap_2020 INT64;
ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN parcel_id_gap_2019 INT64;

FOR county_name_to_check IN (SELECT DISTINCT county FROM central_dev.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
  parcel_id_gap_2022 = (CASE          
            WHEN parcel_id IN (SELECT parcel_id 
            FROM central_dev.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2022
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2022;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM central_dev.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
  parcel_id_gap_2021 = (CASE          
            WHEN parcel_id IN (SELECT parcel_id 
            FROM central_dev.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2021
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2021;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM central_dev.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
  parcel_id_gap_2020 = (CASE          
            WHEN parcel_id IN (SELECT parcel_id 
            FROM central_dev.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2020
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2020;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM central_dev.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
  parcel_id_gap_2019 = (CASE          
            WHEN parcel_id IN (SELECT parcel_id 
            FROM central_dev.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2019
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2019;
END FOR;

ALTER TABLE `ut-sao-tax-prod.central_dev.flagged_parcels` ADD COLUMN clean_parcel INT64;
UPDATE `ut-sao-tax-prod.central_dev.flagged_parcels` SET 
 clean_parcel = (CASE          
          WHEN neg_mrkt_val = 0 AND 
          neg_lnd_val = 0 AND 
          neg_bldngs_val = 0 AND 
          missing_market_val = 0 AND 
          future_build_date = 0 AND 
          pre_utah_build_date = 0 AND 
          no_prop_type = 0 AND 
          zero_sqr_ft = 0 AND 
          zero_acres = 0 AND 
          missing_parcel_id = 0 AND 
          no_situs_city  = 0 AND 
          no_situs_address = 0 AND 
          no_dist_id = 0 AND 
          missing_exempt_status = 0 AND
          missing_exempt_type = 0 AND
          big_2019_market_change = 0 AND 
          big_2020_market_change = 0 AND 
          big_2021_market_change = 0 AND 
          big_2022_market_change  = 0 AND
          big_2023_market_change = 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

