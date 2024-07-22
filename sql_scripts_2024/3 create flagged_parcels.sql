CREATE OR REPLACE TABLE `ut-sao-tax-prod.research_public.flagged_parcels` AS 

SELECT
  main.county,
  main.clean_id,
  main.year,
  TRIM(main.property_type) AS property_type,
  TRIM(main.property_type_internal) AS property_type_internal,
  main.tax_exempt,
  main.tax_exempt_type,
  TRIM(main.tax_exempt_type_internal) AS tax_exempt_type_internal,
  TRIM(main.subdivision) AS subdivision,
  main.detailed_review_date,
  main.update_date, 
  main.accessed_date,
  main.owner_name,
  main.owner_address1,
  main.owner_address2,
  TRIM(main.district) AS district,
  TRIM(main.neighborhood_id) AS neighborhood_id,
  TRIM(main.neighborhood) AS neighborhood,
  main.year_built,
  main.year_built_effective,
  main.sq_feet,
  main.acres,
  TRIM(main.situs_address1) AS situs_address1,
  TRIM(main.situs_address2) AS situs_address2, 
  TRIM(main.situs_city) AS situs_city,
  TRIM(main.situs_zip) AS situs_zip,

  land_stats.land,
  improvements_stats.improvements,

  -- Assessed MV stats
  market,
  market_lag1,
  market_lag2,
  market_lag3,
  market_lag4,
  market_lag5,
  market_lag6,
  market_lag_max,
  market_max_lag_depth,

  main.mc_max,
  main.mc1,
  main.mc2,
  main.mc3,
  main.mc4,
  main.mc5,
  main.mc6,

  main.mcp_max,
  main.mcp1,
  main.mcp2,
  main.mcp3,
  main.mcp4,
  main.mcp5,
  main.mcp6,

  -- Square footage change information
  sq_feet_stats.changed_from_last_year AS sq_feet_changed_from_last_year,
  sq_feet_stats.mc1 AS sq_feet_change,
  sq_feet_stats.mcp1 AS sq_feet_change_perc,
  sq_feet_stats.sq_feet_lag1 AS sq_feet_lag1,

  -- Acres change information
  acres_stats.changed_from_last_year AS acres_changed_from_last_year,
  acres_stats.mc1 AS acres_change,
  acres_stats.mcp1 AS acres_change_perc,
  acres_stats.acres_lag1 AS acres_lag1,

  utah_parcels.geometry

FROM research_public.parcel_market_stats AS main

LEFT JOIN research_public.utah_parcels_prod AS utah_parcels
    ON main.clean_id = utah_parcels.clean_id 
      AND main.county = utah_parcels.county

LEFT JOIN research_public.parcel_sq_feet_stats AS sq_feet_stats
  ON main.clean_id = sq_feet_stats.clean_id 
      AND main.county = sq_feet_stats.county
      AND main.year = sq_feet_stats.year

LEFT JOIN research_public.parcel_land_stats AS land_stats
  ON main.clean_id = land_stats.clean_id 
      AND main.county = land_stats.county
      AND main.year = land_stats.year

LEFT JOIN research_public.parcel_improvements_stats AS improvements_stats
  ON main.clean_id = improvements_stats.clean_id 
      AND main.county = improvements_stats.county
      AND main.year = improvements_stats.year

LEFT JOIN research_public.parcel_acres_stats AS acres_stats
  ON main.clean_id = acres_stats.clean_id 
      AND main.county = acres_stats.county
      AND main.year = acres_stats.year

-- WHERE main.year = 2024
;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN neg_mrkt_val INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 neg_mrkt_val = (CASE 
          WHEN market < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN neg_lnd_val INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 neg_lnd_val = (CASE 
          WHEN land < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN neg_bldngs_val INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 neg_bldngs_val = (CASE 
          WHEN improvements < 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN missing_market_val INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 missing_market_val = (CASE 
          WHEN market IS NULL THEN 1
          WHEN market = 0 AND property_type NOT IN ("Centrally Assessed", "Construction") THEN 1
          ELSE 0
        END) 
WHERE TRUE;



ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN future_build_date INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 future_build_date = (CASE 
          WHEN year_built > 2025 THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN pre_utah_build_date INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 pre_utah_build_date = (CASE 
          WHEN year_built < 1847 AND property_type NOT IN ("Agricultural", "Greenbelt", "Vacant", "Centrally Assessed", "Construction") THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN no_prop_type INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 no_prop_type = (CASE          
          WHEN property_type IS NULL OR 
  TRIM(property_type) = "" OR TRIM(property_type) = "NULL" OR TRIM(property_type) = "Unknown" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


-- Find parcels with improvements value greater than 0, square footage of zerom, and not a farm utility or agricultural building.
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN zero_sqr_ft INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 zero_sqr_ft = (CASE          
          WHEN sq_feet <= 0 AND improvements > 0  AND property_type NOT IN ("Agricultural", "Greenbelt", "Centrally Assessed", "Construction") THEN 1
          ELSE 0
        END) 
WHERE TRUE;

-- Make sure all parcels have positive acreage
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN zero_acres INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 zero_acres = (CASE          
          WHEN acres <= 0 THEN 1
          ELSE 0
        END) 
WHERE TRUE;


-- Flag empty parcel ids
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN missing_parcel_id INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 missing_parcel_id = (CASE          
          WHEN clean_id IS NULL OR 
  TRIM(clean_id) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN no_situs_city INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 no_situs_city = (CASE          
          WHEN (situs_city IS NULL OR 
  TRIM(situs_city) = "") AND property_type NOT IN ("Vacant", "Agricultural", "Centrally Assessed", "Construction", "Greenbelt", "Undeveloped") THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN no_situs_address INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 no_situs_address = (
  CASE          
  WHEN (situs_address1 IS NULL OR 
  TRIM(situs_address1) = "") AND property_type NOT IN ("Vacant", "Agricultural", "Centrally Assessed", "Construction", "Greenbelt", "Undeveloped") THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN no_dist_id INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 no_dist_id = (CASE          
          WHEN district IS NULL OR TRIM(district) = "" THEN 1
          ELSE 0
        END) 
WHERE TRUE;



ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2019_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2019_market_change = (CASE          
          WHEN mcp6 > 300 AND year_built NOT IN (2018, 2019)  THEN 1
          ELSE 0
        END) 
WHERE TRUE;



ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2020_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2020_market_change = (CASE          
          WHEN mcp5 > 300 
          AND year_built NOT IN (2019, 2020) THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2021_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2021_market_change = (CASE          
          WHEN mcp4 > 300 
            AND year_built NOT IN (2020, 2021) THEN 1
          ELSE 0
        END) 
WHERE TRUE;


ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2022_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2022_market_change = (CASE          
          WHEN mcp3 > 300 
            AND year_built NOT IN (2021, 2022) THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2023_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2023_market_change = (CASE          
          WHEN mcp2 > 300 
            AND year_built NOT IN (2022, 2023)  THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN big_2024_market_change INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 big_2024_market_change = (CASE          
          WHEN mcp1 > 300 
            AND year_built NOT IN (2023, 2024) 
            AND property_type NOT IN ("Construction") THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN missing_exempt_status INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 missing_exempt_status = (CASE          
          WHEN tax_exempt IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN missing_exempt_type INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 missing_exempt_type = (CASE          
          WHEN tax_exempt_type IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN missing_geometry INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
 missing_geometry = (CASE          
          WHEN geometry IS NULL THEN 1
          ELSE 0
        END) 
WHERE TRUE;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN parcel_id_gap_2023 INT64;
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN parcel_id_gap_2022 INT64;
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN parcel_id_gap_2021 INT64;
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN parcel_id_gap_2020 INT64;
ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN parcel_id_gap_2019 INT64;


FOR county_name_to_check IN (SELECT DISTINCT county FROM research_public.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
  parcel_id_gap_2023 = (CASE          
            WHEN clean_id IN (SELECT clean_id 
            FROM research_public.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2023
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2023;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM research_public.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
  parcel_id_gap_2022 = (CASE          
            WHEN clean_id IN (SELECT clean_id 
            FROM research_public.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2022
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2022;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM research_public.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
  parcel_id_gap_2021 = (CASE          
            WHEN clean_id IN (SELECT clean_id 
            FROM research_public.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2021
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2021;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM research_public.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
  parcel_id_gap_2020 = (CASE          
            WHEN clean_id IN (SELECT clean_id 
            FROM research_public.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2020
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2020;
END FOR;

FOR county_name_to_check IN (SELECT DISTINCT county FROM research_public.tax_roll ORDER BY county)
DO
  UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
  parcel_id_gap_2019 = (CASE          
            WHEN clean_id IN (SELECT clean_id 
            FROM research_public.parcels_with_gaps 
            WHERE county = county_name_to_check.county 
              AND year_impacted = 2019
            ) THEN 1
            ELSE 0
          END) 
  WHERE county = county_name_to_check.county AND year = 2019;
END FOR;


UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` 
SET parcel_id_gap_2023 = 0 
WHERE parcel_id_gap_2023 IS NULL;

ALTER TABLE `ut-sao-tax-prod.research_public.flagged_parcels` ADD COLUMN clean_parcel INT64;
UPDATE `ut-sao-tax-prod.research_public.flagged_parcels` SET 
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
                big_2023_market_change = 0 AND
                big_2024_market_change = 0 AND
                -- parcel_id_gap_2019 = 0 AND 
                -- parcel_id_gap_2020 = 0 AND 
                -- parcel_id_gap_2021 = 0 AND 
                -- parcel_id_gap_2022 = 0 AND 
                (parcel_id_gap_2023 = 0)
                THEN 1
          ELSE 0
        END) 
WHERE TRUE;









SELECT county, 
SUM(CASE WHEN TRUE THEN 1 ELSE 0 END) AS unique_properties,
SUM(clean_parcel) AS passed_count, 
SUM(CASE WHEN TRUE THEN 1 ELSE 0 END) - SUM(clean_parcel) AS failed_count,
SUM(neg_mrkt_val) AS neg_mrkt_val,  
        SUM(neg_lnd_val) AS neg_lnd_val,
        SUM(neg_bldngs_val) AS neg_bldngs_val,
        SUM(missing_market_val) AS missing_market_val,
        SUM(future_build_date) AS future_build_date,
        SUM(pre_utah_build_date) AS pre_utah_build_date,
        SUM(no_prop_type) AS no_prop_type,
        SUM(zero_sqr_ft) AS zero_sqr_ft,
        SUM(zero_acres) AS zero_acres,
        SUM(missing_parcel_id) AS missing_parcel_id,
        SUM(no_situs_city)  AS no_situs_city,
        SUM(no_situs_address) AS no_situs_address,
        SUM(no_dist_id) AS no_dist_id,
        SUM(missing_exempt_status ) AS missing_exempt_status,
        SUM(missing_exempt_type ) AS missing_exempt_type,
        SUM(big_2019_market_change) AS big_2019_market_change,
        SUM(big_2020_market_change) AS big_2020_market_change,
        SUM(big_2021_market_change) AS big_2021_market_change,
        SUM(big_2022_market_change  ) AS big_2022_market_change,
        SUM(big_2023_market_change ) AS big_2023_market_change,
        SUM(big_2024_market_change ) AS big_2024_market_change,
        -- SUM(parcel_id_gap_2019) AS parcel_id_gap_2019 ,
        -- SUM(parcel_id_gap_2020) AS parcel_id_gap_2020,
        -- SUM(parcel_id_gap_2021) AS parcel_id_gap_2021,
        -- SUM(parcel_id_gap_2022) AS parcel_id_gap_2022,
        SUM(parcel_id_gap_2023) AS parcel_id_gap_2023
FROM `ut-sao-tax-prod.research_public.flagged_parcels` 
WHERE year =2024
GROUP BY county
ORDER BY county





-- Check the percent of missing parcels. 

SELECT 
county, year, property_type,
cnt,valid_geometry_cnt, safe_divide(valid_geometry_cnt, cnt) AS perc_valid_cnt,
market_total, valid_geometry_market_total, safe_divide(valid_geometry_market_total,market_total) AS perc_market_total_valid

FROM (
  SELECT county,year,  property_type, SUM(CASE WHEN TRUE THEN 1 ELSE 0  END) AS cnt, SUM(CASE WHEN geometry IS NULL THEN 0 ELSE 1 END) AS valid_geometry_cnt,
  SUM(CASE WHEN TRUE THEN market ELSE 0  END) AS market_total, SUM(CASE WHEN geometry IS NULL THEN 0 ELSE market END) AS valid_geometry_market_total
  FROM research_public.tax_roll_stats
  -- WHERE county IN ("Sanpete County", "Sevier County")
  GROUP BY county, year, property_type
)
ORDER BY county, year, property_type;

SELECT 
county, year,
cnt,valid_geometry_cnt, safe_divide(valid_geometry_cnt, cnt) AS perc_valid_cnt,
market_total, valid_geometry_market_total, safe_divide(valid_geometry_market_total,market_total) AS perc_market_total_valid

FROM (
  SELECT county,year, SUM(CASE WHEN TRUE THEN 1 ELSE 0  END) AS cnt, SUM(CASE WHEN geometry IS NULL THEN 0 ELSE 1 END) AS valid_geometry_cnt,
  SUM(CASE WHEN TRUE THEN market ELSE 0  END) AS market_total, SUM(CASE WHEN geometry IS NULL THEN 0 ELSE market END) AS valid_geometry_market_total
  FROM research_public.tax_roll_stats
  -- WHERE county IN ("Sanpete County", "Sevier County")
  GROUP BY county, year
)
ORDER BY county, year;