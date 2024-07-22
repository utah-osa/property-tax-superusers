-- ============================================================================
--        File : 01_tax_roll_stats.sql
--      Author : Alex Nielson (alexnielson@utah.gov)
-- Description : Creates a table with various statistics about how market 
--               values, taxes charged, land value, and building value have 
--               changed over the years. This script also joins cleaned tax 
--               roll data to UGRC parcel geometry. 
--        Note : This script should be ran after all the tax rolls have been 
--               cleaned and are in the tax_roll table.
-- ============================================================================

CREATE OR REPLACE TABLE research_public.tax_roll_stats AS 

SELECT  county,
        parcel_id,
        serial_id,
        year, 
        CASE 
          WHEN property_type IS NULL THEN "NULL"
          WHEN TRIM(property_type)="" THEN "NULL"
          ELSE property_type

        END AS property_type,
        CASE 
          WHEN property_type_internal IS NULL 
                OR TRIM(property_type_internal)="" THEN "NULL"
          ELSE property_type_internal
        END AS property_type_internal,
        
        CASE 
          WHEN tax_exempt IS NULL THEN FALSE
          WHEN tax_exempt = "0" THEN FALSE
          WHEN tax_exempt = "1" THEN TRUE
          WHEN tax_exempt = "true" THEN TRUE
          WHEN tax_exempt = "YES" THEN TRUE
          WHEN tax_exempt = "Y" THEN TRUE
          WHEN tax_exempt = "TRUE" THEN TRUE
          ELSE FALSE
        END AS tax_exempt,

        CASE 
          WHEN tax_exempt_type IS NULL THEN "NULL"
          ELSE tax_exempt_type
        END AS tax_exempt_type,
        tax_exempt_type_internal,
        subdivision,
        detailed_review_date,
        update_date, 
        accessed_date,
        owner_name,
        owner_address1,
        owner_address2,
        district,
        neighborhood_id,
        neighborhood,
        year_built,
        year_built_effective,
        sq_feet,
        acres,
        situs_address1,
        situs_address2,
        situs_city,
        situs_zip,
        
        -- Assessed MV stats
        market,
        market_lag4,
        market_lag3,
        market_lag2,
        market_lag1,
        market_lag,

        market_change,
        market_change_2023,
        market_change_2022,
        market_change_2021,
        market_change_2020,
        market_change_2019,

        market_change_perc,
        market_change_perc_2023,
        market_change_perc_2022,
        market_change_perc_2021,
        market_change_perc_2020,
        market_change_perc_2019,


        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market/sq_feet ELSE NULL END AS mv_by_sq_feet_2023, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market_lag1/sq_feet ELSE NULL END AS mv_by_sq_feet_2022, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market_lag2/sq_feet ELSE NULL END AS mv_by_sq_feet_2021, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market_lag3/sq_feet ELSE NULL END AS mv_by_sq_feet_2020, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market_lag4/sq_feet ELSE NULL END AS mv_by_sq_feet_2019,
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN market_lag5/sq_feet ELSE NULL END AS mv_by_sq_feet_2018,
        
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market/acres ELSE NULL END AS mv_by_acres_2023, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market_lag1/acres ELSE NULL END AS mv_by_acres_2022, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market_lag2/acres ELSE NULL END AS mv_by_acres_2021, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market_lag3/acres ELSE NULL END AS mv_by_acres_2020, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market_lag4/acres ELSE NULL END AS mv_by_acres_2019,
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN market_lag5/acres ELSE NULL END AS mv_by_acres_2018,


        -- Taxes Charged stats
        taxes,
        taxes_lag5,
        taxes_lag4,
        taxes_lag3,
        taxes_lag2,
        taxes_lag1,
        taxes_lag,

        taxes_change,
        taxes_change_2023,
        taxes_change_2022,
        taxes_change_2021,
        taxes_change_2020,
        taxes_change_2019,

        taxes_change_perc,
        taxes_change_perc_2023,
        taxes_change_perc_2022,
        taxes_change_perc_2021,
        taxes_change_perc_2020,
        taxes_change_perc_2019,
        
        
        --Buildings
        improvements,
        improvements_lag5,
        improvements_lag4,
        improvements_lag3,
        improvements_lag2,
        improvements_lag1,
        improvements_lag,

        improvements_change,
        improvements_change_2023,
        improvements_change_2022,
        improvements_change_2021,
        improvements_change_2020,
        improvements_change_2019,

        improvements_change_perc,
        improvements_change_perc_2023,
        improvements_change_perc_2022,
        improvements_change_perc_2021,
        improvements_change_perc_2020,
        improvements_change_perc_2019,
        
        
        -- Land assessed value  stats
        land,
        land_lag5,
        land_lag4,
        land_lag3,
        land_lag2,
        land_lag1,
        land_lag,

        land_change,
        land_change_2023,
        land_change_2022,
        land_change_2021,
        land_change_2020,
        land_change_2019,

        land_change_perc,
        land_change_perc_2023,
        land_change_perc_2022,
        land_change_perc_2021,
        land_change_perc_2020,
        land_change_perc_2019,
        
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land/sq_feet ELSE NULL END AS land_by_sq_feet_2023, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land_lag1/sq_feet ELSE NULL END AS land_by_sq_feet_2022, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land_lag2/sq_feet ELSE NULL END AS land_by_sq_feet_2021, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land_lag3/sq_feet ELSE NULL END AS land_by_sq_feet_2020, 
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land_lag4/sq_feet ELSE NULL END AS land_by_sq_feet_2019,
        CASE WHEN sq_feet IS NOT NULL AND sq_feet NOT IN (0,-1) THEN land_lag5/sq_feet ELSE NULL END AS land_by_sq_feet_2018,
        
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land/acres ELSE NULL END AS land_by_acres_2023, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land_lag1/acres ELSE NULL END AS land_by_acres_2022, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land_lag2/acres ELSE NULL END AS land_by_acres_2021, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land_lag3/acres ELSE NULL END AS land_by_acres_2020, 
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land_lag4/acres ELSE NULL END AS land_by_acres_2019,
        CASE WHEN acres IS NOT NULL AND acres NOT IN (0,-1) THEN land_lag5/acres ELSE NULL END AS land_by_acres_2018,

        geometry

FROM

(
  SELECT  tax_roll.county,
              tax_roll.parcel_id,
              serial_id,
              year, 
              property_type,
              property_type_internal,
              tax_exempt,
              tax_exempt_type,
              tax_exempt_type_internal,
              subdivision,
              detailed_review_date,
              update_date, 
              accessed_date,
              owner_name,
              owner_address1,
              owner_address2,
              district,
              neighborhood_id,
              neighborhood,
              year_built,
              year_built_effective,
              sq_feet,
              acres,
              situs_address1,
              situs_address2,
              situs_city,
              situs_zip,
          
          market,
          market_lag5,
          market_lag4,
          market_lag3,
          market_lag2,
          market_lag1,
          market_lag,
          market - market_lag AS market_change,
          
          CASE 
            WHEN market IS NULL or market = 0 THEN 0 
            ELSE       SAFE_DIVIDE((market - market_lag), market_lag) * 100
          END AS market_change_perc,
          
          market      - market_lag1 AS market_change_2023,
          market_lag1 - market_lag2 AS market_change_2022,
          market_lag2 - market_lag3 AS market_change_2021,
          market_lag3 - market_lag4 AS market_change_2020,
          market_lag4 - market_lag5 AS market_change_2019,
          
          
          CASE WHEN market IS NULL OR market = 0 THEN 0 
            ELSE SAFE_DIVIDE((market - market_lag1), market_lag1) * 100 
          END AS market_change_perc_2023,
          CASE WHEN market_lag1 IS NULL OR market_lag1 = 0 THEN 0 
            ELSE SAFE_DIVIDE((market_lag1 - market_lag2), market_lag2) * 100 
          END AS market_change_perc_2022,
          CASE WHEN market_lag2 IS NULL OR market_lag2 = 0 THEN 0 
            ELSE SAFE_DIVIDE((market_lag2 - market_lag3), market_lag3) * 100 
          END AS market_change_perc_2021,
          CASE WHEN market_lag3 IS NULL OR market_lag3 = 0 THEN 0 
            ELSE SAFE_DIVIDE((market_lag3 - market_lag4), market_lag4) * 100 
          END AS market_change_perc_2020,   
          CASE WHEN market_lag4 IS NULL OR market_lag4 = 0 THEN 0 
            ELSE SAFE_DIVIDE((market_lag4 - market_lag5), market_lag5) * 100 
          END AS market_change_perc_2019,        
          
          taxes,
          taxes_lag5,
          taxes_lag4,
          taxes_lag3,
          taxes_lag2,
          taxes_lag1,
          taxes_lag,
          taxes - taxes_lag AS taxes_change,
          
          CASE 
            WHEN taxes IS NULL or taxes = 0 THEN 0 
            ELSE       SAFE_DIVIDE((taxes - taxes_lag), taxes_lag) * 100
          END AS taxes_change_perc,
          
          taxes      - taxes_lag1 AS taxes_change_2023,
          taxes_lag1 - taxes_lag2 AS taxes_change_2022,
          taxes_lag2 - taxes_lag3 AS taxes_change_2021,
          taxes_lag3 - taxes_lag4 AS taxes_change_2020,
          taxes_lag4 - taxes_lag5 AS taxes_change_2019,
          
          CASE WHEN taxes IS NULL OR taxes = 0 THEN 0 
            ELSE SAFE_DIVIDE((taxes - taxes_lag1), taxes_lag1) * 100 
          END AS taxes_change_perc_2023,
          CASE WHEN taxes_lag1 IS NULL OR taxes_lag1 = 0 THEN 0 
            ELSE SAFE_DIVIDE((taxes_lag1 - taxes_lag2), taxes_lag2) * 100 
          END AS taxes_change_perc_2022,
          CASE WHEN taxes_lag2 IS NULL OR taxes_lag2 = 0 THEN 0 
            ELSE SAFE_DIVIDE((taxes_lag2 - taxes_lag3), taxes_lag3) * 100 
          END AS taxes_change_perc_2021,
          CASE WHEN taxes_lag3 IS NULL OR taxes_lag3 = 0 THEN 0 
            ELSE SAFE_DIVIDE((taxes_lag3 - taxes_lag4), taxes_lag4) * 100 
          END AS taxes_change_perc_2020,
          CASE WHEN taxes_lag4 IS NULL OR taxes_lag4 = 0 THEN 0 
            ELSE SAFE_DIVIDE((taxes_lag4 - taxes_lag5), taxes_lag5) * 100 
          END AS taxes_change_perc_2019,
          
          land,
          land_lag5,
          land_lag4,
          land_lag3,
          land_lag2,
          land_lag1,
          land_lag,
          land - land_lag AS land_change,
          
          CASE 
            WHEN land IS NULL or land = 0 THEN 0 
            ELSE       SAFE_DIVIDE((land - land_lag), land_lag) * 100
          END AS land_change_perc,
                    
          land      - land_lag1 AS land_change_2023,
          land_lag1 - land_lag2 AS land_change_2022,
          land_lag2 - land_lag3 AS land_change_2021,
          land_lag3 - land_lag4 AS land_change_2020,
          land_lag4 - land_lag5 AS land_change_2019,
          
          CASE WHEN land IS NULL OR land = 0 THEN 0 
            ELSE SAFE_DIVIDE((land - land_lag1), land_lag1) * 100 
          END AS land_change_perc_2023,
          CASE WHEN land_lag1 IS NULL OR land_lag1 = 0 THEN 0 
            ELSE SAFE_DIVIDE((land_lag1 - land_lag2), land_lag2) * 100 
          END AS land_change_perc_2022,
          CASE WHEN land_lag2 IS NULL OR land_lag2 = 0 THEN 0 
            ELSE SAFE_DIVIDE((land_lag2 - land_lag3), land_lag3) * 100 
          END AS land_change_perc_2021,
          CASE WHEN land_lag3 IS NULL OR land_lag3 = 0 THEN 0 
            ELSE SAFE_DIVIDE((land_lag3 - land_lag4), land_lag4) * 100 
          END AS land_change_perc_2020,
          CASE WHEN land_lag3 IS NULL OR land_lag3 = 0 THEN 0 
            ELSE SAFE_DIVIDE((land_lag4 - land_lag5), land_lag5) * 100 
          END AS land_change_perc_2019,

          -- buildings
          
          improvements,
          improvements_lag5,
          improvements_lag4,
          improvements_lag3,
          improvements_lag2,
          improvements_lag1,
          improvements_lag,
          improvements - improvements_lag AS improvements_change,
          
          CASE 
            WHEN improvements IS NULL or improvements = 0 THEN 0 
            ELSE       SAFE_DIVIDE((improvements - improvements_lag), improvements_lag) * 100
          END AS improvements_change_perc,
          
          improvements      - improvements_lag1 AS improvements_change_2023,
          improvements_lag1 - improvements_lag2 AS improvements_change_2022,
          improvements_lag2 - improvements_lag3 AS improvements_change_2021,
          improvements_lag3 - improvements_lag4 AS improvements_change_2020,
          improvements_lag4 - improvements_lag5 AS improvements_change_2019,
          
          CASE WHEN improvements IS NULL OR improvements = 0 THEN 0 
            ELSE SAFE_DIVIDE((improvements - improvements_lag1), improvements_lag1) * 100 
          END AS improvements_change_perc_2023,
          CASE WHEN improvements_lag1 IS NULL OR improvements_lag1 = 0 THEN 0 
            ELSE SAFE_DIVIDE((improvements_lag1 - improvements_lag2), improvements_lag2) * 100 
          END AS improvements_change_perc_2022,
          CASE WHEN improvements_lag2 IS NULL OR improvements_lag2 = 0 THEN 0 
            ELSE SAFE_DIVIDE((improvements_lag2 - improvements_lag3), improvements_lag3) * 100 
          END AS improvements_change_perc_2021,
          CASE WHEN improvements_lag3 IS NULL OR improvements_lag3 = 0 THEN 0 
            ELSE SAFE_DIVIDE((improvements_lag3 - improvements_lag4), improvements_lag4) * 100 
          END AS improvements_change_perc_2020,
          CASE WHEN improvements_lag4 IS NULL OR improvements_lag4 = 0 THEN 0 
            ELSE SAFE_DIVIDE((improvements_lag4 - improvements_lag5), improvements_lag5) * 100 
          END AS improvements_change_perc_2019,
          
          
          
          
          
          SAFE.ST_GEOGFROMGEOJSON(utah_parcels.geometry,  make_valid => true) AS geometry
    
  FROM 
  (
    SELECT  county,
              parcel_id,
              serial_id,
              year, 
              property_type,
              property_type_internal,
              tax_exempt,
              tax_exempt_type,
              tax_exempt_type_internal,
              subdivision,
              detailed_review_date,
              update_date, 
              accessed_date,
              owner_name,
              owner_address1,
              owner_address2,
              district,
              neighborhood_id,
              neighborhood,
              year_built,
              year_built_effective,
              sq_feet,
              acres,
              situs_address1,
              situs_address2,
              situs_city,
              situs_zip,
          
            market,
            market_lag5,
            market_lag4,
            market_lag3,
            market_lag2,
            market_lag1,
            
            CASE 
              WHEN market_lag5 IS NOT NULL THEN market_lag5
              WHEN market_lag4 IS NOT NULL THEN market_lag4
              WHEN market_lag3 IS NOT NULL THEN market_lag3
              WHEN market_lag2 IS NOT NULL THEN market_lag2
              WHEN market_lag1 IS NOT NULL THEN market_lag1
              ELSE NULL
            END AS market_lag,
            
            taxes, 
            taxes_lag5,
            taxes_lag4,
            taxes_lag3,
            taxes_lag2,
            taxes_lag1,
            
            CASE 
              WHEN taxes_lag5 IS NOT NULL THEN taxes_lag5
              WHEN taxes_lag4 IS NOT NULL THEN taxes_lag4
              WHEN taxes_lag3 IS NOT NULL THEN taxes_lag3
              WHEN taxes_lag2 IS NOT NULL THEN taxes_lag2
              WHEN taxes_lag1 IS NOT NULL THEN taxes_lag1
              ELSE NULL
            END AS taxes_lag,
            
            land,
            land_lag5,
            land_lag4,
            land_lag3,
            land_lag2,
            land_lag1,
            CASE 
              WHEN land_lag5 IS NOT NULL THEN land_lag5
              WHEN land_lag4 IS NOT NULL THEN land_lag4
              WHEN land_lag3 IS NOT NULL THEN land_lag3
              WHEN land_lag2 IS NOT NULL THEN land_lag2
              WHEN land_lag1 IS NOT NULL THEN land_lag1
              ELSE NULL
            END AS land_lag,
            
            -- buildings
            improvements, 
            improvements_lag5,
            improvements_lag4,
            improvements_lag3,
            improvements_lag2,
            improvements_lag1,
            CASE 
              WHEN improvements_lag5 IS NOT NULL THEN improvements_lag5
              WHEN improvements_lag4 IS NOT NULL THEN improvements_lag4
              WHEN improvements_lag3 IS NOT NULL THEN improvements_lag3
              WHEN improvements_lag2 IS NOT NULL THEN improvements_lag2
              WHEN improvements_lag1 IS NOT NULL THEN improvements_lag1
              ELSE NULL
            END AS improvements_lag,
      
    FROM 
    (
      SELECT  county,
              parcel_id,
              serial_id,
              year, 
              property_type,
              property_type_internal,
              tax_exempt,
              tax_exempt_type,
              tax_exempt_type_internal,
              subdivision,
              detailed_review_date,
              update_date, 
              accessed_date,
              owner_name,
              owner_address1,
              owner_address2,
              district,
              neighborhood_id,
              neighborhood,
              year_built,
              year_built_effective,
              sq_feet,
              acres,
              situs_address1,
              situs_address2,
              situs_city,
              situs_zip,
                
                
                
            
              
              -- Market
              market,
              
              LAG(market , 5) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS market_lag5,
              LAG(market , 4) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS market_lag4,
              LAG(market , 3) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS market_lag3,
              LAG(market , 2) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS market_lag2,
              LAG(market , 1) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS market_lag1,
              
              -- taxes
              taxes ,
              LAG(taxes , 5) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS taxes_lag5,
              LAG(taxes , 4) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS taxes_lag4,
              LAG(taxes , 3) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS taxes_lag3,
              LAG(taxes , 2) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS taxes_lag2,
              LAG(taxes , 1) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS taxes_lag1,
              
              -- Land
              land ,
              LAG(land , 5) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS land_lag5,
              LAG(land , 4) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS land_lag4,
              LAG(land , 3) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS land_lag3,
              LAG(land , 2) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS land_lag2,
              LAG(land , 1) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS land_lag1,
              
              
              -- Buildings
              improvements,
              LAG(improvements , 5) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS improvements_lag5,
              LAG(improvements , 4) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS improvements_lag4,
              LAG(improvements , 3) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS improvements_lag3,
              LAG(improvements , 2) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS improvements_lag2,
              LAG(improvements , 1) OVER (PARTITION BY parcel_id, county ORDER BY year ) AS improvements_lag1,

      FROM 
        (
        SELECT  county,
                CASE 
                  WHEN county = "Utah County" THEN regexp_replace(parcel_id, r':', "")
                  WHEN county = "Weber County" AND LENGTH(parcel_id)=8 THEN CONCAT("0",parcel_id)
                  WHEN county = "Salt Lake County" AND LENGTH(parcel_id)=13 THEN CONCAT("0",parcel_id)
                  WHEN county = "Morgan County" THEN serial_id
                  WHEN county = "Piute County" THEN serial_id
                  ELSE parcel_id
                END AS parcel_id,
                serial_id,
                year, 
                property_type,
                property_type_internal,
                tax_exempt,
                tax_exempt_type,
                tax_exempt_type_internal,
                subdivision,
                detailed_review_date,
                update_date, 
                accessed_date,
                owner_name,
                owner_address1,
                owner_address2,
                district,
                neighborhood_id,
                neighborhood,
                year_built,
                year_built_effective,
                sq_feet,
                acres,
                situs_address1,
                situs_address2,
                situs_city,
                situs_zip,
                market,
                taxes_charged AS taxes,
                land,
                improvements
                
                
        FROM `ut-sao-tax-prod.research_public.tax_roll`
        WHERE county != "Kane County"

        ) AS tax_roll

    )
    
  ) AS tax_roll

  -- Join with the UGRC parcel geometry table
  LEFT JOIN 
  (
  SELECT * except(parcel_id), 
          CASE 
            WHEN county = "Box Elder County" THEN REGEXP_REPLACE(parcel_id, r"-","")
            WHEN county = "Rich County" THEN CONCAT(
                                            LEFT(parcel_id, 6),
                                            "0",
                                            RIGHT(left(parcel_id,8),2),
                                            "-0",
                                            right(parcel_id, 3)
                                          )
            WHEN county = "Tooele County" THEN REGEXP_REPLACE(parcel_id, r"-","")
            WHEN county = "Beaver County" THEN CONCAT(SUBSTR(parcel_id, 0, 2),"-", SUBSTR(parcel_id, 3, 4),"-",  SUBSTR(parcel_id, 7, 4))
            
            ELSE parcel_id
          END AS parcel_id,
  FROM  `ut-sao-tax-prod.research_public.utah_parcels_combo`

  UNION ALL 

  SELECT * except(parcel_id), CAST(parcel_id AS STRING) AS parcel_id
  FROM  `ut-sao-tax-prod.research_public.utah_parcels_combo`
  WHERE county = "Tooele County"

  ) AS utah_parcels
    ON tax_roll.parcel_id = utah_parcels.PARCEL_ID 
      AND tax_roll.county = utah_parcels.county
)

 