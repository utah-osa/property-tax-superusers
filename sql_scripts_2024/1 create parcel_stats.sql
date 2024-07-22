-- ============================================================================
--        File : 1 create parcel_stats.sql
--      Author : Alex Nielson (alexnielson@utah.gov)
-- Description : Creates a table with various statistics about how market 
--               values, taxes charged, land value, and building value have 
--               changed over the years. This script also joins cleaned tax 
--               roll data to UGRC parcel geometry. 
--        Note : This script should be ran after all the tax rolls have been 
--               cleaned and are in the tax_roll table.
-- ============================================================================


-- We needed to get the first non null value by parcel to handle the parcels 
-- with multiple buildings. Big Query couldn't handle a generic return data
-- type, so I had to make a 5 nearly identical custom functions. 

-- They each get the first non null value.

CREATE OR REPLACE AGGREGATE FUNCTION `ut-sao-tax-prod.research_public.NNANYVALUE_STRING`(
  user_column STRING)
RETURNS STRING
AS (
  ARRAY_AGG( -- turn the mixed_data values into a list
        user_column -- we'll create an array of values from our mixed_data column
        IGNORE NULLS -- there we go!
        --ascending order
        LIMIT 1 -- only fill the array with 1 thing
    )[SAFE_OFFSET(0)]
);

CREATE OR REPLACE AGGREGATE FUNCTION `ut-sao-tax-prod.research_public.NNANYVALUE_FLOAT`(
  user_column FLOAT64)
RETURNS FLOAT64
AS (
  ARRAY_AGG( -- turn the mixed_data values into a list
        user_column -- we'll create an array of values from our mixed_data column
        IGNORE NULLS -- there we go!
        --ascending order
        LIMIT 1 -- only fill the array with 1 thing
    )[SAFE_OFFSET(0)]
);

CREATE OR REPLACE AGGREGATE FUNCTION `ut-sao-tax-prod.research_public.NNANYVALUE_INT`(
  user_column INT64)
RETURNS INT64
AS (
  ARRAY_AGG( -- turn the mixed_data values into a list
        user_column -- we'll create an array of values from our mixed_data column
        IGNORE NULLS -- there we go!
        --ascending order
        LIMIT 1 -- only fill the array with 1 thing
    )[SAFE_OFFSET(0)]
);

CREATE OR REPLACE AGGREGATE FUNCTION `ut-sao-tax-prod.research_public.NNANYVALUE_BOOL`(
  user_column BOOL)
RETURNS BOOL
AS (
  ARRAY_AGG( -- turn the mixed_data values into a list
        user_column -- we'll create an array of values from our mixed_data column
        IGNORE NULLS -- there we go!
        --ascending order
        LIMIT 1 -- only fill the array with 1 thing
    )[SAFE_OFFSET(0)]
);

CREATE OR REPLACE AGGREGATE FUNCTION `ut-sao-tax-prod.research_public.NNANYVALUE_DATE`(
  user_column DATE)
RETURNS DATE
AS (
  ARRAY_AGG( -- turn the mixed_data values into a list
        user_column -- we'll create an array of values from our mixed_data column
        IGNORE NULLS -- there we go!
        --ascending order
        LIMIT 1 -- only fill the array with 1 thing
    )[SAFE_OFFSET(0)]
);











CREATE OR REPLACE TABLE research_public.parcel_stats AS 
      
      SELECT  county,
              clean_id,
              parcel_ids,
              serial_ids,
              building_ids,
              building_count,
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
              -- owner_name,
              -- owner_address1,
              -- owner_address2,
              district,
              neighborhood_id,
              neighborhood,

              year_builts,
              --year_built_effectives,
              sq_feets,
              acres,
              situs_address1,
              situs_address2,
              situs_city,
              situs_zip,
                
              -- Market
              market_values,
              (SELECT SUM(x) FROM UNNEST(market_values) x) AS market_value_sum,
              (SELECT MAX(x) FROM UNNEST(market_values) x) AS market_value_max,
              (SELECT MIN(x) FROM UNNEST(market_values) x) AS market_value_min,

              -- Taxes
              taxes_values,
              (SELECT SUM(x) FROM UNNEST(taxes_values) x) AS taxes_sum,
              (SELECT MAX(x) FROM UNNEST(taxes_values) x) AS taxes_max,
              (SELECT MIN(x) FROM UNNEST(taxes_values) x) AS taxes_min,

              -- Land
              land_values,
              (SELECT SUM(x) FROM UNNEST(land_values) x) AS land_sum,
              (SELECT MAX(x) FROM UNNEST(land_values) x) AS land_max,
              (SELECT MIN(x) FROM UNNEST(land_values) x) AS land_min,

              -- Improvements
              improvements_values,
              (SELECT SUM(x) FROM UNNEST(improvements_values) x) AS improvements_sum,
              (SELECT MAX(x) FROM UNNEST(improvements_values) x) AS improvements_max,
              (SELECT MIN(x) FROM UNNEST(improvements_values) x) AS improvements_min,

              -- Square Footage
              (SELECT SUM(SAFE_CAST(x AS INT64)) FROM UNNEST(sq_feets) x) AS sq_feet_sum,
              (SELECT MAX(SAFE_CAST(x AS INT64)) FROM UNNEST(sq_feets) x) AS sq_feet_max,
              (SELECT MIN(SAFE_CAST(x AS INT64)) FROM UNNEST(sq_feets) x) AS sq_feet_min,

              -- Year Built
              (SELECT MAX(x) FROM UNNEST(year_builts) x) AS year_builts_max,
              (SELECT MIN(x) FROM UNNEST(year_builts) x) AS year_builts_min --,

              -- Year Built Effective
              --(SELECT MAX(x) FROM UNNEST(year_built_effectives) x) AS year_built_effectives_max,
              --(SELECT MIN(x) FROM UNNEST(year_built_effectives) x) AS year_built_effectives_min,
              
              
              
      FROM 
        (
        SELECT  county,
          CASE 
            WHEN county = "Utah County" THEN regexp_replace(parcel_id, r':', "")
            WHEN county = "Weber County" AND LENGTH(parcel_id)=8 THEN CONCAT("0",TRIM(parcel_id))
            WHEN county = "Salt Lake County" AND LENGTH(parcel_id)=13 THEN CONCAT("0",TRIM(parcel_id))
            WHEN county = "Morgan County" THEN TRIM(serial_id)
            WHEN county = "Piute County" THEN TRIM(serial_id)
            WHEN county = "Daggett County" THEN TRIM(serial_id)
            WHEN county = "Iron County" THEN TRIM(serial_id)
            ELSE TRIM(parcel_id)
          END AS clean_id,
          ARRAY_AGG(TRIM(parcel_id) IGNORE NULLS ORDER BY building_id) AS parcel_ids,
          ARRAY_AGG(TRIM(serial_id) IGNORE NULLS ORDER BY building_id) AS serial_ids,
          ARRAY_AGG(TRIM(building_id) IGNORE NULLS ORDER BY building_id) AS building_ids,
          ANY_VALUE(building_count) AS building_count,
          year, 

          `research_public.NNANYVALUE_STRING`(CASE 
            WHEN property_type IS NULL THEN "Unknown"
            WHEN TRIM(property_type)="" THEN "Unknown"
            ELSE property_type

          END) AS property_type,
          `research_public.NNANYVALUE_STRING`(CASE 
            WHEN property_type_internal IS NULL 
                  OR TRIM(property_type_internal)="" THEN "Unknown"
            ELSE property_type_internal
          END) AS property_type_internal,
          
          `research_public.NNANYVALUE_BOOL`(CASE 
            WHEN tax_exempt IS NULL THEN FALSE
            WHEN tax_exempt = "0" THEN FALSE
            WHEN tax_exempt = "1" THEN TRUE
            WHEN tax_exempt = "true" THEN TRUE
            WHEN tax_exempt = "YES" THEN TRUE
            WHEN tax_exempt = "Y" THEN TRUE
            WHEN tax_exempt = "TRUE" THEN TRUE
            ELSE FALSE
          END) AS tax_exempt,

          `research_public.NNANYVALUE_STRING`(CASE 
            WHEN tax_exempt_type IS NULL THEN "Unknown"
            ELSE tax_exempt_type
          END) AS tax_exempt_type,

          `research_public.NNANYVALUE_STRING`(tax_exempt_type_internal) AS tax_exempt_type_internal,
          `research_public.NNANYVALUE_STRING`(subdivision) AS subdivision,
          `research_public.NNANYVALUE_DATE`(detailed_review_date) AS detailed_review_date,
          `research_public.NNANYVALUE_DATE`(update_date) AS update_date, 
          `research_public.NNANYVALUE_DATE`(accessed_date) AS accessed_date,
          -- `research_public.NNANYVALUE_STRING`(owner_name) AS owner_name,
          -- `research_public.NNANYVALUE_STRING`(owner_address1) AS owner_address1,
          -- `research_public.NNANYVALUE_STRING`(owner_address2) AS owner_address2,
          `research_public.NNANYVALUE_STRING`(district) AS district,
          `research_public.NNANYVALUE_STRING`(neighborhood_id) AS neighborhood_id,
          `research_public.NNANYVALUE_STRING`(neighborhood) AS neighborhood,
          ARRAY_AGG(year_built IGNORE NULLS ORDER BY building_id) as year_builts,
          -- ARRAY_AGG(year_built_effective IGNORE NULLS ORDER BY building_id) as year_built_effectives,
          ARRAY_AGG(sq_feet IGNORE NULLS ORDER BY building_id) as sq_feets,
          `research_public.NNANYVALUE_FLOAT`(acres) AS acres,
          `research_public.NNANYVALUE_STRING`(situs_address1) AS situs_address1,
          `research_public.NNANYVALUE_STRING`(situs_address2) AS situs_address2,
          `research_public.NNANYVALUE_STRING`(situs_city) AS situs_city,
          `research_public.NNANYVALUE_STRING`(situs_zip) AS situs_zip,

          ARRAY_AGG(market IGNORE NULLS ORDER BY building_id) as market_values,
          ARRAY_AGG(taxes_charged IGNORE NULLS ORDER BY building_id) as taxes_values,
          ARRAY_AGG(land IGNORE NULLS ORDER BY building_id) as land_values,
          ARRAY_AGG(improvements IGNORE NULLS ORDER BY building_id) as improvements_values
                
        FROM `ut-sao-tax-prod.research_public.tax_roll`
        GROUP BY 
          county,
          year,
          clean_id
        );

-- If the cnt != repeated, then there is something weird. All the cases I saw are when the parcel id is Null or Blank, so these are going to have issues anyway.


-- -- SELECT *
-- -- Get any parcels that have a market value sum and max that do not equal, but the min and max are the same (it means they are repeated in the array) and we should be ok with them.
-- SELECT  county, year, count(county) as cnt, countif(market_value_max = market_value_min) AS repeated
-- FROM research_public.parcel_stats
-- WHERE market_value_sum != market_value_max
-- GROUP BY county, year
-- ORDER BY county, year


-- SELECT  county, year, count(county) as cnt, countif(taxes_max = taxes_min) AS repeated
-- FROM research_public.parcel_stats
-- WHERE taxes_sum != taxes_max
-- GROUP BY county, year
-- ORDER BY county, year


-- SELECT  county, year, count(county) as cnt, countif(land_max = land_min) AS repeated
-- FROM research_public.parcel_stats
-- WHERE land_sum != land_max
-- GROUP BY county, year
-- ORDER BY county, year

-- SELECT  county, year, count(county) as cnt, countif(improvements_max = improvements_min) AS repeated
-- FROM research_public.parcel_stats
-- WHERE improvements_sum != improvements_max
-- GROUP BY county, year
-- ORDER BY county, year

-- SELECT  county, year, count(county) as cnt, countif(sq_feet_max = sq_feet_min) AS repeated
-- FROM research_public.parcel_stats
-- WHERE sq_feet_sum != sq_feet_max
-- GROUP BY county, year
-- ORDER BY county, year

-- # good news is that based on this everyone except 2 parcels in Juab, they are simply repeats for the market value column.
-- # this will make it easy to aggregate. 

-- For our analysis, to do tie-breakers:
-- get oldest year built,
-- get newest effective year built



-- I want a flattened version where each parcel's multiple buildings has 
-- been aggregated to a single row. Then we can do the lag stuff to 
-- calculate 5 year changes and differences.

CREATE OR REPLACE TABLE research_public.parcel_stats_agg AS 

SELECT 
  county,
  clean_id,
  -- parcel_ids[SAFE_OFFSET(0)] AS parcel_id,
  -- serial_ids[SAFE_OFFSET(0)] AS serial_id,
  building_count,
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
  -- owner_name,
  -- owner_address1,
  -- owner_address2,
  district,
  neighborhood_id,
  neighborhood,
  acres,
  situs_address1,
  situs_address2,
  situs_city,
  situs_zip,
    
  -- Market
  -- market_values,
  CASE 
    WHEN market_value_max = market_value_min THEN market_value_max
    ELSE market_value_sum
  END AS market,

  CASE 
    WHEN taxes_max = taxes_min THEN taxes_max
    ELSE taxes_sum
  END AS taxes,

  CASE 
    WHEN land_max = land_min THEN land_max
    ELSE land_sum
  END AS land,

  CASE 
    WHEN improvements_max = improvements_min THEN improvements_max
    ELSE improvements_sum
  END AS improvements,

  -- sq_feets,
  CASE 
    WHEN sq_feet_min = sq_feet_max THEN sq_feet_max
    ELSE sq_feet_sum
  END AS sq_feet,

  -- year_builts,
  year_builts_min AS year_built --,
  -- year_built_effectives,
  -- CASE 
  --   -- If the year_built_effective value is something like 10 (meaninig ten years, then we want to add 10 years to our most recent year built.)
  --   WHEN year_built_effectives_max < 100 THEN year_built_effectives_max + year_builts_max
  --   ELSE year_built_effectives_max
  -- END AS year_built_effective

FROM research_public.parcel_stats
WHERE clean_id IS NOT NULL AND TRIM(clean_id)!="";









-- DECLARE metric_name STRING DEFAULT "market"; 

CREATE OR REPLACE PROCEDURE `ut-sao-tax-prod.research_public.build_metric_stats`(metric_name STRING)

BEGIN

DECLARE q STRING;
SET q='''
CREATE OR REPLACE TABLE research_public.parcel_'''||metric_name||'''_stats AS 

SELECT

  county,
  clean_id,
  year, 
  building_count,
  property_type,
  property_type_internal,
  tax_exempt,
  tax_exempt_type,
  tax_exempt_type_internal,
  subdivision,
  detailed_review_date,
  update_date, 
  accessed_date,
  -- owner_name,
  -- owner_address1,
  -- owner_address2,
  district,
  neighborhood_id,
  neighborhood,
  year_built,
  -- year_built_effective,
  sq_feet,
  acres,
  situs_address1,
  situs_address2,
  situs_city,
  situs_zip,

  '''||metric_name||''',
  '''||metric_name||'''_lag1,
  '''||metric_name||'''_lag2,
  '''||metric_name||'''_lag3,
  '''||metric_name||'''_lag4,
  '''||metric_name||'''_lag5,
  '''||metric_name||'''_lag6,

  CASE 
    WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
    WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
    WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
    WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
    WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
    WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
    ELSE NULL
  END AS '''||metric_name||'''_lag_max, -- Maximum lag 

  CASE 
    WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN 6
    WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN 5
    WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN 4
    WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN 3
    WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN 2
    WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN 1
    ELSE 0
  END AS '''||metric_name||'''_max_lag_depth,

  
  
  '''||metric_name||'''      - '''||metric_name||'''_lag1 AS mc1, --mc stands for metric change"
  '''||metric_name||'''_lag1 - '''||metric_name||'''_lag2 AS mc2,
  '''||metric_name||'''_lag2 - '''||metric_name||'''_lag3 AS mc3,
  '''||metric_name||'''_lag3 - '''||metric_name||'''_lag4 AS mc4,
  '''||metric_name||'''_lag4 - '''||metric_name||'''_lag5 AS mc5,
  '''||metric_name||'''_lag5 - '''||metric_name||'''_lag6 AS mc6,

  

  '''||metric_name||''' - CASE 
    WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
    WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
    WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
    WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
    WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
    WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
    ELSE NULL
  END AS mc_max, -- market change for maximum lag 


  CASE WHEN '''||metric_name||''' IS NULL OR '''||metric_name||''' = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||''' - '''||metric_name||'''_lag1), '''||metric_name||'''_lag1) * 100 
  END AS mcp1, --mcp stands for "metric change percentage"
  CASE WHEN '''||metric_name||'''_lag1 IS NULL OR '''||metric_name||'''_lag1 = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||'''_lag1 - '''||metric_name||'''_lag2), '''||metric_name||'''_lag2) * 100 
  END AS mcp2,
  CASE WHEN '''||metric_name||'''_lag2 IS NULL OR '''||metric_name||'''_lag2 = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||'''_lag2 - '''||metric_name||'''_lag3), '''||metric_name||'''_lag3) * 100 
  END AS mcp3,
  CASE WHEN '''||metric_name||'''_lag3 IS NULL OR '''||metric_name||'''_lag3 = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||'''_lag3 - '''||metric_name||'''_lag4), '''||metric_name||'''_lag4) * 100 
  END AS mcp4,   
  CASE WHEN '''||metric_name||'''_lag4 IS NULL OR '''||metric_name||'''_lag4 = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||'''_lag4 - '''||metric_name||'''_lag5), '''||metric_name||'''_lag5) * 100 
  END AS mcp5,     
  CASE WHEN '''||metric_name||'''_lag5 IS NULL OR '''||metric_name||'''_lag5 = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||'''_lag5 - '''||metric_name||'''_lag6), '''||metric_name||'''_lag6) * 100 
  END AS mcp6,


  CASE WHEN '''||metric_name||''' IS NULL OR '''||metric_name||''' = 0 THEN 0 
    ELSE SAFE_DIVIDE(('''||metric_name||''' - CASE 
    WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
    WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
    WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
    WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
    WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
    WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
    ELSE NULL
  END ), CASE 
    WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
    WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
    WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
    WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
    WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
    WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
    ELSE NULL
  END ) * 100 
  END AS mcp_max -- Market Change Percentage for Max Lag


    


FROM (
  SELECT 
    county,
    clean_id,
    year, 
    building_count,
    property_type,
    property_type_internal,
    tax_exempt,
    tax_exempt_type,
    tax_exempt_type_internal,
    subdivision,
    detailed_review_date,
    update_date, 
    accessed_date,
    -- owner_name,
    -- owner_address1,
    -- owner_address2,
    district,
    neighborhood_id,
    neighborhood,
    year_built,
    -- year_built_effective,
    sq_feet,
    acres,
    situs_address1,
    situs_address2,
    situs_city,
    situs_zip,

    '''||metric_name||''',
    LAG('''||metric_name||''' , 1) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag1,
    LAG('''||metric_name||''' , 2) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag2,
    LAG('''||metric_name||''' , 3) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag3,
    LAG('''||metric_name||''' , 4) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag4,
    LAG('''||metric_name||''' , 5) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag5,
    LAG('''||metric_name||''' , 6) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag6


  FROM research_public.parcel_stats_agg
)

ORDER BY county,clean_id, year DESC
''';

EXECUTE IMMEDIATE q;

END;


CALL research_public.build_metric_stats("market");
CALL research_public.build_metric_stats("taxes");
CALL research_public.build_metric_stats("improvements");
CALL research_public.build_metric_stats("land");














-- Similar logic to above, but for sq_feet, acres, and year built fields. We want to see 
-- if they changed to see if we can attribute the change in market, improvements, 
-- land etc to a change in one of these fields.

CREATE OR REPLACE PROCEDURE `ut-sao-tax-prod.research_public.build_characteristic_stats`(metric_name STRING)

BEGIN

DECLARE q STRING;
SET q='''
CREATE OR REPLACE TABLE research_public.parcel_'''||metric_name||'''_stats AS 

SELECT county,
    clean_id,
    year, 
    building_count,
    property_type,
    property_type_internal,
    tax_exempt,
    tax_exempt_type,
    tax_exempt_type_internal,
    subdivision,
    detailed_review_date,
    update_date, 
    accessed_date,
    -- owner_name,
    -- owner_address1,
    -- owner_address2,
    district,
    neighborhood_id,
    neighborhood,
    situs_address1,
    situs_address2,
    situs_city,
    situs_zip,

    '''||metric_name||''',
    '''||metric_name||'''_lag1,
    '''||metric_name||'''_lag2,
    '''||metric_name||'''_lag3,
    '''||metric_name||'''_lag4,
    '''||metric_name||'''_lag5,
    '''||metric_name||'''_lag6,

    '''||metric_name||'''_lag_max, -- Maximum lag 

    '''||metric_name||'''_max_lag_depth,

    CASE WHEN '''||metric_name||''' != '''||metric_name||'''_lag1 THEN TRUE
    ELSE FALSE END AS changed_from_last_year,

    
    mc1, --mc stands for metric change"
    mc2,
    mc3,
    mc4,
    mc5,
    mc6,
    mc_max, -- market change for maximum lag 


    mcp1, --mcp stands for "metric change percentage"
    mcp2,
    mcp3,
    mcp4,   
    mcp5,     
    mcp6,


    mcp_max -- Market Change Percentage for Max Lag

FROM (
  SELECT

    county,
    clean_id,
    year, 
    building_count,
    property_type,
    property_type_internal,
    tax_exempt,
    tax_exempt_type,
    tax_exempt_type_internal,
    subdivision,
    detailed_review_date,
    update_date, 
    accessed_date,
    -- owner_name,
    -- owner_address1,
    -- owner_address2,
    district,
    neighborhood_id,
    neighborhood,
    situs_address1,
    situs_address2,
    situs_city,
    situs_zip,

    '''||metric_name||''',
    '''||metric_name||'''_lag1,
    '''||metric_name||'''_lag2,
    '''||metric_name||'''_lag3,
    '''||metric_name||'''_lag4,
    '''||metric_name||'''_lag5,
    '''||metric_name||'''_lag6,

    CASE 
      WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
      WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
      WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
      WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
      WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
      WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
      ELSE NULL
    END AS '''||metric_name||'''_lag_max, -- Maximum lag 

    CASE 
      WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN 6
      WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN 5
      WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN 4
      WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN 3
      WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN 2
      WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN 1
      ELSE 0
    END AS '''||metric_name||'''_max_lag_depth,

    
    
    '''||metric_name||'''      - '''||metric_name||'''_lag1 AS mc1, --mc stands for metric change"
    '''||metric_name||'''_lag1 - '''||metric_name||'''_lag2 AS mc2,
    '''||metric_name||'''_lag2 - '''||metric_name||'''_lag3 AS mc3,
    '''||metric_name||'''_lag3 - '''||metric_name||'''_lag4 AS mc4,
    '''||metric_name||'''_lag4 - '''||metric_name||'''_lag5 AS mc5,
    '''||metric_name||'''_lag5 - '''||metric_name||'''_lag6 AS mc6,

    

    '''||metric_name||''' - CASE 
      WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
      WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
      WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
      WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
      WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
      WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
      ELSE NULL
    END AS mc_max, -- market change for maximum lag 


    CASE WHEN '''||metric_name||''' IS NULL OR '''||metric_name||''' = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||''' - '''||metric_name||'''_lag1), '''||metric_name||'''_lag1) * 100 
    END AS mcp1, --mcp stands for "metric change percentage"
    CASE WHEN '''||metric_name||'''_lag1 IS NULL OR '''||metric_name||'''_lag1 = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||'''_lag1 - '''||metric_name||'''_lag2), '''||metric_name||'''_lag2) * 100 
    END AS mcp2,
    CASE WHEN '''||metric_name||'''_lag2 IS NULL OR '''||metric_name||'''_lag2 = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||'''_lag2 - '''||metric_name||'''_lag3), '''||metric_name||'''_lag3) * 100 
    END AS mcp3,
    CASE WHEN '''||metric_name||'''_lag3 IS NULL OR '''||metric_name||'''_lag3 = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||'''_lag3 - '''||metric_name||'''_lag4), '''||metric_name||'''_lag4) * 100 
    END AS mcp4,   
    CASE WHEN '''||metric_name||'''_lag4 IS NULL OR '''||metric_name||'''_lag4 = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||'''_lag4 - '''||metric_name||'''_lag5), '''||metric_name||'''_lag5) * 100 
    END AS mcp5,     
    CASE WHEN '''||metric_name||'''_lag5 IS NULL OR '''||metric_name||'''_lag5 = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||'''_lag5 - '''||metric_name||'''_lag6), '''||metric_name||'''_lag6) * 100 
    END AS mcp6,


    CASE WHEN '''||metric_name||''' IS NULL OR '''||metric_name||''' = 0 THEN 0 
      ELSE SAFE_DIVIDE(('''||metric_name||''' - CASE 
      WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
      WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
      WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
      WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
      WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
      WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
      ELSE NULL
    END ), CASE 
      WHEN '''||metric_name||'''_lag6 IS NOT NULL THEN '''||metric_name||'''_lag6
      WHEN '''||metric_name||'''_lag5 IS NOT NULL THEN '''||metric_name||'''_lag5
      WHEN '''||metric_name||'''_lag4 IS NOT NULL THEN '''||metric_name||'''_lag4
      WHEN '''||metric_name||'''_lag3 IS NOT NULL THEN '''||metric_name||'''_lag3
      WHEN '''||metric_name||'''_lag2 IS NOT NULL THEN '''||metric_name||'''_lag2
      WHEN '''||metric_name||'''_lag1 IS NOT NULL THEN '''||metric_name||'''_lag1
      ELSE NULL
    END ) * 100 
    END AS mcp_max -- Market Change Percentage for Max Lag


      


  FROM (
    SELECT 
      county,
      clean_id,
      year, 
      building_count,
      property_type,
      property_type_internal,
      tax_exempt,
      tax_exempt_type,
      tax_exempt_type_internal,
      subdivision,
      detailed_review_date,
      update_date, 
      accessed_date,
      -- owner_name,
      -- owner_address1,
      -- owner_address2,
      district,
      neighborhood_id,
      neighborhood,
      situs_address1,
      situs_address2,
      situs_city,
      situs_zip,

      '''||metric_name||''',
      LAG('''||metric_name||''' , 1) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag1,
      LAG('''||metric_name||''' , 2) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag2,
      LAG('''||metric_name||''' , 3) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag3,
      LAG('''||metric_name||''' , 4) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag4,
      LAG('''||metric_name||''' , 5) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag5,
      LAG('''||metric_name||''' , 6) OVER (PARTITION BY clean_id, county ORDER BY year ) AS '''||metric_name||'''_lag6


    FROM research_public.parcel_stats_agg
  )
)
ORDER BY county,clean_id, year DESC
''';

EXECUTE IMMEDIATE q;

END;

CALL research_public.build_characteristic_stats("sq_feet");
CALL research_public.build_characteristic_stats("acres");
CALL research_public.build_characteristic_stats("year_built");

















CREATE OR REPLACE TABLE research_public.parcel_stats_final AS 
SELECT
  main.county,
  main.clean_id,
  -- main.parcel_id,
  -- serial_id,
  year,
  property_type AS prop_type,
  property_type_internal AS prop_typei,
  tax_exempt,
  tax_exempt_type AS ex_type,
  tax_exempt_type_internal AS ex_typei,
  subdivision AS subdiv,
  detailed_review_date AS reviewdate,
  update_date AS updatedate, 
  accessed_date AS accessdate,
  owner_name AS owner,
  owner_address1 AS owner_add1,
  owner_address2 AS owner_add2,
  district,
  neighborhood_id AS nbhd_id,
  neighborhood AS nbhd,
  year_built,
  year_built_effective AS yearbuiltf,
  sq_feet,
  acres,
  situs_address1 AS situs_add1,
  situs_address2 AS situs_add2, 
  TRIM(CONCAT(COALESCE(situs_address1,"")," " ,COALESCE(situs_address2,""))) AS situs_add,
  situs_city,
  situs_zip,

  -- Assessed MV stats
  market,
  market_lag1 AS ml1,
  market_lag2 AS ml2,
  market_lag3 AS ml3,
  market_lag4 AS ml4,
  market_lag5 AS ml5,
  market_lag6 AS ml6,
  market_lag_max AS ml_max,
  market_max_lag_depth AS ml_depth,

  mc_max,
  mc1,
  mc2,
  mc3,
  mc4,
  mc5,
  mc6,

  mcp_max,
  mcp1,
  mcp2,
  mcp3,
  mcp4,
  mcp5,
  mcp6,

  utah_parcels.geometry

FROM research_public.parcel_market_stats AS main

LEFT JOIN research_public.utah_parcels_prod AS utah_parcels
    ON main.clean_id = utah_parcels.clean_id 
      AND main.county = utah_parcels.county

-- left join any other land, improvement, etc stats you want here

WHERE year = 2024;














-- Check that the grouping was done correctly the cnts should be the same.
SELECT county,
  COUNT( clean_id) AS cnt,
  COUNT( DISTINCT clean_id) AS distinct_cnt
  
FROM `ut-sao-tax-prod.research_public.parcel_stats`
WHERE year = 2024 
  AND tax_exempt = FALSE 
  AND clean_id IS NOT NULL

GROUP BY county
ORDER BY county


-- Check that the grouping was done correctly the cnts should be the same.
SELECT county,
  COUNT( clean_id) AS cnt,
  COUNT( DISTINCT clean_id) AS distinct_cnt
  
FROM `ut-sao-tax-prod.research_public.parcel_stats_agg`
WHERE year = 2024 
  AND tax_exempt = FALSE 
  AND clean_id IS NOT NULL

GROUP BY county
ORDER BY county

-- Check that the grouping was done correctly the cnts should be the same.
SELECT county,
  COUNT( clean_id) AS cnt,
  COUNT( DISTINCT clean_id) AS distinct_cnt
  
FROM `ut-sao-tax-prod.research_public.parcel_market_stats`
WHERE year = 2024 
  AND tax_exempt = FALSE 
  AND clean_id IS NOT NULL

GROUP BY county
ORDER BY county




