{{
    config(
        materialized='table'
    )
}}

WITH dim_zones AS (
    SELECT * 
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
),
fhv_trip AS (
    SELECT 
        dispatching_base_num,
        Affiliated_base_number,
        SR_Flag,
        pickup_datetime,
        EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
        dropoff_datetime,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
        pickup_locationid,
        dropoff_locationid
    FROM {{ ref('stg_fhv_tripdata') }}
),
merge_trip AS (
    SELECT 
        fhv_trip.dispatching_base_num,
        fhv_trip.Affiliated_base_number,
        fhv_trip.SR_Flag,
        fhv_trip.pickup_datetime,
        fhv_trip.pickup_year,
        fhv_trip.pickup_month,
        fhv_trip.dropoff_datetime,
        fhv_trip.trip_duration,
        fhv_trip.pickup_locationid,
        pickup_zones.borough AS pickup_borough, 
        pickup_zones.zone AS pickup_zone, 
        fhv_trip.dropoff_locationid,
        dropoff_zone.borough AS dropoff_borough, 
        dropoff_zone.zone AS dropoff_zone
    FROM fhv_trip
    INNER JOIN dim_zones AS pickup_zones
        ON fhv_trip.pickup_locationid = pickup_zones.locationid
    INNER JOIN dim_zones AS dropoff_zone
        ON fhv_trip.dropoff_locationid = dropoff_zone.locationid
    -- Move filters to WHERE clause
    WHERE pickup_zones.zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND fhv_trip.pickup_year = 2019 
        AND fhv_trip.pickup_month = 11
),
p90_trip AS (
    -- Step 2: Compute the continuous P90 trip_duration per (year, month, pickup_location, dropoff_location)
    SELECT 
        *,
        PERCENTILE_CONT(trip_duration, 0.90) OVER(
            PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid
        ) AS p90_value
    FROM merge_trip
),

boshit AS (SELECT *, 
        DENSE_RANK() over(PARTITION by pickup_zone order by p90_value desc) AS p_rank,
FROM p90_trip)

select distinct(pickup_zone), dropoff_zone from boshit
WHERE p_rank = 2