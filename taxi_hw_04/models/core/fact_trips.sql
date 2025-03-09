{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
pro_trip as (
    select trips_unioned.tripid, 
        trips_unioned.vendorid, 
        trips_unioned.service_type,
        trips_unioned.ratecodeid, 
        trips_unioned.pickup_locationid, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        trips_unioned.dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        trips_unioned.pickup_datetime, 
        EXTRACT(YEAR FROM trips_unioned.pickup_datetime) AS pickup_year,
        EXTRACT(MONTH FROM trips_unioned.pickup_datetime) AS pickup_month,
        trips_unioned.dropoff_datetime, 
        trips_unioned.store_and_fwd_flag, 
        trips_unioned.passenger_count, 
        trips_unioned.trip_distance, 
        trips_unioned.trip_type, 
        trips_unioned.fare_amount, 
        trips_unioned.extra, 
        trips_unioned.mta_tax, 
        trips_unioned.tip_amount, 
        trips_unioned.tolls_amount, 
        trips_unioned.ehail_fee, 
        trips_unioned.improvement_surcharge, 
        trips_unioned.total_amount, 
        trips_unioned.payment_type, 
        trips_unioned.payment_type_description
    from trips_unioned
    inner join dim_zones as pickup_zone
    on trips_unioned.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on trips_unioned.dropoff_locationid = dropoff_zone.locationid
    WHERE trips_unioned.fare_amount > 0 
    AND trips_unioned.trip_distance > 0 
    AND trips_unioned.payment_type_description in ('Cash', 'Credit card')
),

bnb_value as (
select *,PERCENTILE_CONT(fare_amount, 0.90) OVER(
            PARTITION BY service_type, pickup_year, pickup_month
        ) AS p90_value,
        PERCENTILE_CONT(fare_amount, 0.95) OVER(
            PARTITION BY service_type, pickup_year, pickup_month
        ) AS p95_value,
        PERCENTILE_CONT(fare_amount, 0.97) OVER(
            PARTITION BY service_type, pickup_year, pickup_month
        ) AS p97_value
from pro_trip
WHERE pickup_year = 2020 and pickup_month = 4
)

SELECT distinct service_type, p90_value, p95_value, p97_value
from bnb_value