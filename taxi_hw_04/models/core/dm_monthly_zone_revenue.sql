{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fct_taxi_trips_quarterly_revenue') }}
)
    select 
    -- Revenue grouping 
    year_quarter,
    service_type, 
    -- Revenue calculation 
    sum(total_amount) as revenue_yearly_quarters_total_amount

    from trips_data
    group by 1,2