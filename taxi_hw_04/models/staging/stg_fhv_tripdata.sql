{{
    config(
        materialized='view'
    )
}}

with abc as 
(
  select *
  from {{ source('staging','fhv_tripdata_ext') }}
  where dispatching_base_num is not null 
)
select
    dispatching_base_num,
    SR_Flag,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    Affiliated_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid

    
from abc

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}