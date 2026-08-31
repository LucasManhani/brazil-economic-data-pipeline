with rates_with_previous as (
    select
        reference_date as effective_date,
        selic_target_rate,
        lag(selic_target_rate) over (
            order by reference_date
        ) as previous_rate
    from {{ ref('meta_selic') }}
)

select
    effective_date,
    selic_target_rate,
    previous_rate,
    selic_target_rate - previous_rate as change_percentage_points,
    case
        when previous_rate is null then 'initial'
        when selic_target_rate > previous_rate then 'increase'
        when selic_target_rate < previous_rate then 'decrease'
    end as change_direction
from rates_with_previous
where
    previous_rate is null
    or selic_target_rate != previous_rate
order by effective_date