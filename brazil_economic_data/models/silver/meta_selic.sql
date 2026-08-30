select
    parse_date('%d/%m/%Y', data) as reference_date,
    safe_cast(valor as numeric) as selic_target_rate
from {{ source('bronze', 'meta_selic') }}
