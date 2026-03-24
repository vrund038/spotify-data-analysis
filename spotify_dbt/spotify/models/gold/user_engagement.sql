select 
    user_id,
    device_type,
    country,
    count(case when event_type='play' then 1 end) as plays,
    count(case when event_type='skip' then 1 end) as skips,
    count(case when event_type='add_to_playlist' then 1 end) as playlist_add,
    date_trunc('day',event_ts) as day
from {{ ref('spotify_silver') }}
group by user_id,device_type,country,date_trunc('day',event_ts)
order by plays desc