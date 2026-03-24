with bronze_data as (
    select 
        event_id,
        user_id,
        song_id,
        artist_name,
        song_name,
        event_type,
        device_type,
        country,
        timestamp as event_ts
    from {{ source('bronze','spotify_events') }}
)

select * 
from bronze_data 
where event_id is not null
and user_id is not null
and song_id is not null
and event_ts is not null