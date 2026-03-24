select
    song_id,
    song_name,
    artist_name,
    count(case when event_type='play' then 1 end) as total_plays,
    count(case when event_type='skip' then 1 end) as total_skips
from {{ ref('spotify_silver') }}
group by song_id,song_name,artist_name
order by total_plays desc