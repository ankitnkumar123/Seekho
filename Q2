WITH events_with_prev AS (
    SELECT
        user_id,
        event_type,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
    FROM
        user_events
),
session_markers AS (
    SELECT
        user_id,
        event_type,
        event_time,
        CASE
            WHEN prev_event_time IS NULL OR 
                 TIMESTAMPDIFF(MINUTE, prev_event_time, event_time) > 30
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM
        events_with_prev
),
session_ids AS (
    SELECT
        user_id,
        event_type,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
    FROM
        session_markers
),
session_metrics AS (
    SELECT
        user_id,
        session_id,
        MIN(event_time) AS session_start_time,
        MAX(event_time) AS session_end_time,
        TIMEDIFF(MAX(event_time), MIN(event_time)) AS session_duration,
        COUNT(*) AS event_count
    FROM
        session_ids
    GROUP BY
        user_id, session_id
)
SELECT
    user_id,
    session_id,
    session_start_time,
    session_end_time,
    TIME_FORMAT(session_duration, '%H:%i:%s') AS session_duration,
    event_count
FROM
    session_metrics
ORDER BY
    user_id, session_start_time;
