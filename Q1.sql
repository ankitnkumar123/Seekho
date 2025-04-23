WITH first_activities AS (
    SELECT
        user_id,
        MIN(activity_date) AS first_activity_date
    FROM
        user_activity
    GROUP BY
        user_id
),
day1_retention AS (
    SELECT
        fa.first_activity_date AS activity_date,
        COUNT(DISTINCT fa.user_id) AS new_users,
        COUNT(DISTINCT CASE 
            WHEN ua.activity_date = DATE_ADD(fa.first_activity_date, INTERVAL 1 DAY) 
            THEN fa.user_id 
            END) AS returned_users
    FROM
        first_activities fa
    LEFT JOIN
        user_activity ua ON fa.user_id = ua.user_id
            AND ua.activity_date = DATE_ADD(fa.first_activity_date, INTERVAL 1 DAY)
    GROUP BY
        fa.first_activity_date
)
SELECT
    activity_date,
    new_users,
    returned_users,
    ROUND(returned_users / new_users, 2) AS day_1_retention_rate
FROM
    day1_retention
ORDER BY
    activity_date;
