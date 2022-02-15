SELECT 
    ID
    , count(totalflow) AS totalflow_count
FROM
    {{ ref('weekday') }}
GROUP BY ID