{{ config(materialized='table') }}  -- Corrected: use parentheses instead of square brackets

WITH filtered_data AS (
    SELECT *
    FROM {{ source('public', 'Data_warehouse') }}
    WHERE cleaned_message IS NOT NULL
)

SELECT 
    cleaned_message,
    Date,
    ID
FROM filtered_data
