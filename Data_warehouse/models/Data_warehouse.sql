{{ config(materialized='table') }}

WITH filtered_data AS (
    SELECT *
    FROM {{ source('public', 'Data_warehouse') }}
    WHERE cleaned_message IS NOT NULL
)

SELECT 
    cleaned_message
FROM filtered_data;
