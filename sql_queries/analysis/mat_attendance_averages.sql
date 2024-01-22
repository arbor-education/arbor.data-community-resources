-- SQL Query to get the attendance averages per month for all schools in my MAT for the last 3 years.

SELECT 
    application_id, 
    YEAR(date) AS year, 
    MONTH(date) AS month, 
    AVG(IFF(IS_PRESENT, 1, 0)) AS avg_true_proportion
FROM 
    ARBOR_BI_CONNECTOR_PRODUCTION.ARBOR_MIS_ENGLAND_MODELLED.ROLL_CALL_ATTENDANCE
WHERE 
    date >= DATEADD(year, -3, CURRENT_DATE())
GROUP BY 
    application_id, year, month
ORDER BY 
    application_id, year, month;
