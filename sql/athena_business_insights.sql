SELECT 
    CASE 
        WHEN avg_vital_value >= 25 THEN '1. Critical High (>25)'
        WHEN avg_vital_value BETWEEN 20 AND 24.99 THEN '2. Elevated (20-24)'
        WHEN avg_vital_value BETWEEN 15 AND 19.99 THEN '3. Normal (15-19)'
        WHEN avg_vital_value BETWEEN 10 AND 14.99 THEN '4. Low (10-14)'
        ELSE '5. Critical Low (<10)'
    END AS health_category,
    COUNT(patient_id) AS total_patients
FROM patient_360_summary
GROUP BY 1
ORDER BY 1;