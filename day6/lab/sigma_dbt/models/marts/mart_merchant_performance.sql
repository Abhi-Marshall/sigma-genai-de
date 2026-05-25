WITH filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM {{ ref('stg_fact_transactions') }}
    WHERE status IN ('COMPLETED', 'FAILED')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    FROM {{ ref('dim_merchant') }}
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        m.merchant_name,
        COUNT(ft.transaction_id) AS total_transactions,
        SUM(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE 0 END) AS total_revenue,
        COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END) AS failed_count,
        (COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END) * 100.0 / COUNT(ft.transaction_id)) AS failure_rate_pct,
        AVG(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE NULL END) AS avg_transaction_value,
        COUNT(DISTINCT ft.customer_id) AS unique_customers
    FROM filtered_transactions ft
    JOIN merchant_details m ON ft.merchant_id = m.merchant_id
    GROUP BY ft.merchant_id, m.merchant_name
)

SELECT
    merchant_id,
    merchant_name,
    total_transactions,
    total_revenue,
    failed_count,
    failure_rate_pct,
    avg_transaction_value,
    unique_customers
FROM aggregated_metrics
