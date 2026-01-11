-- Query 1: Get latest transaction per customer
SELECT
    customer_id,
    transaction_id,
    transaction_date,
    amount
FROM (
    SELECT
        customer_id,
        transaction_id,
        transaction_date,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY transaction_date DESC
        ) AS rn
    FROM transactions
) t
WHERE rn = 1;

-- Query 2: Deduplicate records per customer per day
SELECT
    customer_id,
    transaction_date,
    amount
FROM (
    SELECT
        customer_id,
        transaction_date,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, transaction_date
            ORDER BY transaction_id
        ) AS rn
    FROM transactions
) t
WHERE rn = 1;


-- Query 3: Rank customers by monthly spend
SELECT
    customer_id,
    month,
    total_spend,
    RANK() OVER (
        PARTITION BY month
        ORDER BY total_spend DESC
    ) AS spend_rank
FROM (
    SELECT
        customer_id,
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(amount) AS total_spend
    FROM transactions
    GROUP BY customer_id, DATE_TRUNC('month', transaction_date)
) t;


-- Query 4: Latest transaction per customer per month
SELECT
    customer_id,
    month,
    transaction_id,
    transaction_date,
    amount
FROM (
    SELECT
        customer_id,
        DATE_TRUNC('month', transaction_date) AS month,
        transaction_id,
        transaction_date,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, DATE_TRUNC('month', transaction_date)
            ORDER BY transaction_date DESC
        ) AS rn
    FROM transactions
) t
WHERE rn = 1;

