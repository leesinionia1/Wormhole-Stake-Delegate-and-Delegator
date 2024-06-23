WITH combined_balances AS (
  SELECT
    address,
    balance,
    block_time,
    'ethereum' AS blockchain
  FROM
    tokens_ethereum.balances
  WHERE
    token_address = 0xb0ffa8000886e57f86dd5264b9582b2ad87b2b91
    AND balance != 0
  UNION ALL
  SELECT
    address,
    balance_raw/1e18 as balance,
    block_time,
    'optimism' AS blockchain
  FROM
    tokens_optimism.balances
  WHERE
    token_address = 0xb0ffa8000886e57f86dd5264b9582b2ad87b2b91
    AND balance_raw != 0
  UNION ALL
  SELECT
    address,
    balance,
    block_time,
    'arbitrum' AS blockchain
  FROM
    tokens_arbitrum.balances
  WHERE
    token_address = 0xb0ffa8000886e57f86dd5264b9582b2ad87b2b91
    AND balance != 0
  UNION ALL
  SELECT
    address,
    balance,
    block_time,
    'base' AS blockchain
  FROM
    tokens_base.balances
  WHERE
    token_address = 0xb0ffa8000886e57f86dd5264b9582b2ad87b2b91
    AND balance != 0
),

latest_balances AS (
  SELECT
    address,
    balance,
    blockchain,
    ROW_NUMBER() OVER (PARTITION BY address, blockchain ORDER BY block_time DESC) AS row_num
  FROM
    combined_balances
),

filtered_balances AS (
  SELECT
    address,
    balance,
    blockchain
  FROM
    latest_balances
  WHERE
    row_num = 1
),

ranked_balances AS (
  SELECT
    address,
    balance,
    blockchain,
    SUM(balance) OVER () AS total_balance,
    SUM(balance) OVER (PARTITION BY blockchain) AS sep_balance,
    ROW_NUMBER() OVER (ORDER BY balance DESC) AS rank
  FROM
    filtered_balances
)

SELECT
  address,
  balance,
  blockchain,
  total_balance,
  sep_balance,
  (SELECT balance FROM ranked_balances WHERE rank = 1000) AS top_1000_balance,
  (SELECT balance FROM ranked_balances WHERE rank = 5000) AS top_5000_balance,
  (SELECT balance FROM ranked_balances WHERE rank = 10000) AS top_10000_balance,
  (SELECT balance FROM ranked_balances WHERE rank = 20000) AS top_20000_balance,
  count(address) OVER () AS num_of_stakers
FROM
  ranked_balances
ORDER BY
  balance DESC
