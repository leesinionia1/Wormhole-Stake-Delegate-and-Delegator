WITH
  get_delegators AS (
    SELECT
      blockchain,
      delegator,
      toDelegate,
      evt_block_time,
      evt_block_time AT TIME ZONE 'UTC' AS evt_block_time_utc,
      DATE_DIFF('day', evt_block_time, NOW()) AS days_delegated,
      DATE_FORMAT(evt_block_time AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s') AS delegate_date,
      ROW_NUMBER() OVER (PARTITION BY blockchain, delegator ORDER BY evt_block_time DESC) AS update_rank
    FROM (
      SELECT
        'Ethereum' AS blockchain,
        delegator,
        toDelegate,
        evt_block_time
      FROM wormhole_ethereum.WToken_evt_DelegateChanged
      UNION ALL
      SELECT
        'Base' AS blockchain,
        delegator,
        toDelegate,
        evt_block_time
      FROM wormhole_base.WToken_evt_DelegateChanged
      UNION ALL
      SELECT
        'Optimism' AS blockchain,
        delegator,
        toDelegate,
        evt_block_time
      FROM wormhole_op_optimism.WToken_evt_DelegateChanged
      UNION ALL
      SELECT
        'Arbitrum' AS blockchain,
        delegator,
        toDelegate,
        evt_block_time
      FROM wormhole_arb_arbitrum.WToken_evt_DelegateChanged
    ) b
  ),
  filtered_delegators AS (
    SELECT
      gd.blockchain,
      gd.delegator,
      gd.toDelegate,
      gd.evt_block_time,
      gd.days_delegated,
      gd.delegate_date
    FROM
      get_delegators gd
    WHERE
      gd.update_rank = 1
  ),
  get_balances AS (
    SELECT
      blockchain,
      delegator,
      SUM(value_decimal) AS w_delegated
    FROM (
      SELECT
        'Ethereum' AS blockchain,
        delegator,
        -SUM(value / 1e18) AS value_decimal
      FROM wormhole_ethereum.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf."from" = gd.delegator AND gd.blockchain = 'Ethereum'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Base' AS blockchain,
        delegator,
        -SUM(value / 1e18) AS value_decimal
      FROM wormhole_base.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf."from" = gd.delegator AND gd.blockchain = 'Base'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Optimism' AS blockchain,
        delegator,
        -SUM(value / 1e18) AS value_decimal
      FROM wormhole_op_optimism.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf."from" = gd.delegator AND gd.blockchain = 'Optimism'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Arbitrum' AS blockchain,
        delegator,
        -SUM(value / 1e18) AS value_decimal
      FROM wormhole_arb_arbitrum.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf."from" = gd.delegator AND gd.blockchain = 'Arbitrum'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Ethereum' AS blockchain,
        delegator,
        SUM(value / 1e18) AS value_decimal
      FROM wormhole_ethereum.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf.to = gd.delegator AND gd.blockchain = 'Ethereum'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Base' AS blockchain,
        delegator,
        SUM(value / 1e18) AS value_decimal
      FROM wormhole_base.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf.to = gd.delegator AND gd.blockchain = 'Base'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Optimism' AS blockchain,
        delegator,
        SUM(value / 1e18) AS value_decimal
      FROM wormhole_op_optimism.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf.to = gd.delegator AND gd.blockchain = 'Optimism'
      GROUP BY 1, 2
      UNION ALL
      SELECT
        'Arbitrum' AS blockchain,
        delegator,
        SUM(value / 1e18) AS value_decimal
      FROM wormhole_arb_arbitrum.WToken_evt_Transfer tf
      INNER JOIN filtered_delegators gd ON tf.to = gd.delegator AND gd.blockchain = 'Arbitrum'
      GROUP BY 1, 2
    ) a
    GROUP BY 1, 2
  ),
  total_w_delegated_ethereum AS (
    SELECT
      SUM(gb.w_delegated) AS total_w_delegated_ethereum
    FROM get_balances gb
    WHERE gb.blockchain = 'Ethereum'
  ),
  total_w_delegated_base AS (
    SELECT
      SUM(gb.w_delegated) AS total_w_delegated_base
    FROM get_balances gb
    WHERE gb.blockchain = 'Base'
  ),
  total_w_delegated_optimism AS (
    SELECT
      SUM(gb.w_delegated) AS total_w_delegated_optimism
    FROM get_balances gb
    WHERE gb.blockchain = 'Optimism'
  ),
  total_w_delegated_arbitrum AS (
    SELECT
      SUM(gb.w_delegated) AS total_w_delegated_arbitrum
    FROM get_balances gb
    WHERE gb.blockchain = 'Arbitrum'
  ),
  total_w_delegated_all AS (
    SELECT
      SUM(gb.w_delegated) AS total_w_delegated_all
    FROM get_balances gb
  ),
  unique_delegators_count AS (
    SELECT
      gb.blockchain,
      COUNT(DISTINCT gb.delegator) AS unique_delegators
    FROM get_balances gb
    GROUP BY gb.blockchain
  ),
  decode_delegate AS (
    SELECT
      address,
      name
    FROM
      query_3811994
  ),
  ranked_delegators AS (
    SELECT
      ROW_NUMBER() OVER (
        PARTITION BY gb.blockchain
        ORDER BY gb.w_delegated DESC, gd.days_delegated DESC
      ) AS rank,
      gb.blockchain,
      gb.delegator AS delegator_address,
      get_href(
        CASE 
          WHEN gb.blockchain = 'Ethereum' THEN 'https://etherscan.io/address/' 
          WHEN gb.blockchain = 'Base' THEN 'https://basescan.org/address/' 
          WHEN gb.blockchain = 'Optimism' THEN 'https://optimistic.etherscan.io/address/' 
          WHEN gb.blockchain = 'Arbitrum' THEN 'https://arbiscan.io/address/' 
        END || CAST(gb.delegator AS VARCHAR),
        CAST(gb.delegator AS VARCHAR)
      ) AS delegator_address_link,
      NULL AS delegator_name,
      gb.w_delegated,
      gb.w_delegated / SUM(gb.w_delegated) OVER (PARTITION BY gb.blockchain) AS pct_of_delegated,
      gd.days_delegated,
      gd.delegate_date,
      COALESCE(dd.name, CAST(dd.address AS VARCHAR)) AS delegate_name,
      get_href(
        'https://dune.com/bennyback/delegator-breakdown-by-delegate?Delegate+Address=' || CAST(dd.address AS VARCHAR),
        CAST(dd.address AS VARCHAR)
      ) AS delegate_address_link,
      dd.address AS delegate_address,
      NOW() AS last_refresh,
      SUM(gb.w_delegated) OVER (PARTITION BY gb.blockchain) AS total_w_delegated_blockchain
    FROM
      get_balances gb
      INNER JOIN filtered_delegators gd ON gb.delegator = gd.delegator AND gb.blockchain = gd.blockchain
      LEFT JOIN decode_delegate dd ON gd.toDelegate = dd.address
  ),
  ranked_delegators_filtered AS (
    SELECT
      rd.*
    FROM
      ranked_delegators rd
    WHERE
      rd.w_delegated > 1e-12
      AND rd.pct_of_delegated > 1e-12
      AND rd.days_delegated > 0
  ),
  top_counts AS (
    SELECT
      rdf.blockchain,
      MAX(CASE WHEN rdf.rank = 1000 THEN rdf.w_delegated END) AS tokens_for_top_1k,
      MAX(CASE WHEN rdf.rank = 5000 THEN rdf.w_delegated END) AS tokens_for_top_5k,
      MAX(CASE WHEN rdf.rank = 10000 THEN rdf.w_delegated END) AS tokens_for_top_10k,
      MAX(CASE WHEN rdf.rank = 20000 THEN rdf.w_delegated END) AS tokens_for_top_20k
    FROM ranked_delegators_filtered rdf
    GROUP BY rdf.blockchain
  ),
  total_stakers AS (
    SELECT
      COUNT(DISTINCT rdf.delegator_address) AS num_of_stakers
    FROM ranked_delegators_filtered rdf
  )
SELECT
  rdf.rank,
  rdf.blockchain,
  rdf.delegator_address,
  rdf.delegator_address_link,
  rdf.delegator_name,
  rdf.w_delegated,
  rdf.pct_of_delegated,
  rdf.days_delegated,
  rdf.delegate_date,
  rdf.delegate_name,
  rdf.delegate_address_link,
  rdf.delegate_address,
  rdf.last_refresh,
  rdf.total_w_delegated_blockchain,
  tc.tokens_for_top_1k,
  tc.tokens_for_top_5k,
  tc.tokens_for_top_10k,
  tc.tokens_for_top_20k,
  ts.num_of_stakers,
  twe.total_w_delegated_ethereum,
  twb.total_w_delegated_base,
  two.total_w_delegated_optimism,
  twa.total_w_delegated_arbitrum,
  udc.unique_delegators,
  total_w_delegated_all
FROM
  ranked_delegators_filtered rdf
LEFT JOIN
  top_counts tc ON rdf.blockchain = tc.blockchain
CROSS JOIN
  total_stakers ts
CROSS JOIN
  total_w_delegated_ethereum twe
CROSS JOIN
  total_w_delegated_base twb
CROSS JOIN
  total_w_delegated_optimism two
CROSS JOIN
  total_w_delegated_arbitrum twa
LEFT JOIN
  unique_delegators_count udc ON rdf.blockchain = udc.blockchain
CROSS JOIN
  total_w_delegated_all
ORDER BY rdf.rank, rdf.w_delegated DESC;
