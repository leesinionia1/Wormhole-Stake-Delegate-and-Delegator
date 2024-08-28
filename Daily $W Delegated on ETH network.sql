WITH
  get_delegators AS (
    SELECT
      *,
      DATE_DIFF('day', evt_block_time, NOW()) AS days_delegated,
      DATE_FORMAT(evt_block_time, '%Y-%m-%d') AS delegate_date
    FROM
      (
        SELECT
          delegator,
          toDelegate,
          evt_block_number,
          evt_block_time,
          evt_index,
          evt_tx_hash,
          ROW_NUMBER() OVER (
            PARTITION BY
              delegator
            ORDER BY
              evt_block_number DESC,
              evt_index DESC
          ) AS update_rank
        FROM
          wormhole_ethereum.WToken_evt_DelegateChanged
      ) b
    WHERE
      update_rank = 1
  ),
  get_balances AS (
    SELECT
      delegator,
      SUM(value_decimal) AS w_delegated
    FROM
      (
        SELECT
          delegator,
          - SUM(value / 1e18) AS value_decimal
        FROM
          wormhole_ethereum.WToken_evt_Transfer tf
          INNER JOIN get_delegators gd ON tf."from" = gd.delegator
        GROUP BY
          1
        UNION ALL
        SELECT
          delegator,
          SUM(value / 1e18) AS value_decimal
        FROM
          wormhole_ethereum.WToken_evt_Transfer tf
          INNER JOIN get_delegators gd ON tf.to = gd.delegator
        GROUP BY
          1
      ) a
    GROUP BY
      1
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
        ORDER BY
          w_delegated DESC,
          days_delegated DESC
      ) AS rank,
      *,
      SUM(w_delegated) OVER () AS total_w_delegated
    FROM
      (
        SELECT DISTINCT
          gb.delegator AS delegator_address,
          get_href (
            'https://etherscan.io/address/' || CAST(gb.delegator AS VARCHAR),
            CAST(gb.delegator AS VARCHAR)
          ) AS delegator_address_link,
          NULL AS delegator_name,
          w_delegated,
          gb.w_delegated / SUM(gb.w_delegated) OVER () AS pct_of_delegated,
          days_delegated,
          gd.delegate_date,
          COALESCE(dd.name, CAST(dd.address AS VARCHAR)) AS delegate_name,
          get_href (
            'https://dune.com/bennyback/delegator-breakdown-by-delegate?Delegate+Address=' || CAST(dd.address AS VARCHAR),
            CAST(dd.address AS VARCHAR)
          ) AS delegate_address_link,
          dd.address AS delegate_address,
          NOW() AS last_refresh
        FROM
          get_balances gb
          INNER JOIN get_delegators gd ON gb.delegator = gd.delegator
          LEFT JOIN decode_delegate dd ON gd.toDelegate = dd.address
      ) ranked
  )
SELECT
  rank,
  delegator_address,
  delegator_address_link,
  delegator_name,
  w_delegated,
  pct_of_delegated,
  days_delegated,
  delegate_date,
  delegate_name,
  delegate_address_link,
  delegate_address,
  last_refresh,
  total_w_delegated
FROM
  ranked_delegators
WHERE
  w_delegated > 1e-12
  AND pct_of_delegated > 1e-12
  AND days_delegated > 0
ORDER BY rank, w_delegated DESC;
