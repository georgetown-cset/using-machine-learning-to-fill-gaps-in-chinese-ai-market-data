-- POC: Daniel Chou
SELECT
  entity_id,
  original_entity,
  original_subjects,
  sentence_texts,
  year,
  series,
  amount,
  currency,
  ROUND((amount / units_per_usd), 2) AS amount_usd
FROM
  gcp_cset_amplyfi.cset_amplyfi_deduplicated_investments_zh_friendly a
LEFT JOIN
  open_reference.xe_currency_table_20200528 b
ON
  UPPER(a.currency) = UPPER(b.currency_code)
