-- POC: Daniel Chou
WITH
  split_subjects AS (
    -- split comma-delimited subjects as an array of individual subjects
    -- Note: a subject is the investment funding source, and they tend to the larger entity in the financial transaction
  SELECT
    entity_id,
    SPLIT(subjects, ",") AS subject_list,
    year,
    series,
    amount,
    currency
  FROM
    gcp_cset_amplyfi.amplyfi_deduplicated_investments_zh ),
  flattened_subjects AS (
    -- flatten subject array into separate rows
  SELECT
    entity_id,
    subject,
    year,
    series,
    amount,
    currency
  FROM
    split_subjects a,
    UNNEST(a.subject_list) AS subject),
  flattened_subjects_entities AS (
    -- link subject with its info from entities table
  SELECT
    a. entity_id,
    b.original AS orginal_subject_str,
    year,
    series,
    amount,
    currency
  FROM
    flattened_subjects a
  LEFT JOIN
    gcp_cset_amplyfi.amplyfi_entities_zh b
  ON
    a.subject = b.entity_id),
  concatenated_subjects_entities AS (
    -- concatenate multliple subjects that invested in the entity as as single semi-colon delimited string
  SELECT
    -- resolve subject id to subject original str (subject original instead of subject entity id)
    entity_id,
    STRING_AGG(orginal_subject_str, ";") AS original_subjects,
    year,
    series,
    amount,
    currency
  FROM
    flattened_subjects_entities
  GROUP BY
    entity_id,
    year,
    series,
    amount,
    currency),
  split_sentences AS (
  SELECT
    entity_id,
    CASE
      WHEN sentences IS NULL THEN ["UNAVAILABLE"]
    ELSE
    SPLIT(sentences, ",")
  END
    AS sentence_list,
    year,
    series,
    amount,
    currency,
  FROM
    gcp_cset_amplyfi.amplyfi_deduplicated_investments_zh),
  flattened_sentences AS (
  SELECT
    entity_id,
    sentence,
    year,
    series,
    amount,
    currency
  FROM
    split_sentences a,
    UNNEST(a.sentence_list) AS sentence),
  flattened_sentences_texts AS(
  SELECT
    aa. entity_id,
    bb.text AS sentence_text,
    year,
    series,
    amount,
    currency
  FROM
    flattened_sentences aa
  LEFT JOIN
    gcp_cset_amplyfi.amplyfi_sentences_zh bb
  ON
    aa.sentence = bb.disambiguated_sentence_id),
  concatenated_sentences_texts AS (
  SELECT
    -- resolve sentences id to sentences (text instead of disambiguated sentence id)
    entity_id,
    STRING_AGG(sentence_text, ";") AS sentence_texts,
    year,
    series,
    amount,
    currency
  FROM
    flattened_sentences_texts
  GROUP BY
    entity_id,
    year,
    series,
    amount,
    currency),
  merged_concatenated_subjects_entities_concatenated_sentences_texts AS(
    -- merge concatenated_subjects_entities and concatenated_sentences_texts and keep only one copy of the same info
  SELECT
    COALESCE(aaa.entity_id,
      bbb.entity_id) AS entity_id,
    aaa.original_subjects,
    bbb.sentence_texts,
    COALESCE(aaa.year,
      bbb.year) AS year,
    COALESCE(aaa.series,
      bbb.series) AS series,
    COALESCE(aaa.amount,
      bbb.amount) AS amount,
    COALESCE(aaa.currency,
      bbb.currency) AS currency
  FROM
    concatenated_subjects_entities aaa
  FULL OUTER JOIN
    concatenated_sentences_texts bbb
  ON
    aaa.entity_id = bbb.entity_id
    AND aaa.year = bbb.year
    AND aaa.series = aaa.series
    AND aaa.amount = bbb.amount
    AND aaa.currency = bbb.currency ),
  original_entity_subjects_entities_sentences_texts AS (
    -- Add original entity (investment target)
  SELECT
    DISTINCT aaaa.entity_id,
    bbbb.original AS original_entity,
    aaaa.original_subjects,
    aaaa.sentence_texts,
    aaaa.year,
    aaaa.series,
    aaaa.amount,
    aaaa.currency
  FROM
    merged_concatenated_subjects_entities_concatenated_sentences_texts aaaa
  INNER JOIN
    gcp_cset_amplyfi.amplyfi_entities_zh bbbb
  ON
    aaaa.entity_id = bbbb.entity_id)
SELECT
  -- Use MAX aggregate function as a crude to deduplicate rows
  entity_id,
  original_entity,
  MAX(original_subjects) AS original_subjects,
  MAX(sentence_texts) AS sentence_texts,
  year,
  series,
  amount,
  currency
FROM
  original_entity_subjects_entities_sentences_texts
GROUP BY
  entity_id,
  original_entity,
  year,
  series,
  amount,
  currency
