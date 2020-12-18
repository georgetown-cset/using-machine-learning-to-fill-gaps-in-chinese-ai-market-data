-- POC: Zach Arnold
CREATE OR REPLACE VIEW
  `gcp-cset-projects.za158_sandbox.amplyfi_for_analysis_dedupe` AS ( /*
 Delete entity_text and begin the row with the website URL.
  For each of the t/f columns, if any row is true, pick true, otherwise pick false.
  For the ai_score column, pick the highest value.
  For the _url columns, pick the "min" lexicographical instance
  TODO: Pick the most common URL columns
 */
  SELECT
    -- collatee all the boolean columns as arrays
    website,
    LOGICAL_OR(is_company) AS is_company,
    LOGICAL_OR(has_cb) AS has_cb,
    MIN(cb_url) AS cb_url,
    LOGICAL_OR(cb_ind_ai) AS cb_ind_ai,
    LOGICAL_OR(cb_desc_ai) AS cb_desc_ai,
    LOGICAL_OR(has_pedata) AS has_pedata,
    MIN(pedata_url) AS pedata_url,
    LOGICAL_OR(pedata_label_ai_corrected) AS pedata_label_ai_corrected,
    LOGICAL_OR(pedata_desc_ai) AS pedata_desc_ai,
    MAX(amplyfi_ai_score) AS amplyfi_ai_score
  FROM (
    SELECT
      -- handle null entries
      CASE
        WHEN website IS NULL THEN "UNKNOWN"
      ELSE
      website
    END
      AS website,
      CASE
        WHEN is_company IS NULL THEN FALSE
      ELSE
      is_company
    END
      AS is_company,
      has_cb,
      CASE
        WHEN cb_url IS NULL THEN "UNKNOWN"
      ELSE
      cb_url
    END
      AS cb_url,
      CASE
        WHEN cb_ind_ai IS NULL THEN FALSE
      ELSE
      cb_ind_ai
    END
      AS cb_ind_ai,
      CASE
        WHEN cb_desc_ai IS NULL THEN FALSE
      ELSE
      cb_desc_ai
    END
      AS cb_desc_ai,
      CASE
        WHEN has_pedata IS NULL THEN FALSE
      ELSE
      has_pedata
    END
      AS has_pedata,
      CASE
        WHEN pedata_url IS NULL THEN "UNKNOWN"
      ELSE
      pedata_url
    END
      AS pedata_url,
      CASE
        WHEN pedata_label_ai_corrected IS NULL THEN FALSE
      ELSE
      pedata_label_ai_corrected
    END
      AS pedata_label_ai_corrected,
      CASE
        WHEN pedata_desc_ai IS NULL THEN FALSE
      ELSE
      pedata_desc_ai
    END
      AS pedata_desc_ai,
      CASE
        WHEN amplyfi_ai_score IS NULL THEN 0.0
      ELSE
      amplyfi_ai_score
    END
      AS amplyfi_ai_score
    FROM
      `gcp-cset-projects.za158_sandbox.amplyfi_for_analysis`)
  GROUP BY
    website)
