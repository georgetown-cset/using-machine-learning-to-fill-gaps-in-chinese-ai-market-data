-- POC: Zach Arnold
CREATE OR REPLACE VIEW
  `gcp-cset-projects.za158_sandbox.amplyfi_for_analysis` AS (
  WITH
    annotated_plus_cb_role AS(
    SELECT
      annotation.*,
      cb_orgs.primary_role AS cb_primary_role
    FROM
      `za158_sandbox.amplyfi_annotated_0824` annotation
    LEFT JOIN
      `gcp_cset_crunchbase.organizations` cb_orgs
    ON
      annotation.cb_url=cb_orgs.cb_url)
  SELECT
    entity_text,
    website,
    CASE
      WHEN is_name_of_company = 'Yes' AND (cb_primary_role = 'company' OR cb_primary_role IS NULL) THEN TRUE
  END
    AS is_company,
    --see also below
    CASE
      WHEN cb_url != 'Not found' AND cb_url IS NOT NULL THEN TRUE
  END
    AS has_cb,
    cb_url,
    cb_ind_ai,
    cb_desc_ai,
    CASE
      WHEN pedata_url != 'Not found' AND pedata_url IS NOT NULL THEN TRUE
  END
    AS has_pedata,
    pedata_url,
    CASE
      WHEN pedata_label_ai = TRUE THEN TRUE
      WHEN pedata_label_ai = FALSE THEN FALSE
      WHEN pedata_label_ai IS NULL AND REGEXP_CONTAINS(LOWER(notes), r"(no label)") THEN FALSE
  END
    AS pedata_label_ai_corrected,
    pedata_desc_ai,
    amplyfi_ai_score
  FROM
    annotated_plus_cb_role
  WHERE
    is_name_of_company = 'Yes'
    AND (cb_primary_role = 'company'
      OR cb_primary_role IS NULL)   --is_company
  ORDER BY
    is_company DESC,
    amplyfi_ai_score DESC)
