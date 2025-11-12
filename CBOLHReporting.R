# Liberty Hill CBO Agent Mapping & Reporting Script - LOCAL VERSION
# Purpose: Read CBO-to-Agent mapping and generate monthly activity reports
# Version: 1.0 (Local Testing)
# Last Updated: November 10, 2025
# 
# NOTE: This script queries YOUR replicated tables in liberty-hill-462819
# instead of sql-mirror-db to avoid permission issues when running locally.

library(bigrquery)
library(dplyr)
library(tidyr)

# Configuration
target_project <- "liberty-hill-462819"
target_dataset <- "liberty_hill_replica"
mapping_table <- "cbo_agent_mapping"

# Report date range (October 2025)
report_month_start <- "2025-10-01"
report_month_end <- "2025-10-31"

cat("====================================================\n")
cat("Liberty Hill CBO Activity Report Generator (LOCAL)\n")
cat("====================================================\n\n")

# Authenticate with BigQuery
bq_auth(path = "/Users/sanjanalad/Desktop/LH/libertyhill-bigquery.json")

# ============================================
# STEP 1: Load and process CBO mapping from local CSV
# ============================================
cat("STEP 1: Loading CBO-to-Agent mapping from local CSV...\n")

tryCatch({
  # Read the local CSV file
  mapping_data <- read.csv('/Users/sanjanalad/Downloads/Call Hub Agent +Admin Emails - Sheet1 (2).csv')
  
  cat("  Loaded", nrow(mapping_data), "rows\n")
  cat("  Columns:", paste(names(mapping_data), collapse=", "), "\n")
  
  # Clean and expand the mappings
  mapping_cleaned <- mapping_data %>%
    rename(
      cbo = 1,
      callhub_admin_emails = 2,
      callhub_agent_emails = 3,
      callhub_agent_usernames = 4
    ) %>%
    mutate(across(everything(), ~trimws(as.character(.)))) %>%
    # CRITICAL: Forward-fill the CBO column for rows where CBO is blank
    fill(cbo, .direction = "down") %>%
    # NOW filter out completely empty rows
    filter(!is.na(cbo) & cbo != "")
  
  # Expand comma-separated agent emails AND usernames
  mapping_expanded <- mapping_cleaned %>%
    # First expand emails
    mutate(agent_email_list = strsplit(as.character(callhub_agent_emails), ",")) %>%
    unnest(agent_email_list) %>%
    mutate(agent_email = trimws(agent_email_list)) %>%
    select(cbo, agent_email, callhub_agent_usernames) %>%
    filter(!is.na(agent_email) & agent_email != "")
  
  # Now expand usernames and combine
  username_mappings <- mapping_cleaned %>%
    filter(!is.na(callhub_agent_usernames) & callhub_agent_usernames != "") %>%
    mutate(username_list = strsplit(as.character(callhub_agent_usernames), ",")) %>%
    unnest(username_list) %>%
    mutate(agent_username = trimws(username_list)) %>%
    select(cbo, agent_username) %>%
    filter(!is.na(agent_username) & agent_username != "")
  
  # Combine both - create mapping with BOTH email and username
  mapping_final <- mapping_expanded %>%
    left_join(username_mappings, by = "cbo") %>%
    select(cbo, agent_email, agent_username) %>%
    distinct()
  
  cat("  Created", nrow(mapping_final), "agent-to-CBO mappings\n")
  cat("  Unique CBOs:", length(unique(mapping_final$cbo)), "\n")
  cat("\n  Preview of mappings:\n")
  print(head(mapping_final, 10))
  cat("\n")
  
  # Upload to BigQuery
  cat("  Uploading mapping table to BigQuery...\n")
  target_table_ref <- bq_table(target_project, target_dataset, mapping_table)
  bq_table_upload(target_table_ref, mapping_final, write_disposition = "WRITE_TRUNCATE")
  cat("  ✓ Mapping table updated\n\n")
  
}, error = function(e) {
  cat("  ✗ Failed to load mapping:", e$message, "\n")
  print(e)
  stop("Cannot proceed without mapping data")
})

# ============================================
# STEP 2: Get Calls by CBO
# ============================================
cat("STEP 2: Getting calls by CBO...\n")

calls_query <- paste0("
  SELECT 
    COALESCE(m.cbo, 'Unmapped') as cbo_name,
    COUNT(*) as total_calls,
    COUNT(DISTINCT c.contact_id) as unique_contacts_called
  FROM `", target_project, ".", target_dataset, ".calls_v` c
  LEFT JOIN `", target_project, ".", target_dataset, ".", mapping_table, "` m
    ON LOWER(c.agent_username) = LOWER(m.agent_username)
  WHERE c.call_date >= '", report_month_start, "'
    AND c.call_date <= '", report_month_end, "'
  GROUP BY m.cbo
  ORDER BY total_calls DESC
")

calls_result <- bq_project_query(target_project, calls_query)
calls_data <- bq_table_download(calls_result)

cat("  ✓ Retrieved", nrow(calls_data), "CBO call records\n")
print(calls_data)
cat("\n")

# ============================================
# STEP 3: Get SMS/Text Replies by CBO and Contract Type
# ============================================
cat("STEP 3: Getting SMS/text replies (inbound messages) by CBO and contract type...\n")

sms_query <- paste0("
  WITH message_data AS (
    SELECT 
      m.contact_id,
      m.agent_username,
      m.sms_campaign_id,
      c.campaign_name,
      CASE 
        WHEN LOWER(c.campaign_name) LIKE '%county uninc%' THEN 'County Unincorporated'
        WHEN LOWER(c.campaign_name) LIKE '%county inc%' THEN 'County Incorporated'
        WHEN LOWER(c.campaign_name) LIKE '%la city%' OR LOWER(c.campaign_name) LIKE '%long beach city%' THEN 'City'
        ELSE 'Other'
      END as contract_type
    FROM `", target_project, ".", target_dataset, ".sms_messages` m
    LEFT JOIN `", target_project, ".", target_dataset, ".sms_campaigns` c
      ON m.sms_campaign_id = c.campaign_id
    WHERE m.direction = 'inbound'
      AND m.send_date >= '", report_month_start, "'
      AND m.send_date <= '", report_month_end, "'
  )
  SELECT 
    COALESCE(map.cbo, 'Unmapped') as cbo_name,
    md.contract_type,
    COUNT(*) as text_replies,
    COUNT(DISTINCT md.contact_id) as unique_contacts_replied
  FROM message_data md
  LEFT JOIN `", target_project, ".", target_dataset, ".", mapping_table, "` map
    ON LOWER(md.agent_username) = LOWER(map.agent_username)
  GROUP BY map.cbo, md.contract_type
  ORDER BY cbo_name, contract_type
")

sms_result <- bq_project_query(target_project, sms_query)
sms_data <- bq_table_download(sms_result)

cat("  ✓ Retrieved", nrow(sms_data), "CBO SMS records (by contract type)\n")
print(sms_data)
cat("\n")

# ============================================
# STEP 4: Combine and Create Final Reports
# ============================================
cat("STEP 4: Creating combined activity reports...\n")

# Overall summary (all contract types combined)
sms_overall <- sms_data %>%
  group_by(cbo_name) %>%
  summarise(
    text_replies = sum(text_replies),
    unique_contacts_replied = sum(unique_contacts_replied),
    .groups = "drop"
  )

combined_report_overall <- calls_data %>%
  full_join(sms_overall, by = "cbo_name") %>%
  mutate(
    total_calls = replace_na(total_calls, 0),
    unique_contacts_called = replace_na(unique_contacts_called, 0),
    text_replies = replace_na(text_replies, 0),
    unique_contacts_replied = replace_na(unique_contacts_replied, 0),
    total_interactions = total_calls + text_replies
  ) %>%
  arrange(desc(total_interactions))

# By contract type (SMS only, since calls don't have contract type)
combined_report_by_contract <- sms_data %>%
  arrange(cbo_name, contract_type)

cat("  ✓ Combined reports created\n\n")

# ============================================
# STEP 5: Display and Export Reports
# ============================================
cat("====================================================\n")
cat("MONTHLY ACTIVITY REPORT BY CBO\n")
cat("Report Period:", report_month_start, "to", report_month_end, "\n")
cat("====================================================\n\n")

cat("OVERALL SUMMARY (All Contract Types Combined):\n")
print(combined_report_overall)
cat("\n")

cat("BREAKDOWN BY CONTRACT TYPE (SMS Replies Only):\n")
print(combined_report_by_contract)
cat("\n")

# Export to CSV
output_file_overall <- paste0("/Users/sanjanalad/Downloads/cbo_activity_report_overall_", format(Sys.Date(), "%Y%m%d"), ".csv")
write.csv(combined_report_overall, output_file_overall, row.names = FALSE)
cat("✓ Overall report exported to:", output_file_overall, "\n")

output_file_contract <- paste0("/Users/sanjanalad/Downloads/cbo_activity_report_by_contract_", format(Sys.Date(), "%Y%m%d"), ".csv")
write.csv(combined_report_by_contract, output_file_contract, row.names = FALSE)
cat("✓ Contract type report exported to:", output_file_contract, "\n")

# Upload BOTH reports to BigQuery
cat("\nUploading reports to BigQuery...\n")

# Overall report
report_table_overall <- "cbo_activity_monthly"
report_table_ref_overall <- bq_table(target_project, target_dataset, report_table_overall)
bq_table_upload(report_table_ref_overall, combined_report_overall, write_disposition = "WRITE_TRUNCATE")
cat("✓ Overall report uploaded: ", paste0(target_project, ".", target_dataset, ".", report_table_overall), "\n")

# By contract type report
report_table_contract <- "cbo_activity_by_contract"
report_table_ref_contract <- bq_table(target_project, target_dataset, report_table_contract)
bq_table_upload(report_table_ref_contract, combined_report_by_contract, write_disposition = "WRITE_TRUNCATE")
cat("✓ Contract type report uploaded: ", paste0(target_project, ".", target_dataset, ".", report_table_contract), "\n")
cat("  Emily can now query both tables!\n")

# Summary statistics
cat("\n====================================================\n")
cat("SUMMARY\n")
cat("====================================================\n")
cat("Total CBOs:", length(unique(combined_report_overall$cbo_name)), "\n")
cat("Total Calls:", sum(combined_report_overall$total_calls), "\n")
cat("Total Text Replies:", sum(combined_report_overall$text_replies), "\n")
cat("Total Interactions:", sum(combined_report_overall$total_interactions), "\n")

cat("\nBreakdown by Contract Type:\n")
contract_summary <- combined_report_by_contract %>%
  group_by(contract_type) %>%
  summarise(
    text_replies = sum(text_replies),
    unique_contacts = sum(unique_contacts_replied),
    .groups = "drop"
  )
print(contract_summary)

cat("\nTop 3 CBOs by Activity:\n")
print(head(combined_report_overall %>% select(cbo_name, total_interactions), 3))

cat("\n====================================================\n")
cat("Report generation completed at", format(Sys.time()), "\n")
cat("====================================================\n")
cat("\nEmily can query this data in Python/BigQuery using:\n")
cat("\nOverall Summary:\n")
cat("  SELECT * FROM `liberty-hill-462819.liberty_hill_replica.cbo_activity_monthly`\n")
cat("\nBy Contract Type:\n")
cat("  SELECT * FROM `liberty-hill-462819.liberty_hill_replica.cbo_activity_by_contract`\n")
cat("  WHERE contract_type = 'City'\n")
cat("====================================================\n")
