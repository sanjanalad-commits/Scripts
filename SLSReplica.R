# ================================
# SLS Data Replication Script (Oct 28+ for large tables)
# Project: slstrategy-467606
# ================================

library(bigrquery)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

# ------------------------
# Configuration
# ------------------------
source_project <- "slstrategy-467606"
target_project <- "slstrategy"             # Your personal project
target_dataset <- "slstrategy_replica"     # Target dataset

# Authenticate with service account
bq_auth(path = "/home/sanjana_lad/slstrategy-467606-5d7314708d4d.json")

# ------------------------
# Tables to replicate
# ------------------------
tables_to_replicate <- list(
  # Small tables (replicate as-is)
  list(dataset="callcenter_views", name="agent_teams"),
  list(dataset="callcenter", name="call_details_v"),
  list(dataset="callcenter", name="calls_sample"),
  list(dataset="callcenter", name="campaigns_v"),
  list(dataset="callcenter_views", name="daily_calls_report"),
  list(dataset="callcenter_views", name="funnel_status"),
  list(dataset="callcenter_views", name="patch_through"),
  list(dataset="callcenter_views", name="patch_through_basic"),
  list(dataset="sms_views", name="campaigns", target_name="sms_campaigns"),
  list(dataset="sms_views", name="messages", target_name="sms_messages"),
  list(dataset="sms_views", name="sms_responses"),
  list(dataset="callcenter_views", name="survey_question_reports"),
  list(dataset="callcenter", name="surveys_v")
)

# Large tables (only Oct 28+)
large_tables <- list(
  list(dataset="callcenter", name="contacts_v", filter_col="last_updated", filter_date="2025-10-28"),
  list(dataset="callcenter", name="calls_v", filter_col="call_date", filter_date="2025-10-28")
)

# ------------------------
# Helper: Clean table data
# ------------------------
clean_table_data <- function(table_name, data) {
  if (nrow(data) == 0) return(data)
  if (table_name %in% c("call_details_v", "calls_v", "contacts_v")) {
    data <- data %>%
      mutate(across(where(is.character), ~ str_trim(.))) %>%
      mutate(across(ends_with("_date"), as.Date, origin="1970-01-01"))
  }
  return(data)
}

# ------------------------
# Function to replicate small tables
# ------------------------
replicate_small_table <- function(table_info) {
  target_name <- ifelse(!is.null(table_info$target_name), table_info$target_name, table_info$name)
  cat("Replicating", table_info$dataset, ".", table_info$name, "->", target_name, "...\n")
  
  query <- paste0("SELECT * FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "`")
  result <- bq_project_query(target_project, query)
  data <- bq_table_download(result)
  
  data_cleaned <- clean_table_data(target_name, data)
  target_table <- bq_table(target_project, target_dataset, target_name)
  bq_table_upload(target_table, data_cleaned, write_disposition="WRITE_TRUNCATE")
  
  cat("✓ Completed", target_name, "-", nrow(data_cleaned), "rows\n")
}

# ------------------------
# Function to replicate large tables with date filter
# ------------------------
replicate_large_table <- function(table_info, chunk_size = 100000) {
  target_name <- ifelse(!is.null(table_info$target_name), table_info$target_name, table_info$name)
  col <- table_info$filter_col
  date <- table_info$filter_date
  cat("Replicating", table_info$dataset, ".", table_info$name, "(from", date, ") ->", target_name, "...\n")
  
  offset <- 0
  repeat {
    query <- paste0(
      "SELECT * FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "` ",
      "WHERE ", col, " >= '", date, "' ",
      "LIMIT ", chunk_size, " OFFSET ", offset
    )
    result <- bq_project_query(target_project, query)
    data <- bq_table_download(result)
    
    if (nrow(data) == 0) break
    
    data_cleaned <- clean_table_data(target_name, data)
    target_table <- bq_table(target_project, target_dataset, target_name)
    bq_table_upload(target_table, data_cleaned,
                    write_disposition = ifelse(offset==0, "WRITE_TRUNCATE", "WRITE_APPEND"))
    
    cat("Uploaded rows:", offset+1, "to", offset+nrow(data_cleaned), "\n")
    offset <- offset + chunk_size
  }
}

# ------------------------
# Run replication
# ------------------------
cat("Starting SLS Data Replication (Oct 28+ for calls_v & contacts_v)...\n")

# Small tables
for (table in tables_to_replicate) {
  tryCatch({
    replicate_small_table(table)
  }, error = function(e) {
    cat("✗ Failed", table$name, ":", e$message, "\n")
  })
  Sys.sleep(1)
}

# Large tables
for (table in large_tables) {
  tryCatch({
    replicate_large_table(table)
  }, error = function(e) {
    cat("✗ Failed", table$name, ":", e$message, "\n")
  })
  Sys.sleep(1)
}

cat("\n=== REPLICATION COMPLETE ===\n")
