x# ================================
# SLS Partial Replication (Oct 28+)
# Tables: calls_v & contacts_v
# ================================

library(bigrquery)
library(dplyr)
library(lubridate)
library(stringr)

# ------------------------
# Configuration
# ------------------------
source_project <- "slstrategy-467606"
target_project <- "slstrategy"          # your personal project
target_dataset <- "slstrategy_replica"  # target dataset

# Authenticate using JSON key (service account)
bq_auth(path = "/home/sanjana_lad/slstrategy-467606-5d7314708d4d.json")

# ------------------------
# Tables to replicate with date filters
# ------------------------
tables_to_replicate <- list(
  list(
    dataset = "callcenter",
    name = "calls_v",
    date_column = "call_date",
    from_date = "2025-10-28"
  ),
  list(
    dataset = "callcenter",
    name = "contacts_v",
    date_column = "last_updated",
    from_date = "2025-10-28"
  )
)

# ------------------------
# Helper: Clean table data
# ------------------------
clean_table_data <- function(data) {
  if (nrow(data) == 0) return(data)
  
  data <- data %>%
    mutate(across(where(is.character), ~ str_trim(.)))
  
  return(data)
}

# ------------------------
# Replication function
# ------------------------
replicate_table <- function(table_info) {
  target_table_name <- table_info$name
  cat("Replicating", table_info$name, "(from", table_info$from_date, ") ...\n")
  
  query <- paste0(
    "SELECT * FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "` ",
    "WHERE ", table_info$date_column, " >= '", table_info$from_date, "'"
  )
  
  # Run query
  result <- bq_project_query(target_project, query)
  data <- bq_table_download(result)
  
  if (nrow(data) == 0) {
    cat("No new rows to replicate for", table_info$name, "\n")
    return()
  }
  
  data_cleaned <- clean_table_data(data)
  
  # Upload to target dataset
  target_table <- bq_table(target_project, target_dataset, target_table_name)
  bq_table_upload(target_table, data_cleaned, write_disposition = "WRITE_TRUNCATE")
  
  cat("✓ Completed", table_info$name, "-", nrow(data_cleaned), "rows\n")
}

# ------------------------
# Run replication
# ------------------------
cat("Starting SLS Partial Replication (Oct 28+)...\n")
for (tbl in tables_to_replicate) {
  tryCatch({
    replicate_table(tbl)
  }, error = function(e) {
    cat("✗ Failed", tbl$name, ":", e$message, "\n")
  })
}

cat("\n=== REPLICATION COMPLETE ===\n")
