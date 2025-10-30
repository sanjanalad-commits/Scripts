# SL Strategy Complete Data Replication and Cleaning Script
# Version: 1.0
# Last Updated: October 29, 2025

library(bigrquery)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

# -------------------- CONFIGURATION --------------------
source_project <- "slstrategy-467606"          # Source project
target_project <- "your-personal-project"     # Replace with your BQ project for replica
target_dataset <- "slstrategy_replica"        # Target dataset for replica

# Authenticate with SL Strategy service account
bq_auth(path = "/Users/sanjanalad/Desktop/LH/slstrategy-467606-5d7314708d4d.json")

# -------------------- TABLES TO REPLICATE --------------------
tables_to_replicate <- list(
  # Core Call Center Tables
  list(name = "calls_v", dataset = "callcenter"),
  list(name = "contacts_v", dataset = "callcenter"),
  list(name = "campaigns_v", dataset = "callcenter"),
  list(name = "call_details_v", dataset = "callcenter"),
  list(name = "survey_responses_v", dataset = "callcenter"),
  list(name = "surveys_v", dataset = "callcenter"),

  # Performance Analytics Tables
  list(name = "daily_calls_report", dataset = "callcenter_views"),
  list(name = "survey_question_reports", dataset = "callcenter_views"),
  list(name = "funnel_status", dataset = "callcenter_views"),
  list(name = "agent_teams", dataset = "callcenter_views"),

  # Patch-Through Tables
  list(name = "patch_through", dataset = "callcenter_views"),
  list(name = "patch_through_basic", dataset = "callcenter_views"),

  # SMS Campaign Data
  list(name = "campaigns", dataset = "sms_views", target_name = "sms_campaigns"),
  list(name = "sms_responses", dataset = "sms_views")
)

# -------------------- DATA CLEANING FUNCTION --------------------
clean_table_data <- function(table_name, data) {
  cat("Cleaning", table_name, "...\n")
  
  # Calls table cleaning
  if (table_name == "calls_v") {
    data <- data %>%
      mutate(
        phone_number = str_remove(phone_number, "^\\+") %>% trimws(),
        call_date = as.Date(call_date),
        system_disposition = toupper(trimws(system_disposition)),
        call_duration = as.numeric(call_duration)
      )
  }
  
  # Contacts table cleaning
  if (table_name == "contacts_v") {
    data <- data %>%
      mutate(
        phone_number = str_remove(phone_number, "^\\+") %>% trimws(),
        mobile_number = str_remove(mobile_number, "^\\+") %>% trimws(),
        first_name = trimws(first_name),
        last_name = trimws(last_name),
        city = trimws(city),
        state = trimws(state),
        zip = trimws(zip),
        email = tolower(trimws(email))
      )
  }
  
  # Survey responses cleaning
  if (table_name == "survey_responses_v") {
    data <- data %>%
      mutate(
        survey_answer = trimws(survey_answer),
        survey_response_date = as.Date(survey_response_date)
      )
  }
  
  # Campaigns cleaning
  if (table_name == "campaigns_v") {
    data <- data %>%
      mutate(campaign_name = trimws(campaign_name))
  }
  
  return(data)
}

# -------------------- REPLICATION FUNCTION --------------------
replicate_table <- function(table_info) {
  target_name <- if (!is.null(table_info$target_name)) table_info$target_name else table_info$name
  
  cat("Replicating", table_info$dataset, ".", table_info$name, "->", target_name, "...\n")
  
  tryCatch({
    # Count rows in source table
    count_query <- paste0("SELECT COUNT(*) as cnt FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "`")
    count_result <- bq_project_query(target_project, count_query)
    count_data <- bq_table_download(count_result)
    row_count <- count_data$cnt[1]
    
    if (row_count == 0) {
      cat("Table is empty. Creating empty table with schema...\n")
      create_query <- paste0(
        "CREATE OR REPLACE TABLE `", target_project, ".", target_dataset, ".", target_name, "` AS ",
        "SELECT * FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "` WHERE FALSE"
      )
      bq_project_query(target_project, create_query)
      cat("✓ Completed", target_name, "- empty table created\n")
      return(TRUE)
    }
    
    # Table has data
    query <- paste0("SELECT * FROM `", source_project, ".", table_info$dataset, ".", table_info$name, "`")
    result <- bq_project_query(target_project, query)
    data <- bq_table_download(result)
    
    # Clean data
    data_cleaned <- clean_table_data(target_name, data)
    
    # Upload to target dataset
    target_table <- bq_table(target_project, target_dataset, target_name)
    bq_table_upload(target_table, data_cleaned, write_disposition = "WRITE_TRUNCATE")
    
    cat("✓ Completed", target_name, "-", nrow(data_cleaned), "rows\n")
    return(TRUE)
    
  }, error = function(e) {
    cat("✗ Failed", target_name, ":", e$message, "\n")
    return(FALSE)
  })
}

# -------------------- RUN REPLICATION --------------------
cat("Starting SL Strategy complete data replication...\n")
success_count <- 0

for (table in tables_to_replicate) {
  if (replicate_table(table)) success_count <- success_count + 1
  Sys.sleep(1)
}

cat("\n=== REPLICATION SUMMARY ===\n")
cat("Successfully replicated:", success_count, "out of", length(tables_to_replicate), "tables\n")

# Verify tables in replica dataset
replicated_tables <- bq_dataset_tables(bq_dataset(target_project, target_dataset))
cat("Total tables in replica dataset:", length(replicated_tables), "\n")
