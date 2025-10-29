# ============================================================================
# SUMMARY STATISTICS TABLE GENERATOR FOR TABLEAU
# Creates the top KPI cards: Survey Responses, Participation Rates, etc.
# ============================================================================

library(bigrquery)
library(dplyr)
library(stringr)

# ============================================================================
# STEP 1: READ YOUR CSV FILES
# ============================================================================

# Survey data
pre <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Pre-Webinar.csv', stringsAsFactors = FALSE)
attended <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Post-Web Attendee.csv', stringsAsFactors = FALSE)
nonattended <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Post-Web Non-Attendee.csv', stringsAsFactors = FALSE)

# Demographics/SSOT file
demos <- read.csv('/Users/sanjanalad/Downloads/working 10_28 Copy of H2 2025 Behind The Scenes screening demo sheet - SSOT.csv', stringsAsFactors = FALSE)

# ============================================================================
# STEP 2: CLEAN EMAIL ADDRESSES
# ============================================================================

pre$Email <- tolower(trimws(pre$Email))
attended$Email <- tolower(trimws(attended$Email))
nonattended$Email <- tolower(trimws(nonattended$Email))

if ("SLS_Email" %in% names(demos)) {
  demos$SLS_Email <- tolower(trimws(demos$SLS_Email))
}
if ("Uber_email" %in% names(demos)) {
  demos$Uber_email <- tolower(trimws(demos$Uber_email))
}

# ============================================================================
# STEP 2.5: REMOVE DUPLICATES
# ============================================================================

cat("\n========================================\n")
cat("REMOVING DUPLICATES\n")
cat("========================================\n")

# Count before deduplication
pre_before <- nrow(pre)
attended_before <- nrow(attended)
nonattended_before <- nrow(nonattended)
demos_before <- nrow(demos)

# Remove duplicate responses (keep first occurrence)
# For survey responses: same email = duplicate
pre <- pre[!duplicated(pre$Email), ]
attended <- attended[!duplicated(attended$Email), ]
nonattended <- nonattended[!duplicated(nonattended$Email), ]

# For demos: remove duplicates by both email fields
if ("SLS_Email" %in% names(demos)) {
  demos <- demos[!duplicated(demos$SLS_Email), ]
}
if ("Uber_email" %in% names(demos)) {
  demos <- demos[!duplicated(demos$Uber_email), ]
}

# Report deduplication results
cat("PRE EVENT:\n")
cat("  Before:", pre_before, "rows\n")
cat("  After:", nrow(pre), "rows\n")
cat("  Removed:", pre_before - nrow(pre), "duplicates\n\n")

cat("ATTENDED POST:\n")
cat("  Before:", attended_before, "rows\n")
cat("  After:", nrow(attended), "rows\n")
cat("  Removed:", attended_before - nrow(attended), "duplicates\n\n")

cat("NON-ATTENDED POST:\n")
cat("  Before:", nonattended_before, "rows\n")
cat("  After:", nrow(nonattended), "rows\n")
cat("  Removed:", nonattended_before - nrow(nonattended), "duplicates\n\n")

cat("DEMOGRAPHICS:\n")
cat("  Before:", demos_before, "rows\n")
cat("  After:", nrow(demos), "rows\n")
cat("  Removed:", demos_before - nrow(demos), "duplicates\n")
cat("========================================\n\n")

# ============================================================================
# STEP 3: CALCULATE SUMMARY STATISTICS
# ============================================================================

# Pre Event Stats
pre_responses <- sum(!is.na(pre$Email) & pre$Email != "")
pre_unique <- length(unique(pre$Email[!is.na(pre$Email) & pre$Email != ""]))

# Attended Post Event Stats
attended_responses <- sum(!is.na(attended$Email) & attended$Email != "")
attended_unique <- length(unique(attended$Email[!is.na(attended$Email) & attended$Email != ""]))

# Non-Attended Post Event Stats
nonattended_responses <- sum(!is.na(nonattended$Email) & nonattended$Email != "")
nonattended_unique <- length(unique(nonattended$Email[!is.na(nonattended$Email) & nonattended$Email != ""]))

# ============================================================================
# STEP 4: CALCULATE AUDIENCE (from SSOT)
# ============================================================================

# Total eligible population from SSOT
total_audience <- nrow(demos)

# People who attended (from SSOT Attended column or merge with attended list)
if ("Attended" %in% names(demos)) {
  attended_audience <- sum(demos$Attended == "Yes" | demos$Attended == "TRUE" | demos$Attended == TRUE, na.rm = TRUE)
} else {
  # If no Attended column, use the unique emails from attended survey
  attended_emails <- unique(attended$Email[!is.na(attended$Email) & attended$Email != ""])
  attended_audience <- length(attended_emails)
}

# People who didn't attend
nonattended_audience <- total_audience - attended_audience

# ============================================================================
# STEP 5: CREATE SUMMARY TABLE
# ============================================================================

summary_stats <- data.frame(
  Survey = c("Pre Event", "Attended Post", "Non Attended Post"),
  Responses = c(pre_responses, attended_responses, nonattended_responses),
  Matches = c(pre_unique, attended_unique, nonattended_unique),
  Audience = c(total_audience, attended_audience, nonattended_audience),
  stringsAsFactors = FALSE
)

# Calculate participation rate
summary_stats$Participation_Rate <- round(summary_stats$Matches / summary_stats$Audience, 4)

# ============================================================================
# STEP 6: DISPLAY RESULTS
# ============================================================================

cat("\n========================================\n")
cat("SUMMARY STATISTICS TABLE\n")
cat("========================================\n\n")
print(summary_stats)
cat("\n========================================\n")

# ============================================================================
# STEP 7: UPLOAD TO BIGQUERY
# ============================================================================

project_id <- "slstrategy"
dataset_id <- "UBER_2025"
table_id <- "Survey_Summary_Oct2025"

schema <- list(
  bq_field("Survey", "STRING"),
  bq_field("Responses", "INT64"),
  bq_field("Matches", "INT64"),
  bq_field("Audience", "INT64"),
  bq_field("Participation_Rate", "FLOAT64")
)

bq_table <- paste0(project_id, ".", dataset_id, ".", table_id)

cat("Uploading to BigQuery...\n")
bq_table_upload(
  x = bq_table,
  values = summary_stats,
  fields = schema,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)

cat("\n✅ SUCCESS!\n")
cat("Table created: Survey_Summary_Oct2025\n")
cat("Location: slstrategy.UBER_2025.Survey_Summary_Oct2025\n")
cat("\n========================================\n")
cat("NEXT STEPS:\n")
cat("1. In Tableau, connect to BigQuery\n")
cat("2. Add Survey_Summary_Oct2025 table\n")
cat("3. Use it for your top KPI cards\n")
cat("========================================\n")
