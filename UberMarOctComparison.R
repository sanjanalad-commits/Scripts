# ============================================================================
# COMBINE MARCH & OCTOBER SURVEY DATA FOR COMPARISON DASHBOARD
# Creates Survey_Results_Combined and Survey_Pivot_Combined tables
# ============================================================================

library(bigrquery)
library(dplyr)

# ============================================================================
# STEP 1: READ MARCH DATA FROM BIGQUERY
# ============================================================================

project_id <- "slstrategy"
dataset_id <- "UBER_2025"

cat("\n========================================\n")
cat("READING MARCH DATA FROM BIGQUERY\n")
cat("========================================\n")

# Read March Survey_Results
march_results_table <- bq_table(project_id, dataset_id, "Survey_Results_Mar2025")
march_results <- bq_table_download(march_results_table)
cat("✅ March Survey_Results loaded:", nrow(march_results), "rows\n")

# Read March Survey_Pivot
march_pivot_table <- bq_table(project_id, dataset_id, "Survey_Pivot_Mar2025")
march_pivot <- bq_table_download(march_pivot_table)
cat("✅ March Survey_Pivot loaded:", nrow(march_pivot), "rows\n")

# ============================================================================
# STEP 2: READ OCTOBER DATA FROM BIGQUERY
# ============================================================================

cat("\n========================================\n")
cat("READING OCTOBER DATA FROM BIGQUERY\n")
cat("========================================\n")

# Read October Survey_Results
oct_results_table <- bq_table(project_id, dataset_id, "Survey_Results_Oct2025")
oct_results <- bq_table_download(oct_results_table)
cat("✅ October Survey_Results loaded:", nrow(oct_results), "rows\n")

# Read October Survey_Pivot
oct_pivot_table <- bq_table(project_id, dataset_id, "Survey_Pivot_Oct2025")
oct_pivot <- bq_table_download(oct_pivot_table)
cat("✅ October Survey_Pivot loaded:", nrow(oct_pivot), "rows\n")

# ============================================================================
# STEP 3: ADD EVENT COLUMN TO MARCH DATA
# ============================================================================

cat("\n========================================\n")
cat("ADDING EVENT COLUMN\n")
cat("========================================\n")

march_results$EVENT <- "March 2025"
march_pivot$EVENT <- "March 2025"
cat("✅ Added 'March 2025' to March data\n")

# ============================================================================
# STEP 4: ADD EVENT COLUMN TO OCTOBER DATA
# ============================================================================

oct_results$EVENT <- "October 2025"
oct_pivot$EVENT <- "October 2025"
cat("✅ Added 'October 2025' to October data\n")

# ============================================================================
# STEP 5: ENSURE COLUMNS MATCH BEFORE RBIND
# ============================================================================

cat("\n========================================\n")
cat("ALIGNING COLUMNS\n")
cat("========================================\n")

# Get common columns between March and October
common_results_cols <- intersect(names(march_results), names(oct_results))
common_pivot_cols <- intersect(names(march_pivot), names(oct_pivot))

cat("Survey_Results common columns:", length(common_results_cols), "\n")
cat("Survey_Pivot common columns:", length(common_pivot_cols), "\n")

# Select only common columns (in same order)
march_results <- march_results[, common_results_cols]
oct_results <- oct_results[, common_results_cols]

march_pivot <- march_pivot[, common_pivot_cols]
oct_pivot <- oct_pivot[, common_pivot_cols]

# ============================================================================
# STEP 6: COMBINE (RBIND) MARCH AND OCTOBER
# ============================================================================

cat("\n========================================\n")
cat("COMBINING MARCH AND OCTOBER DATA\n")
cat("========================================\n")

# Combine Survey_Results
combined_results <- rbind(march_results, oct_results)
cat("✅ Survey_Results_Combined created:", nrow(combined_results), "rows\n")
cat("   - March:", nrow(march_results), "rows\n")
cat("   - October:", nrow(oct_results), "rows\n")

# Combine Survey_Pivot
combined_pivot <- rbind(march_pivot, oct_pivot)
cat("✅ Survey_Pivot_Combined created:", nrow(combined_pivot), "rows\n")
cat("   - March:", nrow(march_pivot), "rows\n")
cat("   - October:", nrow(oct_pivot), "rows\n")

# ============================================================================
# STEP 7: REORDER COLUMNS (EVENT FIRST FOR EASY FILTERING)
# ============================================================================

# Move EVENT to the first column
combined_results <- combined_results %>% 
  select(EVENT, everything())

combined_pivot <- combined_pivot %>% 
  select(EVENT, everything())

cat("\n✅ EVENT column moved to first position\n")

# ============================================================================
# STEP 8: UPLOAD COMBINED TABLES TO BIGQUERY
# ============================================================================

cat("\n========================================\n")
cat("UPLOADING COMBINED TABLES TO BIGQUERY\n")
cat("========================================\n")

# Create schema for Survey_Results_Combined
schema_results <- list()
for (col_name in names(combined_results)) {
  col_type <- class(combined_results[[col_name]])[1]
  bq_type <- case_when(
    col_type == "integer" ~ "INT64",
    col_type == "numeric" ~ "FLOAT64",
    col_type == "Date" ~ "DATE",
    col_type == "logical" ~ "BOOLEAN",
    TRUE ~ "STRING"
  )
  schema_results <- c(schema_results, list(bq_field(col_name, bq_type)))
}

# Upload Survey_Results_Combined
table_id_results <- "Survey_Results_Combined"
bq_table_results <- paste0(project_id, ".", dataset_id, ".", table_id_results)

cat("Uploading Survey_Results_Combined...\n")
bq_table_upload(
  x = bq_table_results,
  values = combined_results,
  fields = schema_results,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)
cat("✅ Survey_Results_Combined uploaded:", nrow(combined_results), "rows\n\n")

# Create schema for Survey_Pivot_Combined
schema_pivot <- list()
for (col_name in names(combined_pivot)) {
  col_type <- class(combined_pivot[[col_name]])[1]
  bq_type <- case_when(
    col_type == "integer" ~ "INT64",
    col_type == "numeric" ~ "FLOAT64",
    col_type == "Date" ~ "DATE",
    col_type == "logical" ~ "BOOLEAN",
    TRUE ~ "STRING"
  )
  schema_pivot <- c(schema_pivot, list(bq_field(col_name, bq_type)))
}

# Upload Survey_Pivot_Combined
table_id_pivot <- "Survey_Pivot_Combined"
bq_table_pivot <- paste0(project_id, ".", dataset_id, ".", table_id_pivot)

cat("Uploading Survey_Pivot_Combined...\n")
bq_table_upload(
  x = bq_table_pivot,
  values = combined_pivot,
  fields = schema_pivot,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)
cat("✅ Survey_Pivot_Combined uploaded:", nrow(combined_pivot), "rows\n\n")

# ============================================================================
# STEP 9: SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("✅ SUCCESS! COMBINED TABLES CREATED\n")
cat("========================================\n\n")

cat("Tables uploaded to BigQuery:\n")
cat("1. Survey_Results_Combined\n")
cat("   - March 2025:", nrow(march_results), "rows\n")
cat("   - October 2025:", nrow(oct_results), "rows\n")
cat("   - Total:", nrow(combined_results), "rows\n\n")

cat("2. Survey_Pivot_Combined\n")
cat("   - March 2025:", nrow(march_pivot), "rows\n")
cat("   - October 2025:", nrow(oct_pivot), "rows\n")
cat("   - Total:", nrow(combined_pivot), "rows\n\n")

cat("========================================\n")
cat("TABLEAU DASHBOARD INSTRUCTIONS\n")
cat("========================================\n")
cat("1. Duplicate your March dashboard\n")
cat("2. Replace data sources:\n")
cat("   - Survey_Results_Mar2025 → Survey_Results_Combined\n")
cat("   - Survey_Pivot_Mar2025 → Survey_Pivot_Combined\n\n")

cat("3. Add EVENT filter to dashboard:\n")
cat("   - Drag EVENT field to Filters\n")
cat("   - Show filter on dashboard\n")
cat("   - Users can select: March 2025 | October 2025\n\n")

cat("4. Keep ATTENDED_STATUS filter:\n")
cat("   - Attended | Did Not Attend\n\n")

cat("5. Now you can compare:\n")
cat("   - March Attended vs October Attended\n")
cat("   - March Non-Attended vs October Non-Attended\n")
cat("   - Side-by-side metrics!\n")
cat("========================================\n")

# ============================================================================
# STEP 10: PREVIEW THE DATA
# ============================================================================

cat("\nPreview of combined data:\n")
cat("\nSurvey_Results_Combined - First 5 rows:\n")
print(head(combined_results %>% select(EVENT, ATTENDED_STATUS, NAME, EMAIL, QUESTION), 5))

cat("\nSurvey_Pivot_Combined - First 5 rows:\n")
print(head(combined_pivot %>% select(EVENT, RESPONSE_TYPE, ATTENDED_STATUS, NAME, EMAIL), 5))

cat("\n✅ Script complete!\n")

