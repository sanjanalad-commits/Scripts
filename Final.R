# ---------------------------- Load Required Libraries ----------------------------
library(bigrquery)
library(curl)
library(dplyr)
library(lubridate)
library(httpuv)
library(base)
library(googledrive)
library(stringr)
library(tidyr)
library(data.table)
unloadNamespace("plyr")

# ---------------------------- Read CSVs ----------------------------
attended_post <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Post-Web Attendee.csv', stringsAsFactors = FALSE)
nonattended_post <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Post-Web Non-Attendee.csv', stringsAsFactors = FALSE)
pre <- read.csv('/Users/sanjanalad/Downloads/working 1028 Copy of H2 2025 Behind The Scenes Survey Data  - Pre-Webinar.csv', stringsAsFactors = FALSE)
demos <- read.csv('/Users/sanjanalad/Downloads/working 10_28 Copy of H2 2025 Behind The Scenes screening demo sheet - SSOT.csv', stringsAsFactors = FALSE)

# ---------------------------- POST EVENT Cleanup ----------------------------
attended_post$Attended_Status <- 'Attended'
attended_post$TECH <- 'N/A'
nonattended_post$Attended_Status <- 'Did Not Attend'

post <- rbind(attended_post, nonattended_post)
names(post) <- gsub("[.]", "_", names(post))
post$Name <- str_to_title(post$Name)
post <- post[!(apply(post[3:10], 1, function(x) all(x == ""))), ]
post$Name <- trimws(post$Name)
post$Email <- trimws(post$Email)

post <- post %>%
  pivot_longer(cols = 3:10,
               names_to = "Question",
               values_to = "Post_Event_Response")

post$Email <- tolower(post$Email)

# ---------------------------- PRE EVENT Cleanup ----------------------------
names(pre) <- gsub("[.]", "_", names(pre))
pre$Name <- str_to_title(pre$Name)
pre <- pre[!(apply(pre[3:10], 1, function(x) all(x == ""))), ]
pre$Name <- trimws(pre$Name)
pre$Email <- trimws(pre$Email)

pre <- pre %>%
  pivot_longer(cols = 3:10,
               names_to = "Question",
               values_to = "Pre_Event_Response")

pre$Email <- tolower(pre$Email)

# ---------------------------- DEMOS Cleanup ----------------------------
demos <- demos %>%
  mutate(Uber_FName = str_to_title(Uber_FName),
         Uber_LName = str_to_title(Uber_LName),
         Name = paste(Uber_FName, Uber_LName)) %>%
  select(-Uber_FName, -Uber_LName, -SLS_uuiD, -SLS_Phone, -Uber_uuiD, -Uber_phone,
         -RSVP, -Attended, -Pass.Fail, -X2nd.Screen)

demos <- demos %>% select(-Name)
demos$SLS_Email <- tolower(demos$SLS_Email)
demos$Uber_email <- tolower(demos$Uber_email)

# ---------------------------- REMOVE DUPLICATES BEFORE MERGING ----------------------------
setDT(pre); setDT(post); setDT(demos)

# Count before dedupe
pre_before <- nrow(pre)
post_before <- nrow(post)
demos_before <- nrow(demos)

# Remove duplicates
pre <- unique(pre, by = c("Email", "Question"))
post <- unique(post, by = c("Email", "Question"))
demos <- unique(demos, by = "Uber_email")
demos <- unique(demos, by = "SLS_Email")

# Log dedupe results
message("✅ Duplicates removed:")
message("   PRE: ", pre_before - nrow(pre), " removed (", nrow(pre), " remaining)")
message("   POST: ", post_before - nrow(post), " removed (", nrow(post), " remaining)")
message("   DEMOS: ", demos_before - nrow(demos), " removed (", nrow(demos), " remaining)")

# ---------------------------- PRE + DEMOS Merge ----------------------------
pre <- pre %>%
  left_join(demos, by = c("Email" = "SLS_Email"), relationship = "many-to-many") %>%
  left_join(demos, by = c("Email" = "Uber_email"), suffix = c("", "_alt"), relationship = "many-to-many")

cols_to_update <- c("Uber_city_id", "Uber_city_name", "Uber_state", "Uber_flow",
                    "Uber_loyalty_tier", "percent_eats", "percent_rides", "Uber_Lifetime_trips",
                    "Status", "Start.Date", "Years", "Office.Hour.Eligibility")

pre <- pre %>%
  mutate(across(all_of(cols_to_update), ~ coalesce(.x, get(paste0(cur_column(), "_alt"))))) %>%
  select(-ends_with("_alt"))

# ---------------------------- PRE + POST Merge ----------------------------
pre <- pre %>% rename("Pre_Event_Name" = Name)
post <- post %>% rename("Post_Event_Name" = Name)

# merge 1
results <- merge(pre, post, by = c("Email", "Question"), all.x = TRUE)
results <- select(results, -Post_Event_Name)

# merge 2
results <- merge(results, post, by.x = c("SLS_Email", "Question"),
                 by.y = c("Email", "Question"), all.x = TRUE)
results <- select(results, -Post_Event_Name)
cols_to_update <- c("Attended_Status", "Post_Event_Response", "TECH")
for (col in cols_to_update) {
  results[[col]] <- coalesce(results[[paste0(col, ".x")]],
                             results[[paste0(col, ".y")]],
                             results[[col]])
}
results <- results %>% select(-ends_with(".x"), -ends_with(".y"))

# merge 3
results <- merge(results, post, by.x = c("Uber_email", "Question"),
                 by.y = c("Email", "Question"), all.x = TRUE)
results <- select(results, -Post_Event_Name)
for (col in cols_to_update) {
  results[[col]] <- coalesce(results[[paste0(col, ".x")]],
                             results[[paste0(col, ".y")]],
                             results[[col]])
}
results <- results %>% select(-ends_with(".x"), -ends_with(".y"))

# merge 4
results <- merge(results, post, by.x = c("Pre_Event_Name", "Question"),
                 by.y = c("Post_Event_Name", "Question"), all.x = TRUE)
cols_to_update <- c("Attended_Status", "Post_Event_Response", "Email", "TECH")
for (col in cols_to_update) {
  results[[col]] <- coalesce(results[[paste0(col, ".x")]],
                             results[[paste0(col, ".y")]],
                             results[[col]])
}
results <- results %>% select(-ends_with(".x"), -ends_with(".y"))

# ---------------------------- CLEAN FINAL COMPARISON DATA ----------------------------
survey_results <- results %>%
  filter(!is.na(Post_Event_Response)) %>%
  filter(!is.na(Pre_Event_Response) & Pre_Event_Response != "") %>%
  select(-Uber_email, -SLS_Email, -Uber_city_id, -Status) %>%
  rename(Name = Pre_Event_Name) %>%
  rename_with(~ gsub("^Uber_", "", .x)) %>%
  rename_with(~ gsub("[.]", "_", .x)) %>%
  rename_with(~ toupper(.x))

survey_results <- survey_results %>%
  select(NAME, EMAIL, ATTENDED_STATUS, QUESTION, PRE_EVENT_RESPONSE, POST_EVENT_RESPONSE, FLOW,
         LOYALTY_TIER, CITY_NAME, STATE, YEARS, LIFETIME_TRIPS, START_DATE, PERCENT_EATS, PERCENT_RIDES,
         OFFICE_HOUR_ELIGIBILITY, TECH) %>%
  mutate(
    START_DATE = as.Date(START_DATE, format = "%m/%d/%Y"),
    PERCENT_EATS = as.numeric(gsub("[^0-9.]", "", PERCENT_EATS)),
    PERCENT_RIDES = as.numeric(gsub("[^0-9.]", "", PERCENT_RIDES)),
    LIFETIME_TRIPS = as.integer(LIFETIME_TRIPS),
    CITY_NAME = gsub(",\\s*[A-Za-z]+$", "", CITY_NAME),
    QUESTION = gsub("_", " ", QUESTION),
    POST_EVENT_RESPONSE = ifelse(POST_EVENT_RESPONSE == "Neither Agree nor Disagree", "Neither agree nor disagree", POST_EVENT_RESPONSE),
    PRE_EVENT_RESPONSE = ifelse(PRE_EVENT_RESPONSE == "Neither Agree nor Disagree", "Neither agree nor disagree", PRE_EVENT_RESPONSE)
  ) %>%
  filter(!is.na(FLOW), POST_EVENT_RESPONSE != "")

# ---------------------------- UPLOAD TO BIGQUERY ----------------------------
schema <- list(
  bq_field("NAME", "STRING"),
  bq_field("EMAIL", "STRING"),
  bq_field("ATTENDED_STATUS", "STRING"),
  bq_field("QUESTION", "STRING"),
  bq_field("PRE_EVENT_RESPONSE", "STRING"),
  bq_field("POST_EVENT_RESPONSE", "STRING"),
  bq_field("FLOW", "STRING"),
  bq_field("LOYALTY_TIER", "STRING"),
  bq_field("CITY_NAME", "STRING"),
  bq_field("STATE", "STRING"),
  bq_field("YEARS", "INT64"),
  bq_field("LIFETIME_TRIPS", "INT64"),
  bq_field("START_DATE", "DATE"),
  bq_field("PERCENT_EATS", "FLOAT64"),
  bq_field("PERCENT_RIDES", "FLOAT64"),
  bq_field("OFFICE_HOUR_ELIGIBILITY", "STRING"),
  bq_field("TECH", "STRING")
)

project_id <- "slstrategy"
dataset_id <- "UBER_2025"
table_id <- "Survey_Results_Oct2025"
bq_table <- paste0(project_id, ".", dataset_id, ".", table_id)

bq_table_upload(
  x = bq_table,
  values = survey_results,
  fields = schema,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)

# ---------------------------- PIVOT VERSION ----------------------------
survey_pivot <- survey_results %>%
  pivot_longer(cols = c(PRE_EVENT_RESPONSE, POST_EVENT_RESPONSE),
               names_to = "RESPONSE_TYPE",
               values_to = "RESPONSE") %>%
  mutate(RESPONSE_TYPE = ifelse(RESPONSE_TYPE == "PRE_EVENT_RESPONSE", "Pre Event", "Post Event"))

schema_pivot <- list(
  bq_field("NAME", "STRING"),
  bq_field("EMAIL", "STRING"),
  bq_field("ATTENDED_STATUS", "STRING"),
  bq_field("QUESTION", "STRING"),
  bq_field("RESPONSE_TYPE", "STRING"),
  bq_field("RESPONSE", "STRING"),
  bq_field("FLOW", "STRING"),
  bq_field("LOYALTY_TIER", "STRING"),
  bq_field("CITY_NAME", "STRING"),
  bq_field("STATE", "STRING"),
  bq_field("YEARS", "INT64"),
  bq_field("LIFETIME_TRIPS", "INT64"),
  bq_field("START_DATE", "DATE"),
  bq_field("PERCENT_EATS", "FLOAT64"),
  bq_field("PERCENT_RIDES", "FLOAT64"),
  bq_field("OFFICE_HOUR_ELIGIBILITY", "STRING"),
  bq_field("TECH", "STRING")
)

table_id <- "Survey_Pivot_Oct2025"
bq_table <- paste0(project_id, ".", dataset_id, ".", table_id)

bq_table_upload(
  x = bq_table,
  values = survey_pivot,
  fields = schema_pivot,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)

# ---------------------------- DEEP DIVE VERSION ----------------------------
pre <- read.csv('/Users/sanjanalad/Downloads/working 10_28 Copy of H2 2025 Behind The Scenes screening demo sheet - SSOT.csv', stringsAsFactors = FALSE)
attend <- read.csv('/Users/sanjanalad/Downloads/Copy of H2 2025 Behind The Scenes Survey Data  - Attendee.csv', stringsAsFactors = FALSE)
noattend <- read.csv('/Users/sanjanalad/Downloads/Copy of H2 2025 Behind The Scenes Survey Data  - Non-Attendee.csv', stringsAsFactors = FALSE)

# --- Clean column names ---
names(pre) <- gsub("\\.+", "_", names(pre))
names(attend) <- gsub("\\.+", "_", names(attend))
names(noattend) <- gsub("\\.+", "_", names(noattend))

# --- Fix "No" logic ---
if (ncol(pre) >= 13) {
  pre[[10]] <- ifelse(pre[[11]] != "" | pre[[12]] != "" | pre[[13]] != "", "No", pre[[10]])
}

# --- Clear column 4 if condition not met ---
if (ncol(pre) >= 4 && ncol(pre) >= 3) {
  pre[, 4][pre[, 3] != "I’m familiar with Uber Crew and have participated in an event before"] <- ""
}

# --- Keep only text-based columns (remove IDs, metrics, etc.) ---
pre <- pre %>% select(where(~ !is.numeric(.x) | all(is.na(.x)))) 

# --- Pivot all three datasets ---
pre <- pre %>%
  select(1:2, everything()) %>%  # Ensure first two columns are Name and Email
  pivot_longer(cols = -c(1, 2), names_to = "QUESTION", values_to = "RESPONSE")

attend <- attend %>%
  select(1:2, everything()) %>%
  pivot_longer(cols = -c(1, 2), names_to = "QUESTION", values_to = "RESPONSE")

noattend <- noattend %>%
  select(1:2, everything()) %>%
  pivot_longer(cols = -c(1, 2), names_to = "QUESTION", values_to = "RESPONSE")

# --- Add SURVEY type ---
pre$SURVEY <- 'Pre Event'
attend$SURVEY <- 'Attended Post Event'
noattend$SURVEY <- 'Did Not Attend Post Event'

# --- Align column names and bind ---
common_cols <- intersect(names(pre), intersect(names(attend), names(noattend)))
pre <- pre[, common_cols]
attend <- attend[, common_cols]
noattend <- noattend[, common_cols]

rbind <- rbind(pre, attend, noattend)

# --- Clean question text ---
rbind$QUESTION <- gsub("_", " ", rbind$QUESTION)
rbind$QUESTION <- gsub(" Select one( [12])?", "", rbind$QUESTION)
rbind$QUESTION <- gsub(" Select all that apply( [0-9]*)?", "", rbind$QUESTION)
rbind$QUESTION <- gsub("Uber s", "Uber's", rbind$QUESTION)
rbind$QUESTION <- gsub("If you ve", "If you've", rbind$QUESTION)
rbind$QUESTION <- gsub("Did this webinar encourage you to contribute more feedback to Uber ",
                       "Did this webinar encourage you to contribute more feedback to Uber?", rbind$QUESTION)
rbind$QUESTION <- gsub("How likely are you to attend future webinars hosted by Uber Crew ",
                       "How likely are you to attend future webinars hosted by Uber Crew?", rbind$QUESTION)
rbind$QUESTION <- gsub("Are you planning to attend the upcoming round of Office Hours ",
                       "Are you planning to attend the upcoming round of Office Hours?", rbind$QUESTION)
rbind$QUESTION <- gsub("Did this webinar make you more interested in applying to be a Crew Member ",
                       "Did this webinar make you more interested in applying to be a Crew Member?", rbind$QUESTION)
rbind$QUESTION <- gsub("Why didn t you attend The Road Ahead ",
                       "Why didn’t you attend The Road Ahead?", rbind$QUESTION)
rbind$QUESTION <- gsub("Did you receive a notification to register for The Road Ahead ",
                       "Did you receive a notification to register for The Road Ahead?", rbind$QUESTION)
rbind$QUESTION <- gsub("If yes what topic do you plan on booking for ",
                       "If yes, what topic do you plan on booking for?", rbind$QUESTION)

rbind <- rbind[rbind$RESPONSE != "", ]

rbind$QUESTION <- trimws(rbind$QUESTION)
rbind$RESPONSE <- trimws(rbind$RESPONSE)

# ------------------------ Write to BigQuery ------------------------
schema <- list(
  bq_field("NAME", "STRING"),
  bq_field("EMAIL", "STRING"),
  bq_field("QUESTION", "STRING"),
  bq_field("RESPONSE", "STRING"),
  bq_field("SURVEY", "STRING")
)

project_id <- "slstrategy"
dataset_id <- "UBER_2025"
table_id <- "Survey_DeepDive_Oct2025"
bq_table <- paste0(project_id, ".", dataset_id, ".", table_id)

bq_table_upload(
  x = bq_table,
  values = rbind,
  fields = schema,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition = "WRITE_TRUNCATE"
)


message("✅ All three tables uploaded successfully to BigQuery (Oct 2025)")

