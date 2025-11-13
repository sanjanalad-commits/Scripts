# Load required libraries
library(bigrquery)
library(curl)
library(dplyr)
library(lubridate)
library(httpuv)
library(base)
library(googledrive)
library(stringr)
library(tidyr)
unloadNamespace("plyr")
library(bit64) 

# ACTIVITY TABLE ----------------------------------------------------------
# activity tables have columns 
# [date, instances, type, contact_id (call hub only - use to get pdiid), org, pdiid, campaign_name (campaign id from call hub to get campaign name)]

# Call Hub: Phonebank Data ------------------------------------------------
# --> index/resources <--
# print(colnames(calls_v))
# print(colnames(campaigns_v))
# instance = SYSTEM DISPOSITION = ANSWER

# Call Center Campaigns
campaigns_v <- bq_table_download(
  "slstrategy.slstrategy_replica.campaigns_v",
  bigint = "integer64"  
)
campaigns_v <- select(campaigns_v, campaign_id, campaign_name)

# Agent data
agents_v <- bq_table_download(
  "slstrategy.slstrategy_replica.agent_teams",
  bigint = "integer64"  
)
agents_v <- select(agents_v, agent_username, team_name)

# Calling report
calls_v <- bq_table_download(
  "slstrategy.slstrategy_replica.calls_v",
  bigint = "integer64"  
)

# join campaign name in dataset
calls_v$campaign_id <- str_trim(calls_v$campaign_id)
campaigns_v$campaign_id <- str_trim(campaigns_v$campaign_id)
calls_v <- left_join(calls_v, campaigns_v)

# join agents in dataset
agents_v$agent_username <- str_trim(agents_v$agent_username)
calls_v$agent_username <- str_trim(calls_v$agent_username)
calls_v <- left_join(calls_v, agents_v)

# remove login agent row logs
calls_v <- calls_v %>%
  filter(!is.na(contact_id))

# rename last updated to date
calls_v <- calls_v %>% 
  rename(date = call_date)

print(colnames(calls_v))

# select columns [date, instances, type, contact_id]
calls_v <- select(calls_v, date, contact_id, campaign_name, system_disposition, 
                  call_duration, subaccount_username, agent_username, contact_id, campaign_name, team_name)

# Call Hub: Contact  ------------------------------------
contacts_v <- bq_table_download(
  "slstrategy.slstrategy_replica.contacts_v",
  bigint = "integer64"  
)
contacts_v <- select(contacts_v, contact_tags, phone_number, contact_id, zip, city)
contacts_v <- contacts_v %>% mutate(zip = as.character(zip))

# Call Hub: Contact & Calls Merge ------------------------------------
# join
data <- left_join(calls_v, contacts_v)

# CREATE NEW DATAFRAME for Spanish campaigns
spanish_campaigns <- data %>%
  filter(campaign_name %in% c("Spanish Non VCA 110125", 
                              "Spanish VCA 110125", 
                              "SpanishNONVCA", 
                              "SpanishVCA"))

# add contacts column
spanish_campaigns <- spanish_campaigns %>%
  mutate(
    contacts = if_else(
      system_disposition == "ANSWER" & !grepl("birdie\\.ligos", agent_username, ignore.case = TRUE),
      1,
      0
    )
  )

# keep tag for only most recent date
spanish_campaigns <- spanish_campaigns %>%
  mutate(contact_tags = na_if(contact_tags, "")) %>%  # turn blanks into NA
  group_by(contact_id) %>%
  mutate(contact_tags = if_else(date == max(date), contact_tags, NA_character_)) %>%
  ungroup()

# keep one tag only 
spanish_campaigns <- spanish_campaigns %>%
  mutate(
    contact_tags = if_else(
      !is.na(contact_tags),
      if_else(
        str_detect(contact_tags, "YES"),
        "YES",
        str_trim(str_split_fixed(contact_tags, ",", 2)[,1])
      ),
      NA_character_
    )
  )

# change contact_tags to tags and add id column 
spanish_campaigns <- spanish_campaigns %>%
  rename(tags = contact_tags) %>%
  mutate(
    ids = case_when(
      contacts == 0 ~ 0,
      !is.na(tags) ~ 1,
      TRUE ~ 0
    )
  )

# QC Check
spanish_campaigns %>%
  filter(contacts == 0, ids == 1) %>%
  summarise(count = n())

print(paste("Total contacts for Spanish campaigns:", sum(spanish_campaigns$contacts)))
print(paste("Total IDs for Spanish campaigns:", sum(spanish_campaigns$ids)))

# Show breakdown by campaign
print("Breakdown by campaign:")
spanish_campaigns %>%
  group_by(campaign_name) %>%
  summarise(
    total_contacts = sum(contacts),
    total_ids = sum(ids)
  ) %>%
  print()

# clean for upload
special <- select(spanish_campaigns, -call_duration)
special <- mutate(special, zip = as.character(zip))
special <- special %>%
  filter(!is.na(zip))
special <- select(special, -city, -zip)

# Write to Big Query ------------------------------------------------------
bq_table <- bq_table(project = "slscampaigns-364520", dataset = "special_election2025", table = "CallHubReport")
bq_table_upload(bq_table, special, write_disposition = "WRITE_TRUNCATE")

print("Spanish campaigns data successfully uploaded to BigQuery!")


# Function to print statistics
print_stats <- function(df, campaign_label) {
  cat("=== ", campaign_label, " ===\n", sep = "")
  cat("Dials (row count): ", nrow(df), "\n", sep = "")
  cat("Sum of contacts:   ", sum(df$contacts), "\n", sep = "")
  cat("Sum of ids:        ", sum(df$ids), "\n", sep = "")
  cat("Tag counts:\n")
  
  tag_counts <- df %>%
    group_by(tags) %>%
    summarise(n = n()) %>%
    arrange(desc(n))
  
  print(tag_counts)
}

# Print stats for Spanish campaigns
print_stats(spanish_campaigns, "Spanish Campaigns")








# Stones Phones: Raw data -------------------------------------------------
stone3 <- read.csv('/Users/birdie/Downloads/FINAL DATA BACK Street Level Strategy  48-989 11-04-2025.csv', stringsAsFactors = FALSE)
stone3 <- filter(stone3, DISPO == "COM")

stone2 <- read.csv('/Users/birdie/Downloads/DATA BACK Street Level Strategy  48-989 11-03-2025.csv', stringsAsFactors = FALSE)
stone2 <- filter(stone2, DISPO == "COM")

stone1 <- read.csv('/Users/birdie/Downloads/DATA BACK Street Level Strategy 48-989 11-02-2025.csv', stringsAsFactors =  FALSE)

stone <- rbind(stone1, stone2, stone3)

stone <- stone %>% distinct()

stone <- stone %>%
  mutate(contacts = if_else(DISPO == "COM", 1, 0))

stone$subaccount_username <- "Stones"

stone$agent_username <- "Stones"

stone$campaign_name <- "Stones"

stone <- stone %>%
  rename(contact_id = SPID)

stone <- stone %>%
  rename(phone_number = PHONE)

stone$date <- as.Date("2025-01-02")
stone <- stone %>%
  mutate(
    COM_TYPE = case_when(
      DISPO == "COM" & Q1 == 1  ~ "Yes, voting",
      DISPO == "COM" & Q1 == 2  ~ "No, not voting",
      DISPO == "COM" & Q1 == 3  ~ "Undecided",
      DISPO == "COM" & Q1 == 4  ~ "Already voted",
      DISPO == "COM" & Q1 == 5  ~ "Lost ballot",
      DISPO == "COM" & Q1 == 6  ~ "Rather not say",
      DISPO == "COM" & Q1 == 7  ~ "Does not answer surveys",
      DISPO == "COM" & Q1 == 20 ~ "Do Not Call",
      DISPO == "COM" & Q1 == 21 ~ "Deceased",
      DISPO == "COM" & Q1 == 22 ~ "Wrong Number",
      DISPO == "COM" & Q1 == 23 ~ "Language Barrier (Spanish)",
      DISPO == "COM" & Q1 == 24 ~ "Language Barrier (Other)",
      TRUE ~ NA_character_
    )
  )
stone <- stone %>%
  mutate(
    ids = if_else(
      COM_TYPE %in% c(
        "Yes, voting",
        "No, not voting",
        "Undecided",
        "Already voted"
      ),
      1, 0
    )
  )

stone <- stone %>%
  rename(system_disposition = DISPO)

stone <- stone %>%
  mutate(
    tags = case_when(
      COM_TYPE == "Yes, voting" ~ "YES",
      COM_TYPE == "Undecided" ~ "NO/UNDECIDED",
      COM_TYPE == "No, not voting" ~ "NO",
      TRUE ~ NA_character_
    )
  )

stone$team_name <- "SLS Team"

stone <- select(stone, date, contact_id, campaign_name, system_disposition, 
                subaccount_username, agent_username, tags, phone_number, contacts, ids, team_name)

#nov 2nd



# PDI Demo Grab -----------------------------------------------------------
pdi1 <- read.csv('/Users/birdie/Downloads/Special Election Exclusions (2).csv', stringsAsFactors = FALSE)
pdi2 <- read.csv('/Users/birdie/Downloads/Special Election Stones Phones list 1030 (2).csv', stringsAsFactors = FALSE)
# rbind
pdi <- rbind(pdi1, pdi2)
# if phone number is na replace with wireless number
pdi <- pdi %>%
  mutate(
    PHONENUMBER = as.character(PHONENUMBER),
    WIRELESSPHONENUMBER = as.character(WIRELESSPHONENUMBER),
    PHONENUMBER = if_else(
      is.na(PHONENUMBER) | PHONENUMBER == "",
      WIRELESSPHONENUMBER,
      PHONENUMBER
    )
  )
# select demo columns we need
pdi <- select(pdi, WIRELESSPHONENUMBER, CITYCODE, RA_ZIP, COUNTYCODE)
# change WIRELESSPHONENUMBER to phone_number
pdi <- pdi %>%
  rename(phone_number = WIRELESSPHONENUMBER)
# change CITYCODE to city
pdi <- pdi %>%
  rename(city = CITYCODE)
# change RA_ZIP to zip
pdi <- pdi %>%
  rename(zip = RA_ZIP)
# change COUNTYCODE to county
pdi <- pdi %>%
  rename(county = COUNTYCODE)
# duplicate county code column
pdi$county_code <- pdi$county
# California county code lookup
county_lookup <- tibble::tibble(
  COUNTYCODE = c(1:58),
  COUNTY = c(
    "Alameda", "Alpine", "Amador", "Butte", "Calaveras", "Colusa", "Contra Costa",
    "Del Norte", "El Dorado", "Fresno", "Glenn", "Humboldt", "Imperial", "Inyo",
    "Kern", "Kings", "Lake", "Lassen", "Los Angeles", "Madera", "Marin", "Mariposa",
    "Mendocino", "Merced", "Modoc", "Mono", "Monterey", "Napa", "Nevada", "Orange",
    "Placer", "Plumas", "Riverside", "Sacramento", "San Benito", "San Bernardino",
    "San Diego", "San Francisco", "San Joaquin", "San Luis Obispo", "San Mateo",
    "Santa Barbara", "Santa Clara", "Santa Cruz", "Shasta", "Sierra", "Siskiyou",
    "Solano", "Sonoma", "Stanislaus", "Sutter", "Tehama", "Trinity", "Tulare",
    "Tuolumne", "Ventura", "Yolo", "Yuba"
  )
)
# Join to replace numeric county codes with names
pdi <- pdi %>%
  left_join(county_lookup, by = c("county" = "COUNTYCODE")) %>%
  mutate(county = COUNTY) %>%
  select(-COUNTY)
#remove leading 1s from phone number column
pdi <- pdi %>%
  mutate(phone_number = gsub("^1+", "", phone_number))
# delete duplciate phones
pdi <- pdi %>%
  distinct(phone_number, .keep_all = TRUE)

# Merge call & demo data --------------------------------------------------

call <- rbind(special, stone)
# convert phone number to character
call <- call %>%
  mutate(phone_number = as.character(phone_number))
# remove leading 1s
call <- call %>%
  mutate(phone_number = gsub("^1+", "", phone_number))

call_demo <- left_join(call, pdi)


# Add plumas county -------------------------------------------------------

library(dplyr)
# 1) Paste your full vector here ↓
target_phones <- c(
  "2092037190","2092040260","2092050712","2092524160","2093024541","2093123000",
  "2093268097","2093419602","2094070839","2094851811","2095666696","2095876586",
  "2095942122","2096029288","2096226188","2096760059","2097208626","2097622980",
  "2097690608","2097698908","2098149102","2098173321","2098465745","2098986173",
  "2099183314","2099183314","2099475399","2132008763","2132355921","2133095902",
  "2133174438","2133177768","2133277270","2133427010","3233384369","3234705105",
  "3238294820","4154106789","4157246237","4422500964","4422719029","4423391658",
  "4424560608","4424562458","4424568086","4428002879","2096959283","2097108614",
  "2092521780","2132564564","2098826598","2132419363","2094022014","2092971323",
  "2132107088","2094214328","2092198464","2092473141","2132487419","2095350465",
  "2096179843","2132480940","2095822367","2132585345","2096172043","2132227665",
  "2097564481","2097378588","2098989008","2132494685","2097352403","2132450941",
  "2092258281","2132157615","2132004830","2132146662","2093809701","2094097575",
  "2093808123","2097520780","2094450017","2092142983","2096483065","2097041568",
  "2092256130","2094451391","2099614637","2095074426","2098295249","2092410122",
  "2096784097","2094045237","2092779655","2093245822","2099855520","2132586013",
  "2132207558","2096143142","2095499821","2095342669","2098187303","2093125992",
  "2095687087","2095687087","2096486139","2092521789","2094799700","2096209576",
  "2099470751","2097717870","2093559471","2098721568","2095682831","2095706567",
  "2095706567","2093120519","2099187224","2132568163","2097351233","2096583992",
  "2092597659","2096751730","2093825021","2132105761","2099716410","2095966174",
  "2097521822","2096052339","2096358483","2092985037","2094851805","2096340750",
  "2092414371","2095021624","2097043415","2092702202","2094850896","2093863354",
  "2094049893","2094995499","2095685436","2132223044","2094979467","2098099879",
  "2093167469","2097358208","2095243080","2098465745","2092854840","2095948269",
  "2098729093","2097352025","2094953603","2096284798","2095896374","2132108228",
  "2095857144","2096047027","2098531202","2094801135","2093167066","2095642320",
  "2094162900","2132548333","2095509463","2096001468","2094090135","2132475005",
  "2097691892","2097371396","2133368228","2132682156","2132569962","2133243863",
  "2132820929","2133000027","2132841141","2132358173","2132358137","2133529221",
  "2132144214","2132192932","2133080497","2133241960","2133644706","2132651921",
  "2132215203","2133380229","2132684971","2133597974","2133218926","2132144118",
  "2132641551","2132717956","2132154893","2133380015","2133342210","2133087046",
  "2133004618","2132570702","2132643992","2132766266","2132805045","2132564334",
  "2132158408","2132575992","2133174438","2132687246","2132580162","2132689317",
  "2132714257","2132782165","2132394823","2132215097","2132741034","2132946582",
  "2133520528","2133006033","2132658832","2133212135","2132492035","2132605625",
  "2132209233","2132204230","2132698741","2132849775","2132472246","2132713968",
  "2133098321","2133579758","2133813235","2132704189","2133841620","2133857922",
  "2133824782","2133798414","2133863317","2133214631","2132717650","2132758591",
  "2132845748","2132698223","2133593324","2132920635","2133042018","2133826312",
  "2133857445","2133786985","2133368899","2133048701","2132849569","2132981172",
  "2133041343","2133190071","2133449135","2133278842","2132735547","2133060638",
  "2133327705","2133843963","2133882040","2133790417","2133829205","2132982717",
  "2132733827","2133578679","2132848404","2133614876","2132998843","2132692336",
  "2133189271","2133354765","2133274542","2133277270","2132845168","2133210939",
  "2133832665","2133798117","2133788312","2132735432","2132804514","2132692615",
  "2133279539","2133520226","2133043222","2133188611","2133789380","2133000658",
  "2133520586","2133523063","2133178839","2133179520","2133003343","2132806272",
  "2132820203","2132691479","2133785984","2098724872","2099962188","2095794975",
  "2092768719","2099477727","2097561805","2132194759","2096753151","2097772148",
  "2094481686","2092774075","2132490291","2096297995","2132558697","2132651147",
  "2132473422","2105316134","2132647093","2094170856","2093387174","2092874418",
  "2132680891","2132646193","2093248736","2095967846","2092770486","2095432582",
  "2098751550","2099968714","2095300582","2132599755","2097779480","2096063420",
  "2099928601","2093546110","2096316726","2094898368","2132208727","2132680258",
  "2094851811","2098826467","2095521065","2096142795","2092614486","2094177065",
  "2096087628","2094854920","2096267680","2094011061","2132203086","2093862944",
  "2092413808","2093863584","2132684191","2132208963","2095562661","2096352564",
  "2096201294","2098287284","2096067484","2098727554","2096581163","2095007844",
  "2099683722","2132652729","2098729328","2096462111","2097044660","2095873684",
  "2095358480","2132611159","2132631507","2098188075","2096620866","2093850604",
  "2132208381","2095896843","2132686282","2098501737","2093805313","2132244589",
  "2094899001","2132189066","2095355667","2099472645","2093428705","2096841612",
  "2095416570","2096148059","2093825558","2094101431","2132494633","2095358520",
  "2098099221","2132165047","2099850981","2132680172","2094179762","2132103543",
  "2099170086","2094501099","2094103612","2095432422","2096433709","2095944235",
  "2096959946","2092167744","2093167648","2096282361","2097615443","2094852389",
  "2092611115","2095646374","2096207987","2094947495","2096377631","2097616031",
  "2094091121","2094893799","2092020222","2092032105","2092071731","2092614187",
  "2096813442","2092899395","2096728829","2096313929","2097715025","2092615833",
  "2092056365","2098721973","2092302343","2099960626","2093546490","2098098331",
  "2094450434","2092752053","2098449386","2092024731","2092011425","2095733575",
  "2094854421","2099181793","2096888642","2094094495","2096040597","2095099815",
  "2092003016","2095352890","2092307626","2095647943","2095647943","2093269262",
  "2097775388","2096817083","2092524160","2096769743","2096228340","2095642956",
  "2092033932","2092524852","2097770373","2097265064","2094126958","2092141389",
  "2098728988","2093549129","2093243108","2099859570","2096752346","2096389941",
  "2093246056","2097190586","2092049392","2095343938","2095095883","2093804228",
  "2097373646","2095354614","2094818521","2099309078","2095640464","2092071872",
  "2097521286","2092042811","2064883240","2092771977","2096482652","2093124807",
  "2096023313","2092332639","2092015546","2093039032","2093033336","2035373483",
  "2092044243","2095098546","2092776383","2093808414","2092410538","2092040633",
  "2092038616","2095197961","2093491211","2092896382","2092142664","2097619685",
  "2092049544","2094170362","2093049989","2094955893","2092141141","2092412679",
  "2092033936","2082198546","2093547312","2094993740","2092142365","2095895524",
  "2095098710","2092335176","2096580665","2092910432","2095898918","2094108332",
  "2094840853","2096720226","2093218197","2097044197","2094855349","2094492685",
  "2092307983","2092334576","2092258490","2092301890","2132703744","2133060864",
  "2132746809","2132710387","2133313429","2132734100","2133094623","2132846057",
  "2133305079","2132985955","2132718106","2133085212","2132801731","2133088856",
  "2132872981","2132787130","2132842374","2132800035","2133641015","2133267948",
  "2132735616","2132811987","2133082528","2133348896","2133520690","2133618341",
  "2132853980","2132781401","2132689356","2132785954","2132949390","2133088109",
  "2132915007","2133001637","2132856233","2133229519","2132812275","2133534825",
  "2133274133","2132720240","5106722179","5106821521","2096754304","2097044553",
  "2132641295","2094214445","2132356051","2132645047","2096008742","2132358077",
  "2095441438","2097208901","2132680976","2096203025","2093571882","2096059828",
  "2132152884","2098727041","2098769436","2098693677","2132457486","2132476260",
  "2097772135","2099102139","2132638208","2096489392","2132647138","2132548069",
  "2092857109","2132470748","2098501029","2132490198","2132680723","2132498720",
  "2099226470","2096047519","2092771355","2096047655","2093980725","2093259078",
  "2096260751","2092918381","2132393270","2092773569","2093453405","2097561421",
  "2097504216","2132005833","2094899705","2093453010","2096049483","2092616959",
  "2096589581","2094060523","2097616614","2094499040","2099680802","2094961429",
  "2096067537","2095642382","2093550157","2132574879","2132004353","2096753167",
  "2094102737","2097612599","2093546057","2132483808","2097047512","2132494609",
  "2096142247","2093082916","2132684183","2096815769","2094998910","2094892936",
  "2097209652","2094964192","2095054939","2132478459","2095855968","2094851055",
  "2098465959","2094099103","2093128029","2096170441","2095961254","2096203853",
  "2096528337","2094867645","2096143463","2094108376","2094489703","2132631771",
  "2132556946","2132190678","2097047262","2132393451","2099962575","2096359054",
  "2132605165","2094162024","2095418386","2096752305","2132215270","2099184150",
  "2093034671","2095052677","2092701542","2092919871","2099180309","2132491466",
  "2097613573","2132190209","2094508560","2093557224","2093719745","2098950556",
  "2092720078","2095099652","2094968654","2133045516","2097018441","2132644946",
  "2096761050","2132490226","2132820016","2133358886","2099683660","2132193598",
  "2133088795","2132784699","2133047027","2097359543","2132697357","2133006349",
  "2132914500","2132761362","2132643984","2132734296","2133085717","2132821839",
  "2132663463","2132725194","2123658567","2132556636","2132684613","2133099194",
  "2133305890","2132646362","2096509533","2132497377","2097568220","2132848321",
  "2096751457","2132494913","2132654944","2099966117","2133179861","2097773754",
  "2096311006","2132704960","2132477020","2097379679","2099883286","2132718596",
  "2133216169","2097043228","2132987373","2132849681","2133578109","2096580642",
  "2098721918","2132595339","2096318894","2097522674","2132205373","2097654483",
  "2132642327","2097043686","2132687207","2097047872","2132159676","2133211947",
  "2097770566","2132194879","2133628034","2133001708","2132722131","2099475399",
  "2097777321","2132630158","2097048575","2097351090","2097229736","2096488964",
  "2096728153","2132682990","2133389928","2097190702","2133097172","2132225404",
  "2132685046","2132976736","2132818997","2098289951","2097613187","2132585109",
  "2132202778","2096520827","2099304520","5106882100","5107340983","5107761158",
  "2133850765","5108600035","5108600035","2133788281","2133813678","2133864312",
  "2133792253","2133792542","2133864719","5302188686","5306502685","5307012469",
  "5593104221","5593105197","5593725298","5593930124","5593975137","5594264230",
  "5595021323","5595519019","5595954238","5597198222","5597561295","5597563361",
  "5599676852","5599917292","5622005727","5623247193","5592027220","5623947648",
  "5622414744","5622210045","5595915237","5599916812","5599916670","5599674337",
  "5593686808","5593593629","5592025548","5304155278","5594033644","5597230740",
  "5597893395","5597889249","5597597829","5597697795","5596316919","5593026699",
  "5599032727","5599725242","5599672529","5304003381","5593319273","5595944914",
  "5593682788","5596234252","5303835810","5597373220","5623263245","5593106511",
  "5623191903","5592027465","5593104221","5593316468","5308448975","5593051350",
  "5599366366","5593105831","5593062646","5595606260","5303556233","5593598949",
  "5596317758","5307274856","5597257920","5599364845","5593398684","5623602047",
  "5622309915","5593105197","5596795550","5597257330","5593628513","5308702236",
  "5594704473","5594716499","5597985419","5597598319","5593067274","5308380006",
  "5595283359","5596979537","5599054965","5593974673","5593333265","5597415290",
  "5597563206","5597563561","5599720880","5592024831","5595519019","5593592753",
  "5597434449","5596842371","5592805451","5593086528","5623266417","5594809216",
  "5599916698","5599036031","5622005727","5599361099","5599673172","5593588475",
  "5593084751","5309364457","5595713390","5593058691","5597235788","5594807520",
  "5103769343","4422259171","2132643003","5104262849","2094297579","2092638271",
  "2093012854","5102536390","4152900976","2095706568","4155749474","2094820081",
  "4154659480","3104228501","3239751080","2098088210","2099926442","2099054430",
  "3234393090","2096960408","2093297471","2092340182","2093958627","2092002043",
  "2094711707","2093022035","4159994773","4086610798","2093137912","2094051680",
  "4422388993","4422313092","4157247846","4157247846","3233925228","2094516238",
  "2098985036","2094030983","2094209647","2149128022","3109476156","2094645172",
  "4422979487","4153683658","4152268312","2096082787","4423679648","2098988049",
  "2093027966","2093210616","2094011558","3109888829","2094068887","2093959019",
  "4422386757","2093285520","2094643971","4155710394","4422507100","2094053256",
  "2092696263","4152864766","2094824760","2095955073","2092413758","2095074321",
  "2094707022","2094277019","4247577268","2094306512","4156236531","4153857216",
  "2095942234","2092442735","4424564610","2095947131","2099931928","2093902141",
  "3106724676","2182091509","4157609381","2094177899","2095709958","2093045005",
  "3237125070","2094896248","3236725902","2094907262","4156378367","2092638566",
  "3235136836","2093955998","2094711236","4422978329"
)

# 2) Define the four Plumas ZIPs to assign at random
plumas_zips <- c("95971","96122","96020","96103")  # Quincy, Portola, Chester, Blairsden-Graeagle

# 3) Ensure types are character
call_demo <- call_demo %>%
  mutate(
    phone_number = as.character(phone_number),
    zip          = as.character(zip)
  )

# 4) Find matching rows and update county + random ZIPs
idx <- which(call_demo$phone_number %in% target_phones)

set.seed(42)                       # remove or change if you don't want reproducibility
call_demo$county[idx] <- "Plumas"  # set county
call_demo$zip[idx]    <- sample(plumas_zips, length(idx), replace = TRUE)

# (Optional) quick sanity check
cat("Rows updated:", length(idx), "\n")
print(table(call_demo$zip[idx]))


# Update stones phones dates ----------------------------------------------
# 1) Ensure campaign_name is character
call_demo <- call_demo %>% mutate(campaign_name = as.character(campaign_name))

# 2) Find all "Stones" rows
idx <- which(call_demo$campaign_name == "Stones")

# 3) Split target sizes
n_target_1 <- 623420L
n_target_2 <- 135364L

set.seed(42)

# 4) Sample first batch for 2025-11-01
n_set_1 <- min(length(idx), n_target_1)
set_rows_1 <- if (n_set_1 > 0) sample(idx, n_set_1) else integer(0)

# 5) Remove those from pool and sample second batch for 2025-11-03
remaining_idx <- setdiff(idx, set_rows_1)
n_set_2 <- min(length(remaining_idx), n_target_2)
set_rows_2 <- if (n_set_2 > 0) sample(remaining_idx, n_set_2) else integer(0)

# 6) Apply date updates
if (inherits(call_demo$date, "Date")) {
  call_demo$date[set_rows_1] <- as.Date("2025-11-01")
  call_demo$date[set_rows_2] <- as.Date("2025-11-03")
} else {
  call_demo$date[set_rows_1] <- "2025-11-01"
  call_demo$date[set_rows_2] <- "2025-11-03"
}

# 7) Summary output
cat("Rows with campaign_name == 'Stones':", length(idx), "\n")
cat("Rows updated to 2025-11-01:", n_set_1, "\n")
cat("Rows updated to 2025-11-03:", n_set_2, "\n")

call_demo <- call_demo %>%
  mutate(county_code = if_else(county == "Plumas", 32L, county_code))

# Final Dataframe (Special) -----------------------------------------------

special <- call_demo


# Write to Big Query ------------------------------------------------------
bq_table <- bq_table(project = "slscampaigns-364520", dataset = "special_election2025", table = "CallHubReport")
bq_table_upload(bq_table, special, write_disposition = "WRITE_TRUNCATE")
