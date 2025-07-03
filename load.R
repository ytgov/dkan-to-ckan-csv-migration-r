library(tidyverse)
library(fs)
library(readxl)
library(rmarkdown)
library(janitor)


# Source files ------------------------------------------------------------

# Source data file to import
source_dataset_file <- "input/20250627/YG_Open_Gov_DKAN_dataset_export_20250627.xlsx"
source_atipp_requests_file <- "input/20250627/YG_Open_Gov_DKAN_ATIPP_export_20250627.xlsx"
source_pia_summaries_file <- "input/20250627/YG_Open_Gov_DKAN_PIA_export_20250627.xlsx"

source_dataset_resources_file <- "input/20250627/YG_Open_Gov_DKAN_resource_export_20250627.xlsx"
source_atipp_requests_resources_file <- "input/20250627/YG_Open_Gov_DKAN_ATIPP_response_export_20250627.xlsx"

source_geoyukon_dataset_file <- "input/20250624/geoyukon_datasets.csv"
source_geoyukon_resources_file <- "input/20250624/geoyukon_resources.csv"

setting_run_pandoc_markdown_conversions <- TRUE

# Start time logging ------------------------------------------------------

run_start_time <- now()
paste("Start time:", run_start_time)

# Helper functions --------------------------------------------------------

# Note: DKAN currently expects CRLF line endings (not LF) and so those have been specified in the project-specific R settings.
# TODO: Confirm what line endings are needed by Link Digital's CKAN infrastructure.

# This script exports CSV files that don't have BOMs (which Excel adds, and that DKAN can't process).

# Writes a CSV file to the "out/" directory, and returns the data if necessary for future piped functions.
write_out_csv <- function(df, filename, na = "") {
  
  df %>%
    write_csv(
      str_c(filename, ".csv"), 
      na = na,
      eol = "\r\n",
    )
  
  df
  
}

filter_is_published <- function(df) {
  
  df |> 
    filter(is_published == 1)
  
}

mutate_languages <- function(df) {
  
  df |> 
    mutate(
      language = case_when(
        languages == "en-CA" ~ "english",
        languages == "en" ~ "english",
        languages == "en-CA,fr-CA" ~ "multiple_languages",
        languages == "fr-CA" ~ "french",
        .default = ""
      )
    ) |> 
    relocate(
      language, .before = "languages"
    )
  
}

mutate_license_id <- function(df) {
  
  df |> 
    mutate(
      license_id	 = "OGL-Yukon-2.0"
    ) |> 
    relocate(
      license_id, .before = "licence"
    )
  
}

str_datetime_to_time <- function(str) {
  
  str_sub(str, 0L, 10L)
  
}

str_fme_datetime_to_datetime <- function(str) {
  # From 2025-06-24T10:08:15.914Z
  # to   2025-06-24 19:46:34
  
  str_replace_all(str_sub(str, 0L, 19L), "T", " ")
  
}

# Dataset processing ------------------------------------------------------

xlsx_dataset_nodes <- read_excel(source_dataset_file)

# 1. Filter by "type" and "information_type" to retrieve the following:
# dataset
# pia_summary_information
# summary_information

# Note: as of 20250515, the latest export from Dave is already limited to these 3 types.
# 
# datasets <- xlsx_dataset_nodes |> 
#   filter(type %in% c("dataset", "pia_summary_information", "summary_information"))
datasets <- xlsx_dataset_nodes

datasets <- datasets |> 
  mutate(
    schema_type = case_when(
      content_type == "dataset" & information_type == "Open data" ~ "data",
      content_type == "dataset" ~ "information",
      .default = content_type
    )
  ) |> 
  relocate(
    schema_type, .before = "content_type"
  )

# 2. Exclude datasets that were never published

datasets <- datasets |> 
  filter_is_published()

# 9. Exclude active Harvest sources
# This includes 
# -	GeoYukon layers / CSW items
# -	Yukon Geological Survey publications and data
# -	YBS Community Statistics items
# - Yukon Register of Historic Places

# The sources to remove also includes harvested documents that have already been removed, as indicated below.
# datasets |> select(harvest_source, harvest_source_modified) |> arrange(harvest_source_modified) |> distinct() |> View()
# xlsx_dataset_nodes |> count(harvest_source) |> arrange(desc(n)) |> View()

active_harvest_sources_to_remove <- c(
  "GEOMATICS 43_08052019",
  "YGS_Comp_21_08052019",
  "YGS_PUB_OTHER_97_10052019",
  "YGS_Pub_559_10052019",
  "Geomatics layers_10052019",
  "HERITAGE 27_17062019", 
  "CommunityServices_49", # All removed and replaced with redirects to a newer geo application
  "ATIPP AIR", # Replaced by newer ATIPP Registry documents
  "Geomatics Metadata",
  "YBS 105_18092018" # Re-assigned to newer consolidated dataset entries in SC01 below
  
)

datasets <- datasets |>
  mutate(
    is_active_harvest_source = case_when(
      harvest_source %in% active_harvest_sources_to_remove ~ TRUE,
      .default = FALSE
    )
  )

# Remove datasets that are part of active harvest sources from the export
datasets <- datasets |>
  filter(is_active_harvest_source == FALSE)

# SC02. Special case: import GeoYukon datasets and resources, and create imitation DKAN node ID and node parent IDs.

geoyukon_node_starting_id = 110000

geoyukon_datasets <- read_csv(source_geoyukon_dataset_file) |> 
  clean_names()

# Used by rename(), where new = "old"
field_mapping <- c(
  node_id = "id",
  publishers_groups = "publisher",
  homepage_url = "homepage",
  custodian = "author_name",
  last_revised = "modified_date"
  
)

geoyukon_datasets <- geoyukon_datasets |> 
  rename(all_of(field_mapping))

geoyukon_datasets <- geoyukon_datasets |> 
  mutate(
    tags = str_c(tags, ",geoyukon-import,geoyukon-import-20250627"),
    content_type = "dataset",
    schema_type = "data",
    node_id = node_id + geoyukon_node_starting_id,
    last_revised = str_fme_datetime_to_datetime(last_revised),
    # topics = str_to_sentence(topics)
  )

# TODO - additional cleanup here
# [done] fix mailto: emails
# [done] move tag cleanup afterwards
# [done] move frequency cleanup afterwards
# [done] remove data dictionary
# [done] check that dates created and modified look good.
# [done] fix topics
# [done] fix frequency
# add created date to original FME output

# Remove single template(?) entry with a description of {{description}}
geoyukon_datasets <- geoyukon_datasets |> 
  filter(description != "{{description}}")

# Fix a single entry with a topic of "Nature and evnvironment"
# Should be "Nature and environment"
geoyukon_datasets <- geoyukon_datasets |> 
  mutate(
    topics = str_replace_all(topics, "Nature and evnvironment", "Nature and environment")
  )

geoyukon_datasets <- geoyukon_datasets |> 
  mutate(
    contact_email = "Geomatics.Help@yukon.ca",
    data_dictionary = NA_character_,
    authored = last_revised # Update if FME created date is available.
  )


# Bind these together
datasets <- datasets |> 
  bind_rows(geoyukon_datasets)


# 3. Update languages to match the new possible values
# english / french / multiple_languages / other / ""
# TODO - review if we can use ISO language codes while also supporting Yukon First Nations languages
# datasets |> select(languages) |> distinct()

datasets <- datasets |>
  mutate_languages()

# datasets |> count(languages)
# datasets |> count(language)
# datasets |> filter(languages != "und") |> View()

# 4. Update license to use "OGL-Yukon-2.0" reference
# datasets |> filter(licence != "ogly") |> View()

datasets <- datasets |> 
  mutate_license_id()


# 5. Update update frequency and confirm valid input
# none / ad_hoc / annual / annual / semiannual / quarterly / monthly / weekly / daily / hourly / real_time
# datasets |> count(how_often_updated) 
# datasets |> count(frequency)
# 
# datasets |> filter(update_frequency == "R/PT1S") |> View()


# with help from
# https://resources.data.gov/schemas/dcat-us/v1.1/iso8601_guidance/
datasets <- datasets |>
  mutate(
    original_update_frequency = frequency,
    update_frequency	 = case_when(
      is.na(how_often_updated) & frequency == "R/P1D" ~ "daily",
      is.na(how_often_updated) & frequency == "R/P1M" ~ "monthly",
      is.na(how_often_updated) & frequency == "R/P1W" ~ "weekly",
      is.na(how_often_updated) & frequency == "R/P1Y" ~ "annual",
      is.na(how_often_updated) & frequency == "R/P3M" ~ "quarterly",
      is.na(how_often_updated) & frequency == "R/P5Y" ~ "none", # every 5 years
      is.na(how_often_updated) & frequency == "R/PT1S" ~ "daily", # continuously updated
      is.na(how_often_updated) & frequency == "irregular" ~ "ad_hoc",
      
      # GeoYukon frequency labels
      is.na(how_often_updated) & frequency == "Annually" ~ "annual",
      is.na(how_often_updated) & frequency == "As Needed" ~ "ad_hoc",
      is.na(how_often_updated) & frequency == "Continual" ~ "daily",
      is.na(how_often_updated) & frequency == "Daily" ~ "daily",
      is.na(how_often_updated) & frequency == "Irregular" ~ "ad_hoc",
      is.na(how_often_updated) & frequency == "Monthly" ~ "monthly",
      is.na(how_often_updated) & frequency == "Not Planned" ~ "none",
      is.na(how_often_updated) & frequency == "Quarterly" ~ "quarterly",
      is.na(how_often_updated) & frequency == "Unknown" ~ "none",
      is.na(how_often_updated) & frequency == "Weekly" ~ "weekly",
      
      # Additional DKAN frequency labels
      how_often_updated == "Annually" ~ "annual",
      how_often_updated == "Irregularly" ~ "ad_hoc",
      how_often_updated == "Monthly" ~ "monthly",
      how_often_updated == "Quarterly" ~ "quarterly",
      how_often_updated == "Semiannual" ~ "semiannual",
      .default = NA_character_
    )
  ) |> 
  relocate(
    update_frequency, .before = "how_often_updated",
  ) |> 
  relocate(
    original_update_frequency, .before = "how_often_updated",
  ) 

# 6. Update temporal coverage to exclude "Wednesday, December 31, 1969 - 16:00" entries
# datasets |> select(temporal_coverage_from) |> arrange(temporal_coverage_from) |> distinct()
# datasets |> select(temporal_coverage_to) |> arrange(temporal_coverage_to) |> distinct()

datasets <- datasets |>
  mutate(
    temporal_coverage_from = case_when(
      temporal_coverage_from == "1969-12-31 16:00:00" ~ NA_character_,
      .default = temporal_coverage_from
    ),
    temporal_coverage_to = case_when(
      temporal_coverage_to == "1969-12-31 16:00:00" ~ NA_character_,
      .default = temporal_coverage_to
    ),
  )



# 7. Update temporal coverage to be by year rather than by date
datasets <- datasets |> 
  mutate(
    temporal_coverage_from = str_sub(temporal_coverage_from, 0L, 10L),
    temporal_coverage_to = str_sub(temporal_coverage_to, 0L, 10L),
  )

# 8. Combine geographic coverage that's just "Yukon" or "YUKON"
datasets |> select(geographical_coverage_location) |> arrange(geographical_coverage_location) |> distinct()

spatial_coverage_yukon_variants <- c(
  "YUKON",
  "Yukon",
  "Yukon Territory"
)

datasets <- datasets |>
  mutate(
    geographical_coverage_location = case_when(
      geographical_coverage_location %in% spatial_coverage_yukon_variants ~ "Yukon",
      .default = geographical_coverage_location
    )
  )



# 10. Set canonical publisher organizations
# (remove multiple entries; consolidate across related DKAN fields)
# (also reset "Government of Yukon" to either Education or HPW as required)

# Sub-departmental level organizations that we're keeping (since they act in a government-wide role):
# ATIPP Office
# Yukon Bureau of Statistics
# Procurement Support Centre
# Geomatics Yukon

# datasets |> count(publishers_groups) |> arrange(desc(n)) |> View()
# datasets |> filter(str_detect(publishers_groups, ",")) |> count(publishers_groups) |> arrange(desc(n)) |> View()

# Updating "Government of Yukon" entries
datasets <- datasets |>
  mutate(
    publishers_groups = case_when(
      publishers_groups == "Government of Yukon" & topics == "Education and training" ~ "Education",
      publishers_groups == "Government of Yukon" & topics != "Education and training" ~ "ATIPP Office",
      .default = publishers_groups
    )
  )

# Manually updating multiple entries to a single department
# Also cleaning up one-off branch organizations and switching to departmental level
datasets <- datasets |>
  mutate(
    publishers_groups = case_when(
      publishers_groups == "Community Services,Geomatics Yukon" ~ "Community Services",
      publishers_groups == "Education,Highways and Public Works" ~ "Highways and Public Works",
      publishers_groups == "Energy, Mines and Resources,Government of Yukon" ~ "Energy, Mines and Resources",
      
      publishers_groups == "Compliance Monitoring and Inspection branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Client Monitoring and Inspections branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Energy branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Forestry Management branch" ~ "Energy, Mines and Resources",
      
      publishers_groups == "Lands branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Mineral Resources branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Oil and Gas branch" ~ "Energy, Mines and Resources",
      publishers_groups == "Policy and Communications" ~ "Justice",
      
      publishers_groups == "Transportation Maintenance Branch" ~ "Highways and Public Works",
      
      publishers_groups == "Water Resources branch" ~ "Environment",
      publishers_groups == "Wildland Fire Management branch" ~ "Community Services",
      
      publishers_groups == "Women's Directorate" ~ "Women and Gender Equity Directorate",
      
      publishers_groups == "" ~ "Yukon Geological Survey",
      
      .default = publishers_groups
    )
  )


# 11. Remove any tags that aren't used more than 2 times

# Confirm that title is unique; it is.
# datasets |> 
#   count(title) |> 
#   arrange(desc(n))

# Replace "–" with "-" for consistency
# Then separate into rows
dataset_tags <- datasets |> 
  select(title, tags) |> 
  mutate(
    tags = str_replace(tags, "–", "-")
  ) |> 
  separate_longer_delim(tags, ",") |> 
  distinct() # Remove multiple identical entries for the same dataset

# Filter out tags with strange punctuation
dataset_tags <- dataset_tags |> 
  mutate(
    tags = str_to_lower(str_replace(tags, " no. ", " "))
  ) |> 
  filter(! str_detect(tags, "[:.()]"))

# Filter out tags that are just "yukon", "yukon territory", or "yt" etc.
tags_to_remove = c(
  "yukon",
  "yukon territory",
  "yt",
  "yg",
  "ytg"
)

dataset_tags <- dataset_tags |> 
  filter(! tags %in% tags_to_remove)

  
dataset_tags <- dataset_tags |> 
  group_by(tags) |> 
  add_count(tags) |> 
  arrange(desc(n))

# dataset_tags |>
#   select(tags, n) |>
#   distinct() |>
#   View()

reduced_dataset_tags <- dataset_tags |> 
  filter(n >= 2)

# Already grouped by `tags` from add_count() above
# We need to instead group by title to re-merge tags by publication
# str_flatten_comma() adds spaces, so we'll use the normal str_flatten()
reduced_dataset_tags <- reduced_dataset_tags |> 
  ungroup() |> 
  arrange(tags) |> 
  group_by(title) |> 
  mutate(
    tags_combined = str_flatten(tags, collapse = ",")
  )

reduced_dataset_tags <- reduced_dataset_tags |> 
  select(title, tags_combined) |> 
  distinct()

# Re-merge with the datasets frame
datasets <- datasets |> 
  left_join(reduced_dataset_tags, by = "title") |> 
  relocate(
    tags_combined, .before = "tags"
  )

# Remove the original tags column and rename so that the tags-related functions below continue as normal.
datasets <- datasets |> 
  select(! tags) |> 
  rename(
    tags = "tags_combined"
  )

# 11. Add a "atipp_registry" tag to open information entries that originated from the Access to Information registry
# information_type = "Access information registry"
# datasets |> count(tags) |> arrange(desc(n)) |> View()

datasets <- datasets |>
  mutate(
    tags = case_when(
      information_type == "Access information registry" & is.na(tags) ~ "atipp_registry",
      information_type == "Access information registry" ~ str_c("atipp_registry", ",", tags),
      .default = tags
    )
  )

# 12. Formatting for open information ATIPP columns
datasets <- datasets |> 
  mutate(
    required_atipp_compliance = case_when(
      required_atipp_compliance == 1 ~ "Yes",
      required_atipp_compliance == 0 ~ "No",
      .default = ""
    )
  )

# datasets |> count(publication_type)

datasets <- datasets |> 
  mutate(
    publication_type = case_when(
      publication_type == "Auditor final reports" ~ "auditor_final_reports",
      publication_type == "Departmental Manuals and Policy Statements" ~ "departmental_manuals_and_policy_statements",
      publication_type == "Final reports providing advice or recommendations to the public body" ~ "final_reports_providing_advice_or_recommendations_to_public_body",
      publication_type == "Information or a record available to the public without requiring an access request" ~ "information_or_record_available_to_public_without_access_request",
      publication_type == "Organizational responsibilities and functions" ~ "organizational_responsibilities_and_functions",
      publication_type == "Organizational structures" ~ "organizational_structures",
      .default = ""
    )
  )


# x. Check for recent updates that are newer than expected (via mystery DKAN cron job?)
# Include harvest information in the internal notes (for harvests that haven't been excluded above.)
datasets <- datasets |> 
  mutate(
    internal_notes = case_when(
      ! is.na(harvest_source) ~ str_c("Harvest source: ", harvest_source, ". ", "Harvest source modified: ", harvest_source_modified, ". "),
      .default = ""
    ),
    internal_notes = case_when(
      ! is.na(additional_info_items) ~ str_c(internal_notes, additional_info_items),
      .default = internal_notes
    )
  )

# Manual fix for Education enrolment data, which seems to have an incomplete harvest cron that keeps updating its metadata:
datasets <- datasets |> 
  mutate(
    last_revised = case_when(
      description == "Enrolment by school and grade for all Yukon schools" ~ authored,
      harvest_source == "Environment_54" ~ authored,
      harvest_source == "Environment_48" ~ authored,
      title == "Baseline Science Review" ~ authored,
      .default = last_revised
    )
  )

# x. Update the "Arts, music, literature" topic to remove commas
# to help with topics exports later
datasets <- datasets |> 
  mutate(
    topics = str_replace_all(topics, "Arts, music, literature", "Arts music and literature")
  )

topics <- datasets |> 
  select(title, topics) |> 
  separate_longer_delim(cols = topics, delim = ",") |> 
  mutate(
    topics = str_to_sentence(topics)
  ) |> 
  filter(! is.na(topics))

topics <- topics |> 
  group_by(topics) |> 
  add_count(topics)

# topics |>
#   select(topics, n) |>
#   distinct() |> 
#   arrange(desc(n)) |>
#   View()


topics_to_remove = c(
  "Access",
  "Compliance",
  "Manual",
  "Annual reports",
  "Atipp office",
  "Forms and templates",
  "Guidance"
)

reduced_topics <- topics |> 
  filter(! topics %in% topics_to_remove)

# Similar approach to tags above
reduced_topics <- reduced_topics |> 
  ungroup() |> 
  arrange(title, topics) |>
  group_by(title) |> 
  mutate(
    topics_combined = str_flatten(topics, collapse = ",")
  )

reduced_topics <- reduced_topics |> 
  select(title, topics_combined) |> 
  distinct()

# Re-merge with the datasets frame
datasets <- datasets |> 
  left_join(reduced_topics, by = "title") |> 
  relocate(
    topics_combined, .before = "topics"
  )

# Remove the original tags column and rename so that the tags-related functions below continue as normal.
datasets <- datasets |> 
  select(! topics) |> 
  rename(
    topics = "topics_combined"
  )

# x. Update homepage_url for YBS SEWP entries, as well as WGED
datasets <- datasets |> 
  mutate(
    homepage_url = case_when(
      homepage_url == "http://sewp.gov.yk.ca/home" ~ "https://community-statistics.service.yukon.ca/",
      homepage_url == "https://yukon.ca/en/womens-directorate" ~ "https://yukon.ca/en/women-gender-equity-directorate",
      homepage_url == "http://www.atipp.gov.yk.ca/ati-requests.html" ~ "https://yukon.ca/en/request-access-information-records",
      homepage_url == "https://hss.yukon.ca/medicaltravel.php" ~ "https://yukon.ca/en/medical-treatment-travel",
      homepage_url == "yukon.ca/libraries" ~ "https://yukon.ca/en/yukon-public-libraries",
      .default = homepage_url
    )
  )

# x. Reset authored or last_revised dates that are "1969-12-31"
# Note: there aren't any, at least after filtering out active harvest sources above.
# datasets |> count(authored) |> arrange(authored) |> View()
# datasets |> count(last_revised) |> arrange(last_revised) |> View()

# x. Detect and update HTML-based description fields
datasets <- datasets |> 
  mutate(
    is_html_description = case_when(
      str_detect(description, "<") ~ TRUE,
      .default = FALSE
    )
  ) |> relocate(
    is_html_description, .before = "description"
  )

html_descriptions <- datasets |> 
  filter(is_html_description == TRUE) |> 
  select(node_id, title, description)

# Optionally turn this off since it's quite slow:
if(setting_run_pandoc_markdown_conversions) {
  
  # Write HTML files
  for(i in seq_along(html_descriptions[[1]])) {
    
    # print(html_descriptions[[1]][i])
    # html_descriptions[[1]][i]
    
    # Thanks to https://stackoverflow.com/a/55225065/756641
    html_descriptions[[3]][i] |> 
      write_lines( str_c("html_conversion/html/", html_descriptions[[1]][i], ".html"))
    
  }
  
  # Convert them with Pandoc
  for(i in seq_along(html_descriptions[[1]])) {
    
    # Thanks to https://stackoverflow.com/a/78140357
    # also thanks to https://www.reddit.com/r/pandoc/comments/wa8pun/comment/ii061ce/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button
    pandoc_convert(
      input = path_wd(str_c("html_conversion/html/", html_descriptions[[1]][i], ".html")),
      to = "commonmark",
      options = c("--wrap=preserve"),
      output = path_wd(str_c("html_conversion/md/", html_descriptions[[1]][i], ".md")),
    )
    
  }
  
  # Bring back in the Markdown descriptions
  html_descriptions <- html_descriptions |> 
    mutate(
      description_updated = NA_character_
    )
  
  for(i in seq_along(html_descriptions[[1]])) {
    
    html_descriptions[[4]][i] = read_file(str_c("html_conversion/md/", html_descriptions[[1]][i], ".md"))
    
  }
  
  # Re-join the updated descriptions back to the original datasets table
  html_descriptions <- html_descriptions |> 
    select(node_id, description_updated)
  
  datasets <- datasets |> 
    left_join(html_descriptions, by = "node_id") |> 
    relocate(
      description_updated, .before = "description"
    )
  
  # SC04. Special case for Geomatics endnotes that aren't formatted properly.
  datasets <- datasets |> 
    mutate(
      description_updated = str_replace_all(description_updated, "<a href=\"https://yukon.ca/geoyukon\" rel=\"nofollow ugc\" style=\"text-decoration:underline;\">GeoYukon</a> by the <a href=\"https://yukon.ca/maps\" rel=\"nofollow ugc\" style=\"text-decoration:underline;\">Government of Yukon</a>", "[GeoYukon](https://yukon.ca/geoyukon) by the [Government of Yukon](https://yukon.ca/maps)"),
      description_updated = str_replace_all(description_updated, "<a href=\"https://yukon.ca/geoyukon\" rel=\"nofollow ugc\">GeoYukon</a> by the <a href=\"https://yukon.ca/maps\" rel=\"nofollow ugc\">Government of Yukon</a>", "[GeoYukon](https://yukon.ca/geoyukon) by the [Government of Yukon](https://yukon.ca/maps)"),
      
      description_updated = str_replace_all(description_updated, "<a href=\"mailto:geomatics.help@yukon.ca\" rel=\"nofollow ugc\" style=\"text-decoration:underline;\">geomatics.help@yukon.ca</a>", "[geomatics.help@yukon.ca](mailto:geomatics.help@yukon.ca)"),
      description_updated = str_replace_all(description_updated, "<a href=\"mailto:geomatics.help@yukon.ca\" rel=\"nofollow ugc\">geomatics.help@yukon.ca</a>", "[geomatics.help@yukon.ca](mailto:geomatics.help@yukon.ca)")
      
    )
  
  # Re-combine the description column
  datasets <- datasets |> 
    mutate(
      description = case_when(
        !is.na(description_updated) ~ description_updated,
        .default = description
      )
    )
  
}



# x. Rename columns to match the field names in the CKAN schema

# Used by rename(), where new = "old"
field_mapping <- c(
  notes = "description",
  organization_title = "publishers_groups",
  internal_contact_name = "contact_name",
  internal_contact_email = "contact_email",
  metadata_created = "authored",
  metadata_modified = "last_revised",
  temporal_coverage_start_date = "temporal_coverage_from",
  temporal_coverage_end_date = "temporal_coverage_to",
  spatial_coverage_locations = "geographical_coverage_location",
  publication_required_under_atipp_act = "required_atipp_compliance",
  publication_type_under_atipp_act = "publication_type",
  dkan_uri = "uri",
  dkan_node_id = "node_id"
  
)

datasets <- datasets |> 
  rename(all_of(field_mapping))

# Select actually-used columns for export
# Do this in 2 parts: data, and information
# since these both have slightly different schemas.

data_export <- datasets |> 
  filter(
    schema_type == "data"
  ) |> 
  select(
    schema_type,
    title,
    notes,
    topics,
    tags,
    internal_contact_name,
    internal_contact_email,
    organization_title,
    custodian,
    homepage_url,
    internal_notes,
    language,
    license_id,
    update_frequency,
    metadata_created,
    metadata_modified,
    spatial_coverage_locations,
    temporal_coverage_start_date,
    temporal_coverage_end_date,
    dkan_uri,
    dkan_node_id
  )

information_export <- datasets |> 
  filter(
    schema_type == "information"
  ) |> 
  select(
    schema_type,
    title,
    notes,
    topics,
    tags,
    internal_contact_name,
    internal_contact_email,
    organization_title,
    custodian,
    homepage_url,
    internal_notes,
    language,
    license_id,
    publication_required_under_atipp_act,
    publication_type_under_atipp_act,
    metadata_created,
    metadata_modified,
    dkan_uri,
    dkan_node_id
  )

# Write export to CSV
write_out_csv(data_export, "output/data")
write_out_csv(information_export, "output/information")

# Completed ATIPP Requests processing -------------------------------------

xlsx_atipp_requests_nodes <- read_excel(source_atipp_requests_file)

access_requests <- xlsx_atipp_requests_nodes

access_requests <- access_requests |> 
  filter_is_published() |> 
  mutate_languages() |> 
  mutate(
    licence = "ogly"
  ) |> 
  mutate_license_id()

access_requests <- access_requests |> 
  mutate(
    schema_type = "access-requests"
  )

access_requests <- access_requests |> 
  mutate(
    organization_title = case_when(
      is.na(public_body_name) ~ publishers_groups,
      .default = public_body_name
    )
  ) |> 
  relocate(
    organization_title, .before = "public_body_name"
  )

# access_requests |> count(organization_title) |> arrange(desc(n)) |> View()

# Manual organization name cleanup
access_requests <- access_requests |> 
  mutate(
    organization_title = case_when(
      organization_title == "Women’s Directorate" ~ "Women and Gender Equity Directorate",
      organization_title == "Workers’ Compensation Health and Safety Board, Workers’ Compensation Act" ~ "Yukon Workers’ Compensation Health and Safety Board",
      organization_title == "Workers’ Compensation, Health and Safety Board" ~ "Yukon Workers’ Compensation Health and Safety Board",
      organization_title == "Yukon Hospital Corporation Board of Trustees, Hospital Act" ~ "Yukon Hospital Corporation",
      organization_title == "Yukon Liquor Corporation Board of Directors" ~ "Yukon Liquor Corporation",
      organization_title == "ATIPP Office" ~ "Public Service Commission",
      .default = organization_title
      
    )
  )

access_requests <- access_requests |> 
  mutate(
    date_of_request = str_datetime_to_time(date_of_request),
    file_id = str_c(str_sub(file_number_year, 3L, 4L), "-", file_number_sequence)
  )

# Update response_type format
# not_specified granted_in_full granted_in_part withheld_in_full no_records_found excluded_information
access_requests <- access_requests |> 
  mutate(
    response_type = str_replace_all(str_to_lower(response_type), " ", "_")
  )

# Update is_fees format
access_requests <- access_requests |> 
  mutate(
    is_fees = case_when(
      is_fees == 1 ~ "Yes",
      is_fees == 0 ~ "No",
      .default = ""
    )
  )


# Used by rename(), where new = "old"
field_mapping <- c(
  notes = "description",
  fees = "is_fees",
  metadata_created = "authored",
  metadata_modified = "last_revised",
  dkan_uri = "uri",
  dkan_node_id = "node_id"
  
)

access_requests <- access_requests |> 
  rename(all_of(field_mapping))

# Select actually-used columns for export
access_requests_export <- access_requests |> 
  select(
    schema_type,
    title,
    file_id,
    notes,
    organization_title,
    date_of_request,
    response_type,
    fees,
    language,
    license_id,
    metadata_created,
    metadata_modified,
    dkan_uri,
    dkan_node_id
  )

# Write export to CSV
write_out_csv(access_requests_export, "output/access_requests")

# PIA Summaries processing ------------------------------------------------

xlsx_pia_summaries_nodes <- read_excel(source_pia_summaries_file)

pia_summaries <- xlsx_pia_summaries_nodes

pia_summaries <- pia_summaries |> 
  filter_is_published() |> 
  mutate_languages() |> 
  mutate(
    licence = "ogly"
  ) |> 
  mutate_license_id()

pia_summaries <- pia_summaries |> 
  mutate(
    schema_type = "pia-summaries"
  ) |> 
  mutate(
    date_of_approval = str_datetime_to_time(date_of_approval),
  )

# Filter out the one redundant entry (missing a public_body_name, and also a duplicate of a later entry)
pia_summaries <- pia_summaries |> 
  filter(! is.na(public_body_name))

# pia_summaries |> count(public_body_name) |> arrange(desc(n)) |> View()

# Used by rename(), where new = "old"
field_mapping <- c(
  notes = "description",
  privacy_impact_assessment_number = "pia_number",
  organization_title = "public_body_name",
  metadata_created = "authored",
  metadata_modified = "last_revised",
  dkan_uri = "uri",
  dkan_node_id = "node_id"
  
)

pia_summaries <- pia_summaries |> 
  rename(all_of(field_mapping))

# Select actually-used columns for export

pia_summaries_export <- pia_summaries |> 
  select(
    schema_type,
    title,
    notes,
    organization_title,
    language,
    license_id,
    privacy_impact_assessment_number,
    date_of_approval,
    metadata_created,
    metadata_modified,
    dkan_uri,
    dkan_node_id
  )

# Write export to CSV
write_out_csv(pia_summaries_export, "output/pia_summaries")

# Dataset resources processing --------------------------------------------

# There are a bunch of initial resources without dataset_node_id entries,
# so we'll manually set column types here.
dkan_files_base_url <- "https://open.yukon.ca/sites/default/files/"

xlsx_dataset_resources_nodes <- read_excel(
  source_dataset_resources_file,
  col_types = c(
    "numeric", # node_id 
    "numeric", # dataset_node_id 
    "text", # content_type 
    "text", # title   
    "text", # description  
    "text", # uri    
    "text", # publishers_groups  
    "numeric", # local_file_id 
    "text", # local_file_name   
    "text", # local_file_path  
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "guess",
    "text", # category
    "guess",
    "guess",
    "text", # last_radioactivity_emission
    "numeric", # resource_hits
    "guess",
    "guess",
    "guess",
    "text", # eliminated_node_comment
    "guess",
    "guess",
    "guess",
    "guess")
  )

dataset_resources <- xlsx_dataset_resources_nodes

dataset_resources <- dataset_resources |> 
  filter_is_published() |> 
  mutate_languages()

# 1. Filter to just resources that have a current dataset_node_id
# (which should eliminate resources attached to deleted entries)
dataset_resources <- dataset_resources |> 
  filter(! is.na(dataset_node_id))

# dataset_resources |> count(dataset_node_id) |> View()

# SC01. Special case for YBS historical resources, which we want to re-assign to single datasets for e.g. Census 2001, Census 2006, etc.

# Census 2001 = 61168
# Census 2006 = 61170
# National Household Survey 2011 = 61172
# Census 2011 = 61174
# Census 2016 = 61176 (Community Statistics website)
# Census 2021 = 61178 (Community Statistics website)
dataset_resources <- dataset_resources |> 
  mutate(
    dataset_node_id = case_when(
      str_detect(title, "Census 2001") ~ 61168,
      str_detect(title, "Census 2006") ~ 61170,
      str_detect(title, "National Household Survey 2011") ~ 61172,
      str_detect(title, "Census 2011") ~ 61174,
      .default = dataset_node_id
    )
  )


# 2. Filter again to just resources whose DKAN node IDs are included above (!)
# to exclude resources from active-harvest sources that we'll bring back in later.
# (Also simultaneously add missing publishers_groups via the dataset DKAN node IDs of the parent dataset)

dataset_resource_parents <- datasets |> 
  select(schema_type, dkan_node_id, title, organization_title) |> 
  rename(
    dataset_node_id = "dkan_node_id",
    dataset_title = "title"
  )

dataset_resources <- dataset_resources |> 
  left_join(dataset_resource_parents, by = "dataset_node_id")

dataset_resources <- dataset_resources |> 
  filter(! is.na(schema_type))

# 3. Filter out files that are local but that have a file size of 0 (upload errors, it appears).
dataset_resources <- dataset_resources |> 
  filter(! (! is.na(local_file_name) & local_file_bytes == 0))

# 4. Differentiate uploads from remote files/URLs.
dataset_resources <- dataset_resources |> 
  mutate(
    url_type = case_when(
      ! is.na(local_file_name) ~ "upload",
      .default = NA_character_
    )
  ) |> 
  relocate(
    url_type, .before = "local_file_name"
  )

# 5. Create a public URL from the local_file_name
# https://open.yukon.ca/sites/default/files/ + local_file_name
dataset_resources <- dataset_resources |> 
  mutate(
    url = case_when(
      ! is.na(local_file_name) ~ str_c(dkan_files_base_url, local_file_name),
      ! is.na(remote_uri) ~ remote_uri,
      ! is.na(linked_file_uri) ~ linked_file_uri,
      .default = NA_character_
    )
  ) |> 
  relocate(
    url, .before = "local_file_name"
  )

# 6. Set format based on existing file names (including missing entries)
# Update file types to match filenames or use HTML for URLs (rather than "data")
# TODO - confirm if we need to also set mimetype for CKAN import (e.g. "text/csv" or "application/pdf", etc.)
dataset_resources <- dataset_resources |> 
  mutate(
    format_raw = case_when(
      ! is.na(local_file_name) ~ path_ext(local_file_name),
      ! is.na(remote_uri) ~ path_ext(remote_uri),
      ! is.na(linked_file_uri) ~ path_ext(linked_file_uri),
      .default = "html" # If not set, likely a webpage with a URL that ends without a file extension.
    )
  ) |> 
  mutate(
    format_raw = str_to_upper(format_raw) # Don't forget to capitalize any comparisons from here on in.
  ) |> 
  relocate(
    format_raw, .before = "local_file_name"
  )


# dataset_resources |> count(format_raw) |> arrange(desc(n)) |> View()
# dataset_resources |> count(format) |> arrange(desc(n)) |> View()


# SC03. Import GeoYukon resources and associate them together.
geoyukon_resources <- read_csv(source_geoyukon_resources_file) |> 
  clean_names()

geoyukon_resources <- geoyukon_resources |> 
  filter(title != "{{name}}")

geoyukon_resources <- geoyukon_resources |> 
  mutate(
    dataset_id = dataset_id + geoyukon_node_starting_id,
    # schema_type = "data",
    format_raw = str_to_upper(format)
  )

geoyukon_resources <- geoyukon_resources |> 
  rename(
    url = "link",
    dataset_node_id = dataset_id
  )

geoyukon_dataset_resource_parents <- geoyukon_datasets |> 
  select(schema_type, node_id, title, publishers_groups, last_revised, authored) |> 
  rename(
    dataset_node_id = "node_id",
    dataset_title = "title",
    organization_title = "publishers_groups"
  )

geoyukon_resources <- geoyukon_resources |> 
  left_join(geoyukon_dataset_resource_parents, by = "dataset_node_id")

# Update resource titles to be more descriptive
geoyukon_resources <- geoyukon_resources |> 
  mutate(
    title = case_when(
      format == "html" ~ "ArcGIS Online layers",
      format == "ftp" ~ "Shape files",
      format == "xml" ~ "XML metadata"
    )
  )

# Fix for &amp; URL errors from ArcGIS Online
geoyukon_resources <- geoyukon_resources |> 
  mutate(
    url = str_replace_all(url, "&amp;", "&")
  )

# Bind these together
dataset_resources <- dataset_resources |> 
  bind_rows(geoyukon_resources)


# After merging other imports, clean up file formats:
# Based on the current resources export
valid_format_types = c(
  "PDF",
  "HTML",
  "CSV",
  "ZIP",
  "DOCX",
  "XLSX",
  "XLS",
  "DOC",
  "KMZ",
  "TXT"
)

dataset_resources <- dataset_resources |> 
  mutate(
    format = case_when(
      format_raw %in% valid_format_types ~ format_raw,
      is.na(format_raw) ~ "HTML",
      .default = "HTML"
    )
  ) |> 
  relocate(
    format, .before = "format_raw"
  )



# 8. Split into data and information resource exports separately
# Used by rename(), where new = "old"
field_mapping <- c(
  # notes = "description",
  name = "title",
  size = "local_file_bytes", # TODO - if this is from remote files, it's also available.
  created = "authored",
  last_modified = "last_revised",
  dkan_uri = "uri",
  dkan_resource_node_id = "node_id",
  dkan_parent_dataset_node_id = "dataset_node_id",
  dkan_parent_dataset_title = "dataset_title"
  
)

dataset_resources <- dataset_resources |> 
  rename(all_of(field_mapping))

data_resources_export <- dataset_resources |> 
  filter(
    schema_type == "data"
  ) |> 
  select(
    schema_type,
    name,
    description,
    format,
    created,
    last_modified,
    size,
    url,
    url_type,
    dkan_parent_dataset_node_id,
    dkan_parent_dataset_title,
    dkan_resource_node_id,
    dkan_uri
  )

information_resources_export <- dataset_resources |> 
  filter(
    schema_type == "information"
  ) |> 
  select(
    schema_type,
    name,
    description,
    format,
    created,
    last_modified,
    size,
    url,
    url_type,
    dkan_parent_dataset_node_id,
    dkan_parent_dataset_title,
    dkan_resource_node_id,
    dkan_uri
  )

# 9. Write export to CSV
write_out_csv(data_resources_export, "output/resources_data")
write_out_csv(information_resources_export, "output/resources_information")



# Completed ATIPP Requests resources processing ---------------------------

xlsx_atipp_requests_resources_nodes <- read_excel(source_atipp_requests_resources_file)

atipp_requests_resources <- xlsx_atipp_requests_resources_nodes

# atipp_requests_resources doesn't include is_published nor languages columns
# atipp_requests_resources <- atipp_requests_resources |> 
#   filter_is_published() |> 
#   mutate_languages()

# For this data frame, per Dave:
# "the node_id column is the foreign key of the parent request’s node ID"

atipp_requests_resource_parents <- access_requests |> 
  select(schema_type, dkan_node_id, dkan_uri, title, organization_title) |> 
  rename(
    node_id = "dkan_node_id",
    access_request_title = "title",
    dkan_access_request_uri = "dkan_uri",
  )

atipp_requests_resources <- atipp_requests_resources |> 
  left_join(atipp_requests_resource_parents, by = "node_id")

atipp_requests_resources <- atipp_requests_resources |> 
  filter(! is.na(schema_type))


atipp_requests_resources <- atipp_requests_resources |> 
  mutate(
    url_type = case_when(
      ! is.na(response_file_name) ~ "upload",
      .default = NA_character_
    )
  ) |> 
  relocate(
    url_type, .before = "response_file_name"
  )

atipp_requests_resources <- atipp_requests_resources |> 
  mutate(
    url = str_c(dkan_files_base_url, response_file_name)
  ) |> 
  relocate(
    url, .before = "response_file_name"
  )


atipp_requests_resources <- atipp_requests_resources |> 
  mutate(
    format = path_ext(response_file_name)
  ) |> 
  relocate(
    format, .before = "response_file_name"
  )

# ATIPP request resources don't have a file title, so we'll just re-use the file name minus the extension
atipp_requests_resources <- atipp_requests_resources |> 
  mutate(
    title = path_ext_remove(response_file_name),
    authored = response_file_timestamp,
    last_revised = response_file_timestamp,
    description = ""
  )

# Used by rename(), where new = "old"
field_mapping <- c(
  name = "title",
  size = "response_file_bytes",
  created = "authored",
  last_modified = "last_revised",
  dkan_parent_dataset_uri = "dkan_access_request_uri",
  dkan_parent_dataset_node_id = "node_id",
  dkan_parent_dataset_title = "access_request_title"
  
)

atipp_requests_resources <- atipp_requests_resources |> 
  rename(all_of(field_mapping))

atipp_requests_resources_export <- atipp_requests_resources |> 
  select(
    schema_type,
    name,
    description,
    format,
    created,
    last_modified,
    size,
    url,
    url_type,
    dkan_parent_dataset_node_id,
    dkan_parent_dataset_title,
    dkan_parent_dataset_uri
    # dkan_resource_node_id,
    # dkan_uri
  )

write_out_csv(atipp_requests_resources_export, "output/resources_access_requests")


# Export all organization_title entries, and all topics -------------------

# Gather organization_title entries from all 4 schema types.

# Covers both open data and open information
organizations_datasets <- datasets |> 
  select(organization_title) |> 
  distinct()

organizations_access_requests <- access_requests |> 
  select(organization_title) |> 
  distinct()

organizations_pia_summaries <- pia_summaries |> 
  select(organization_title) |> 
  distinct()

organizations <- organizations_datasets |> 
  bind_rows(organizations_access_requests) |> 
  bind_rows(organizations_pia_summaries) |> 
  distinct() |> 
  arrange(organization_title)

write_out_csv(organizations, "output/organizations")

# Get topics from datasets (not present for PIA summaries or access requests)

topics <- datasets |> 
  select(topics) |> 
  separate_longer_delim(cols = topics, delim = ",") |> 
  arrange(topics) |> 
  distinct() |>
  filter(! is.na(topics))

write_out_csv(topics, "output/topics")



# End time logging --------------------------------------------------------

run_end_time <- now()
paste("Start time was:", run_start_time)
paste("End time was:", run_end_time)

paste("Run duration:", round(time_length(interval(run_start_time, run_end_time), "seconds"), digits = 2), "seconds")

