library(tidyverse)
library(fs)
library(readxl)


# Source files ------------------------------------------------------------

# Source data file to import
source_dataset_file <- "input/20250515/YG_Open_Gov_DKAN_dataset_export.xlsx"


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
  filter(is_published == 1)


# 3. Update languages to match the new possible values
# english / french / multiple_languages / other / ""
# TODO - review if we can use ISO language codes while also supporting Yukon First Nations languages
# datasets |> select(languages) |> distinct()

datasets <- datasets |>
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

# datasets |> count(languages)
# datasets |> count(language)
# datasets |> filter(languages != "und") |> View()

# 4. Update license to use "OGL-Yukon-2.0" reference
# datasets |> filter(licence != "ogly") |> View()

datasets <- datasets |> mutate(
  license_id	 = "OGL-Yukon-2.0"
) |> 
  relocate(
    license_id, .before = "licence"
  )


# 5. Update update frequency and confirm valid input
# none / ad_hoc / annual / annual / semiannual / quarterly / monthly / weekly / daily / hourly / real_time
# datasets |> count(how_often_updated) 
datasets |> count(frequency)
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
datasets |> select(temporal_coverage_from) |> arrange(temporal_coverage_from) |> distinct()
datasets |> select(temporal_coverage_to) |> arrange(temporal_coverage_to) |> distinct()

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
  "Geomatics Metadata"
  
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
  separate_longer_delim(tags, ",")

# Filter out tags with strange punctuation
dataset_tags <- dataset_tags |> 
  mutate(
    tags = str_to_lower(str_replace(tags, " no. ", " "))
  ) |> 
  filter(! str_detect(tags, "[:.()]"))
  
# Testing regex here:
# dataset_tags |> 
#   filter(str_detect(tags, "[:.()]")) |> 
#   View()
  
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


# x. Check for recent updates that are newer than expected (via mystery DKAN cron job?)
# TODO - come back to this
# datasets |> 
#   filter(title == "Registered Trapping Concessions") |> 
#   View()


# x. Reset authored or last_revised dates that are "1969-12-31"
# Note: there aren't any, at least after filtering out active harvest sources above.
# datasets |> count(authored) |> arrange(authored) |> View()
# datasets |> count(last_revised) |> arrange(last_revised) |> View()

# x. Rename columns to match the field names in the CKAN schema

# Used by rename(), where new = "old"
field_mapping <- c(
  notes = "description",
  internal_contact_name = "contact_name",
  internal_contact_email = "contact_email",
  metadata_created = "authored",
  metadata_modified = "last_revised",
  temporal_coverage_start_date = "temporal_coverage_from",
  temporal_coverage_end_date = "temporal_coverage_to",
  spatial_coverage_locations = "geographical_coverage_location",
  dkan_url_path = "uri"
  
)

datasets <- datasets |> 
  rename(all_of(field_mapping))

# Select actually-used columns for export

datasets_export <- datasets |> 
  select(
    schema_type,
    title,
    notes,
    tags,
    license_id,
    metadata_created,
    metadata_modified,
    update_frequency,
    custodian,
    internal_contact_name,
    internal_contact_email,
    homepage_url,
    spatial_coverage_locations,
    temporal_coverage_start_date,
    temporal_coverage_end_date,
    dkan_url_path
  )


# Completed ATIPP Requests processing -------------------------------------



# PIA Summaries processing ------------------------------------------------



# Dataset resources processing --------------------------------------------

# x. Update file types to match filenames or use HTML for URLs (rather than "data")


# Completed ATIPP Requests resources processing ---------------------------


