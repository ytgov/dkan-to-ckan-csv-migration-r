library(tidyverse)
library(fs)
library(readxl)

# Source data file to import
source_dataset_file <- "input/20250515/YG_Open_Gov_DKAN_dataset_export.xlsx"

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


# x. Update languages to match the new possible values

# x. Update license to use "OGL-Yukon-2.0" reference

# x. Update update frequency and confirm valid input

# x. Update temporal coverage to exclude "Wednesday, December 31, 1969 - 16:00" entries

# x. Update temporal coverage to be by year rather than by date

# x. Exclude active Harvest sources

# x. Set canonical publisher organizations
# (remove multiple entries; consolidate across related DKAN fields)

# x. Add a "atipp_registry" tag to open information entries that originated from the Access to Information registry


# x. Rename columns to match the field names in the CKAN schema

# Used by rename(), where new = "old"
field_mapping <- c(
  notes = "description",
  internal_contact_name = "contact_name",
  internal_contact_email = "contact_email",
  metadata_created = "authored",
  metadata_modified = "last_revised",
  update_frequency = "frequency",
  temporal_coverage_start_date = "temporal_coverage_from",
  temporal_coverage_end_date = "temporal_coverage_to",
  spatial_coverage_locations = "geographical_coverage_location",
  dkan_url_path = "uri"
  
)

datasets <- datasets |> 
  rename(all_of(field_mapping))

# Check specific fields:
# datasets[22] |> filter(! is.na(data_dictionary)) |> distinct()

# data_standard_url


# datasets[40] |> filter(! is.na(data_standard_url)) |> distinct()

# Select actually-used columns for export

datasets_export <- datasets |> 
  select(
    schema_type,
    title,
    notes,
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
