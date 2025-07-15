library(tidyverse)
library(fs)
library(readxl)
library(rmarkdown)
library(janitor)
library(pdftools)

# Test PDF
input_pdf <- pdf_text("input/20250715/20200100-Release_2.pdf")

head(input_pdf)

# input_pdf_split <- strsplit(input_pdf, "\n")

merged_pdf <- str_flatten(input_pdf)

# split_by_prefix <- str_split(merged_pdf, pattern = "\nA-")


# Drop first page heading
merged_pdf_no_header <- str_split(merged_pdf, pattern = "\n\n\n Request", n = 2)[[1]][2]

merged_pdf_no_header <- str_c(" Request", merged_pdf_no_header)

# Drop table column headings
merged_pdf_no_header <- str_split(merged_pdf_no_header, pattern = " Paid\n", n = 2)[[1]][2]



requests <- as_tibble(merged_pdf_no_header, .name_repair = "universal")

requests <- requests |> 
  separate_longer_delim(
    cols = value,
    delim = "A-"
  )


requests <- requests |> 
  mutate(
    value = str_replace_all(value, pattern = "Health       and", replacement = "Health and        "),
    #
    value = str_replace_all(value, pattern = "Highways and ", replacement = "Highways and    ")
  )

requests <- requests |>
  separate_wider_position(
    cols = value,
    widths = c(
      request_number_l1 = 7,
      public_body_l1 = 14,
      response_l1 = 15,
      fees_paid_l1 = 5,
      request_summary_l1 = 51,
      request_number_l2 = 7,
      public_body_l2 = 16,
      response_l2 = 10,
      fees_paid_l2 = 5,
      request_summary_l2 = 40
      
      ),
    too_many = "debug",
    too_few = "align_start"
    )

View(requests)

