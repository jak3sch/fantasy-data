library(nflreadr)
library(tidyverse)
library(dplyr)

roster <- nflreadr::load_rosters() %>%
  dplyr::mutate(
    depth_chart_position = dplyr::case_when(
      depth_chart_position == "K" ~ "PK",
      TRUE ~ depth_chart_position
    ),
    sync_helper = paste0(rookie_year, depth_chart_position, gsub(" ", "", full_name))
  ) %>%
  dplyr::select(gsis_id, sync_helper)

ff_playerids <- nflreadr::load_ff_playerids() %>%
  #dplyr::filter(draft_year >= 2022) %>% # Get all rookies from 2022 and 2023 with MFL_ids
  dplyr::filter(is.na(gsis_id) | is.na(pfr_id) & draft_year > 2015) %>%
  dplyr::mutate(
    name = nflreadr::clean_player_names(name),
    position = ifelse(position == "S", "DB", position),
    sync_helper = paste0(draft_year, position, gsub(" ", "", name)),
  ) %>%
  # merge missing gsis_ids with exisitng ones fromload_rosters()
  dplyr::left_join(
    roster %>%
      dplyr::rename(id = gsis_id),
    by = "sync_helper"
  ) %>%
  dplyr::mutate(gsis_id = ifelse(is.na(gsis_id), id, gsis_id)) %>%
  dplyr::select(-id)

#Get pfr_ids out of pfr dataset
pfr_ids <- nflreadr::load_snap_counts(seasons = 2015:2023) %>%
  dplyr::mutate(player = nflreadr::clean_player_names(player)) %>%
  dplyr::group_by(pfr_player_id, position) %>%
  dplyr::arrange(season, week) %>%
  dplyr::filter(dplyr::row_number() == 1) %>%
  dplyr::filter(
    # filter some ids manually
    !pfr_player_id %in% c(
      "BrowMi0a", # executive
      "JohnBr20", # LB drafted in 2006
      "AlexMa00" # DB drafted in 2014
    )
  ) %>%
  dplyr::mutate(
    position = ifelse(position %in% c("FS", "SS"), "DB", position), #ff_playerids() only knows DB as position
    sync_helper = paste0(season, position, gsub(" ", "", player)),
    sync_helper = dplyr::case_when(
      sync_helper == "2023WRJustynRoss" ~ "2022WRJustynRoss", # ross didnt played in his rookie season, so the sync helper is not correct
      sync_helper == "2022LBNathanLandman" ~ "2022LBNateLandman",
      TRUE ~ sync_helper
    )
  ) %>%
  dplyr::ungroup() %>%
  dplyr::select(player, pfr_player_id, position, sync_helper) %>%
  dplyr::distinct()

# get gsis-ids from weekly rosters
gsis_to_sync <- nflreadr::load_rosters_weekly(2015:2023) %>%
  dplyr::filter(rookie_year >= 2015) %>%
  dplyr::mutate(
    player = nflreadr::clean_player_names(full_name),
    depth_chart_position = ifelse(position %in% c("LB"), position, depth_chart_position)
  ) %>%
  dplyr::group_by(gsis_id, depth_chart_position) %>%
  dplyr::arrange(season, week) %>%
  dplyr::filter(dplyr::row_number() == 1) %>%
  dplyr::mutate(sync_helper = paste0(season, depth_chart_position, gsub(" ", "", player))) %>%
  dplyr::ungroup() %>%
  dplyr::select(player, gsis_id, espn_id, yahoo_id, pff_id, sleeper_id, sync_helper) %>%
  dplyr::filter(!is.na(gsis_id))

# combine gsis with pfr ids from rookies
gsis_to_pfr_ids <- gsis_to_sync %>%
  dplyr::left_join(
    pfr_ids %>%
      dplyr::select(pfr_player_id, sync_helper),
    by = "sync_helper",
    relationship = "many-to-many"
  ) %>%
  dplyr::filter(!is.na(pfr_player_id)) %>%
  #dplyr::select(gsis_id,pfr_player_id) %>%
  dplyr::rename(pfr_id = pfr_player_id)  %>%
  dplyr::distinct() %>%

  # add mfl ids to join later datasets withou gsis id
  dplyr::left_join(
    ff_playerids %>%
      dplyr::select(mfl_id, sync_helper),
    by = "sync_helper"
  )

# check for duplicates
test <- gsis_to_pfr_ids %>%
  dplyr::group_by(sync_helper) %>%
  dplyr::summarise(count = dplyr::n())

# helper function to set values
f_set_values <- function(df) {
  df %>%
    dplyr::mutate(
      gsis_id = ifelse(!is.na(gsis_id), gsis_id, gsis_id_new),
      pfr_id = ifelse(!is.na(pfr_id), pfr_id, pfr_id_new),
      sleeper_id = ifelse(!is.na(sleeper_id), sleeper_id, sleeper_id_new),
      pff_id = ifelse(!is.na(pff_id), pff_id, pff_id_new),
      espn_id = ifelse(!is.na(espn_id), espn_id, espn_id_new),
      yahoo_id = ifelse(!is.na(yahoo_id), yahoo_id, yahoo_id_new)
    ) %>%
    dplyr::select(-dplyr::ends_with("_new"))
}

# join all nflverse ids with new pfr_id
joined_nflverse_ids <- ff_playerids %>%
  dplyr::select(mfl_id, sleeper_id, pfr_id, gsis_id, pff_id, espn_id, yahoo_id, name) %>%
  dplyr::left_join(
    gsis_to_pfr_ids %>%
      dplyr::rename_at(vars(-mfl_id),function(x) paste0(x,"_new")),
    by = "mfl_id"
  ) %>%
  f_set_values()

# load existing data file
existing_file <- readr::read_csv("missing-ids/files/missing_ids.csv", col_types = "cccccccccc") %>%
  dplyr::select(-1)

new_file <- existing_file %>%
  # add existing ids from load_ff_playerids()
  dplyr::left_join(
    joined_nflverse_ids %>%
      dplyr::rename_at(vars(-mfl_id),function(x) paste0(x,"_new")),
    by = "mfl_id"
  ) %>%
  f_set_values()

new_players <- joined_nflverse_ids %>%
  dplyr::filter(!mfl_id %in% existing_file$mfl_id) %>%
  dplyr::filter(!is.na(gsis_id)) %>%
  dplyr::mutate(
    ras_id = NA,
    otc_id = NA
  ) %>%
  dplyr::select(-name)

write.csv(rbind(new_file, new_players), "missing-ids/files/missing_ids.csv", row.names = TRUE)
