################################################################################
# INSTRUCTIONS: This R script defines a Strategus-based Kaplan-Meier survival
#               analysis study using the CohortSurvival module.
################################################################################

library(dplyr)
library(Strategus)

# Study time window
studyStartDate <- "19000101" # YYYYMMDD
studyEndDate <- "20231231" # YYYYMMDD

# Load cohort definitions
cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  settingsFileName = "./gibleed_cohorts/Cohorts.csv",
  jsonFolder = "./gibleed_cohorts/cohorts",
  sqlFolder = "./gibleed_cohorts/sql/sql_server"
)
# print(cohortDefinitionSet)

# Target and outcome cohorts
targetCohortTable <- "sample_study"
targetCohortId <- 1
outcomeCohortTable <- "sample_study"
outcomeCohortId <- 3

# Stratification variables (if any)
# strata <- list(
#   #gender = c("gender")
#   age = c("gender")
# )
strata <- list("gender")
# strata <- NULL


# CohortSurvivalModule ---------------------------------------------------------
csModuleSettingsCreator <- CohortSurvivalModule$new()

cohortSurvivalModuleSpecifications <- csModuleSettingsCreator$createModuleSpecifications(
  targetCohortId = targetCohortId,
  outcomeCohortId = outcomeCohortId,
  strata = strata,
  analysisType = "single_event",
  competingOutcomeCohortTable = NULL
)

print(cohortSurvivalModuleSpecifications)

# Cohort Generator -------------------------------------------------------------
cgModuleSettingsCreator <- CohortGeneratorModule$new()
cohortDefinitionShared <- cgModuleSettingsCreator$createCohortSharedResourceSpecifications(cohortDefinitionSet)
cohortGeneratorModuleSpecifications <- cgModuleSettingsCreator$createModuleSpecifications()

# Create the analysis specifications -------------------------------------------
analysisSpecifications <- Strategus::createEmptyAnalysisSpecificiations() |>
  Strategus::addSharedResources(cohortDefinitionShared) |>
  Strategus::addModuleSpecifications(cohortGeneratorModuleSpecifications) |>
  Strategus::addModuleSpecifications(cohortSurvivalModuleSpecifications)

# Save the analysis specifications to a JSON file
path <- file.path("analysis_spec_cs.json")
ParallelLogger::saveSettingsToJson(
  analysisSpecifications,
  path
)
cat("Analysis specifications saved to", path, "\n")
