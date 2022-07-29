#!/usr/bin/env Rscript

library(RiskStratifiedEstimation)
library(tidyverse)
source("~/git/RiskStratifiedEstimation/R/HelperFunctions.R")

args <- commandArgs(trailingOnly = TRUE)

database <- as.character(args)

prefix <- paste(
  "age_50",
  "tr_1",
  sep = "_"
)

negativeControls <- readr::read_csv(
  "negativeControls.csv",
  col_types = "dc"
) %>%
  dplyr::pull(outcomeId)

mapTreatments <- readr::read_csv(
  "mapTreatments.csv",
  col_types = "dc"
)

mapOutcomes   <- readr::read_csv(
  "mapOutcomes.csv",
  col_types = "dc"
)

databaseSettings <- createDatabaseSettings(
	cdmDatabaseSchema      = "cdm",
	cohortDatabaseSchema   = "Scratch.dbo",
	outcomeDatabaseSchema  = "Scratch.dbo",
	resultsDatabaseSchema  = "Scratch.dbo",
	exposureDatabaseSchema = "Scratch.dbo",
	cohortTable            = "table",
	outcomeTable           = "table",
	exposureTable          = "table",
	mergedCohortTable      = "mergedTable"
)


covariateSettings <-
	createGetCovariateSettings(
		covariateSettingsCm =
			FeatureExtraction::createCovariateSettings(
				useDemographicsGender           = TRUE,
				useDemographicsAge              = TRUE,
				useConditionOccurrenceLongTerm  = TRUE,
				useConditionOccurrenceShortTerm = TRUE,
				useDrugExposureLongTerm         = TRUE,
				useDrugExposureShortTerm        = TRUE,
				useDrugEraLongTerm              = TRUE,
				useDrugEraShortTerm             = TRUE,
				useCharlsonIndex                = TRUE,
				addDescendantsToExclude         = TRUE,
				addDescendantsToInclude         = TRUE
			),
		covariateSettingsPlp =
			FeatureExtraction::createCovariateSettings(
				useDemographicsGender           = TRUE,
				useDemographicsAge              = TRUE,
				useConditionOccurrenceLongTerm  = TRUE,
				useConditionOccurrenceShortTerm = TRUE,
				useDrugExposureLongTerm         = TRUE,
				useDrugExposureShortTerm        = TRUE,
				useDrugEraLongTerm              = TRUE,
				useDrugEraShortTerm             = TRUE,
				useCharlsonIndex                = TRUE,
				addDescendantsToExclude         = TRUE
			)
	)

#-------------------------------------------------------------------------------
# What should I do with prior outcomes?
#-------------------------------------------------------------------------------
populationSettings <-
	createPopulationSettings(
		populationPlpSettings = createPopulationPlpSettingsArgs(
			riskWindowStart                = 1,
			riskWindowEnd                  = 365,
			minTimeAtRisk                  = 364,
			removeSubjectsWithPriorOutcome = FALSE,
			includeAllOutcomes             = TRUE
		),
		populationCmSettings = createPopulationCmSettingsArgs(
			removeDuplicateSubjects = "keep first",
			riskWindowStart         = 1,
			riskWindowEnd           = 365,
		)
		# postProcessing = function(population) {
		#   population %>%
		#     dplyr::filter(ageYear >= 65) %>%
		#     return()
		# }
	)

guideline <- function(prediction) {
  prediction %>%
    dplyr::mutate(
      riskStratum = dplyr::case_when(
        ageYear <= 54 ~ as.numeric(value >= .0094),
        ageYear <= 59 ~ as.numeric(value >= .0132),
        ageYear <= 64 ~ as.numeric(value >= .0168),
        ageYear <= 69 ~ as.numeric(value >= .0228),
        ageYear <= 74 ~ as.numeric(value >= .0264),
        ageYear <= 79 ~ as.numeric(value >= .0312),
        ageYear <= 84 ~ as.numeric(value >= .0372),
        TRUE          ~ as.numeric(value >= .0396)
      ),
      riskStratum = riskStratum + 1,
      labels = ifelse(
        riskStratum == 1,
        "low risk",
        "high risk"
      )
    )
}

# guideline <- function(prediction) {
#  prediction %>%
#    dplyr::mutate(
#      riskStratum = dplyr::case_when(
#        ageYear <= 54 ~ as.numeric(value >= .0094*2),
#        ageYear <= 59 ~ as.numeric(value >= .0132*2),
#        ageYear <= 64 ~ as.numeric(value >= .0168*2),
#        ageYear <= 69 ~ as.numeric(value >= .0228*2),
#        ageYear <= 74 ~ as.numeric(value >= .0264*2),
#        ageYear <= 79 ~ as.numeric(value >= .0312*2),
#        ageYear <= 84 ~ as.numeric(value >= .0372*2),
#        TRUE          ~ as.numeric(value >= .0396*2)
#      ),
#      riskStratum = riskStratum + 1,
#      labels = ifelse(
#        riskStratum == 1,
#        "low risk",
#        "high risk"
#      )
#    )
# }

runSettings <- createRunSettings(
  runPlpSettings = createRunPlpSettingsArgs(
    testSplit        = "subject",
    modelSettings = PatientLevelPrediction::setLassoLogisticRegression(),
    matchingSettings = createMatchOnPsArgs(
      maxRatio = 4
    )
  ),
  runCmSettings = RiskStratifiedEstimation::createRunCmSettingsArgs(
    analyses = list(
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix, "m_1_10", sep = "_"),
      #   riskStratificationMethod     = "equal",
      #   riskStratificationThresholds = 4,
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
      #   timePoint                    = 365
      # ),
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix, "q_25_75", sep = "_"),
        riskStratificationMethod     = "quantile",
        riskStratificationThresholds = c(0, .75, 1),
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365
      )
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix, "gl", sep = "_"),
      #   riskStratificationMethod     = "custom",
      #   riskStratificationThresholds = guideline,
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
      #   timePoint                    = 365,
      #   stratificationOutcomes       = 102 
      # )
    ),
		psSettings = createCreatePsArgs(
			control = Cyclops::createControl(
				threads       = 5,
				maxIterations = 5e3
			)
		),
    fitOutcomeModelsThreads = 3,
    balanceThreads          = 5,
    negativeControlThreads  = 10,
    createPsThreads         = 10
  )
)

getDataSettings <- createGetDataSettings(
  plpDataFolder          = file.path("data", database, "plpData"),
  cohortMethodDataFolder = file.path("data", database, "cmData.zip")
)


analysisSettings <- createAnalysisSettings(
  analysisId              = paste(database, "new_ter_bis", "50", "1_year", sep = "_"),
  databaseName            = database,
  treatmentCohortId       = 12803,
  comparatorCohortId      = 12802,
  outcomeIds              = 101:103,
  analysisMatrix          = matrix(1, ncol = 3, nrow = 3),
  mapOutcomes             = mapOutcomes,
  mapTreatments           = mapTreatments,
  saveDirectory           = "results",
  verbosity               = "INFO",
  negativeControlOutcomes = negativeControls,
  balanceThreads          = 15,
  negativeControlThreads  = 15
)


analysisPath <- file.path(
  analysisSettings$saveDirectory,
  analysisSettings$analysisId
)

shinyDir <- file.path(
  analysisPath,
  "shiny"
)

predictOutcomes <- analysisSettings$outcomeIds[which(colSums(analysisSettings$analysisMatrix) != 0)]

#-----------------------------------------------------------------------------
# extract the second element of a list of lists (here the label)
#-----------------------------------------------------------------------------
analysisLabels <- unlist(
  rlist::list.map(
    runSettings$runCmSettings$analyses,
    label
  )
)

names(analysisLabels) <- NULL
names(runSettings$runCmSettings$analyses) <- analysisLabels
analysisSettings$analysisLabels <- analysisLabels

plpData <- PatientLevelPrediction::loadPlpData(
  getDataSettings$plpDataFolder
)

cohortMethodData <- CohortMethod::loadCohortMethodData(
  getDataSettings$cohortMethodDataFolder
)

cohortsCm <- cohortMethodData$cohorts %>%
  dplyr::collect() %>%
  dplyr::mutate(
    cohortStartDate = lubridate::as_date(cohortStartDate)
  )

cohortsPlp <- plpData$cohorts %>% dplyr::tibble()

initialPopulation <- cohortsCm %>%
  dplyr::left_join(
    cohortsPlp,
    by = c(
      "rowId",
      "cohortStartDate",
      "daysFromObsStart",
      "daysToCohortEnd",
      "daysToObsEnd"
    )
  )

#-----------------------------------------------------------------------------
# Negative controls
#-----------------------------------------------------------------------------
if (!is.null(analysisSettings$negativeControlOutcomes)) {
  cluster <- ParallelLogger::makeCluster(
    runSettings$runCmSettings$negativeControlThreads
  )
  
  ParallelLogger::clusterRequire(
    cluster,
    "RiskStratifiedEstimation"
  )
  ParallelLogger::clusterRequire(
    cluster,
    "CohortMethod"
  )
  negativeControlIds <- analysisSettings$negativeControlOutcomes
  
  dummy <- ParallelLogger::clusterApply(
    cluster            = cluster,
    x                  = negativeControlIds,
    fun                = fitPsModelOverall,
    initialPopulation  = initialPopulation,
    getDataSettings    = getDataSettings,
    populationSettings = populationSettings,
    analysisSettings   = analysisSettings,
    runCmSettings      = runSettings$runCmSettings,
    isNegativeControl  = TRUE
  )
  
  for (i in seq_along(runSettings$runCmSettings$analyses)) {
    ParallelLogger::logInfo(
      paste(
        "Fitting overall negative control outcome models for analysis:",
        analysisLabels[i]
      )
    )
    includeOverallResults(
      analysisSettings  = analysisSettings,
      getDataSettings   = getDataSettings,
      analysis          = runSettings$runCmSettings$analyses[[i]],
      runSettings       = runSettings,
      isNegativeControl = TRUE
    )
  }
  
  mergeTempFiles(shinyDir, "mappedOverallResultsNegativeControls")
  
  for (predictOutcome in predictOutcomes) {
    ParallelLogger::logInfo(
      paste(
        "Fitting propensity score models for negative controls within risk strata of:",
        predictOutcome
      )
    )
    dummy <- ParallelLogger::clusterApply(
      cluster = cluster,
      x                  = negativeControlIds,
      fun                = fitPsModelSwitch,
      initialPopulation  = initialPopulation,
      predictOutcome     = predictOutcome,
      analysisSettings   = analysisSettings,
      getDataSettings    = getDataSettings,
      populationSettings = populationSettings,
      runSettings        = runSettings
    )
    
    for (i in seq_along(analysisLabels)) {
      ParallelLogger::logInfo(
        paste(
          "Fitting outcome models for negative controls for analysis: ",
          analysisLabels[i]
        )
      )
      analysis <- runSettings$runCmSettings$analyses[[i]]
      pathToPs <- file.path(
        analysisSettings$saveDirectory,
        analysisSettings$analysisId,
        "Estimation",
        analysisLabels[i],
        predictOutcome
      )
      
      dummy <- tryCatch({
        ParallelLogger::clusterApply(
          cluster         = cluster,
          x               = negativeControlIds,
          fun             = fitOutcomeModels,
          getDataSettings = getDataSettings,
          pathToPs        = pathToPs,
          analysis        = analysis
        )},
        error = function(e) {
          e$message
        }
      )
    }
  }
}

ParallelLogger::logInfo(
  "Creating and saving overall results"
)
createOverallResults(analysisSettings)

settings <- list(
  analysisSettings   = analysisSettings,
  getDataSettings    = getDataSettings,
  databaseSettings   = databaseSettings,
  covariateSettings  = covariateSettings,
  populationSettings = populationSettings,
  runSettings        = runSettings
)

saveRDS(
  settings,
  file.path(
    analysisSettings$saveDirectory,
    analysisSettings$analysisId,
    "settings.rds"
  )
)

ParallelLogger::logInfo(
  'Run finished successfully'
)

# stop logger
ParallelLogger::clearLoggers()

logger <- ParallelLogger::createLogger(
  name      = "SIMPLE",
  threshold = "INFO",
  appenders = list(
    ParallelLogger::createConsoleAppender(
      layout = ParallelLogger::layoutTimestamp
    )
  )
)
