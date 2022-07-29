options(
  andromedaTempFolder = "tmp"
)

library(RiskStratifiedEstimation)
library(tidyverse)

database <- "truven_mdcr"
databaseVersion <- "v2009"
databaseName <- "mdcr"
scratchSchema <- Sys.getenv("SCRATCH_SCHEMA")
analysisId <- paste("ter_bis_itt", "50", database, databaseVersion, sep = "_")

server <- file.path(
  Sys.getenv("OHDA_URL"),
  database
)
connectionDetails <- createConnectionDetails(
  dbms = 'redshift',
  server = server,
  port = 5439,
  user = Sys.getenv("OHDA_USER"),
  password = Sys.getenv("OHDA_PASSWORD"),
  extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory",
  pathToDriver = Sys.getenv("REDSHIFT_DRIVER")
)


excludedCovariateConceptIds <- c(
  1521987,
  1521987402,
  43559997302,
  1521987404,
  43559997304,
  1524674,
  21604156,
  21604155,
  1516800,
  21604154,
  1512480,
  21604152,
  1557272
) 


prefix <- paste(
  "age_50",
  "tr_1",
  sep = "_"
)

prefix2 <- paste(
  "age_50",
  "tr_2",
  sep = "_"
)

prefix5 <- paste(
  "age_50",
  "tr_5",
  sep = "_"
)

negativeControls <- readr::read_csv(
  "negativeControls.csv",
  col_types = "dc"
) %>%
  dplyr::filter(outcomeId != 4119796) %>%
  dplyr::pull(outcomeId)

mapTreatments <- readr::read_csv(
  "mapTreatments.csv",
  col_types = "dc"
)

mapOutcomes   <- readr::read_csv(
  "mapOutcomes.csv",
  col_types = "dc"
)

cdmDatabaseSchema <- paste(
  "cdm",
  database,
  databaseVersion,
  sep = "_"
)

databaseSettings <- createDatabaseSettings(
  databaseName           = databaseName,
	cdmDatabaseSchema      = cdmDatabaseSchema,
	cohortDatabaseSchema   = scratchSchema,
	outcomeDatabaseSchema  = scratchSchema,
	resultsDatabaseSchema  = scratchSchema,
	exposureDatabaseSchema = scratchSchema,
	cohortTable            = "osteoporosis_exposures",
	outcomeTable           = "osteoporosis_outcomes",
	exposureTable          = "osteoporosis_exposures",
	mergedCohortTable      = "osteoporosis_merged"
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
				addDescendantsToInclude         = TRUE,
				excludedCovariateConceptIds     = excludedCovariateConceptIds
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
				addDescendantsToExclude         = TRUE,
				excludedCovariateConceptIds     = excludedCovariateConceptIds
			)
	)

#-------------------------------------------------------------------------------
# What should I do with prior outcomes?
#-------------------------------------------------------------------------------
populationSettings <-
	createPopulationSettings(
	  populationPlpSettings = PatientLevelPrediction::createStudyPopulationSettings(
			riskWindowStart                = 1,
			riskWindowEnd                  = 0,
			endAnchor                      = "cohort end",
			minTimeAtRisk                  = 1,
			removeSubjectsWithPriorOutcome = FALSE,
			includeAllOutcomes             = TRUE
	  ),
		populationCmSettings = CohortMethod::createCreateStudyPopulationArgs(
			removeDuplicateSubjects        = "keep first",
			removeSubjectsWithPriorOutcome = FALSE,
			riskWindowStart                = 1,
			riskWindowEnd                  = 0,
			endAnchor                      = "cohort end"
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

guideline2 <- function(prediction) {
 prediction %>%
   dplyr::mutate(
     riskStratum = dplyr::case_when(
       ageYear <= 54 ~ as.numeric(value >= .0094*2),
       ageYear <= 59 ~ as.numeric(value >= .0132*2),
       ageYear <= 64 ~ as.numeric(value >= .0168*2),
       ageYear <= 69 ~ as.numeric(value >= .0228*2),
       ageYear <= 74 ~ as.numeric(value >= .0264*2),
       ageYear <= 79 ~ as.numeric(value >= .0312*2),
       ageYear <= 84 ~ as.numeric(value >= .0372*2),
       TRUE          ~ as.numeric(value >= .0396*2)
     ),
     riskStratum = riskStratum + 1,
     labels = ifelse(
       riskStratum == 1,
       "low risk",
       "high risk"
     )
   )
}

guideline5 <- function(prediction) {
 prediction %>%
   dplyr::mutate(
     riskStratum = dplyr::case_when(
       ageYear <= 54 ~ as.numeric(value >= .0094*5),
       ageYear <= 59 ~ as.numeric(value >= .0132*5),
       ageYear <= 64 ~ as.numeric(value >= .0168*5),
       ageYear <= 69 ~ as.numeric(value >= .0228*5),
       ageYear <= 74 ~ as.numeric(value >= .0264*5),
       ageYear <= 79 ~ as.numeric(value >= .0312*5),
       ageYear <= 84 ~ as.numeric(value >= .0372*5),
       TRUE          ~ as.numeric(value >= .0396*5)
     ),
     riskStratum = riskStratum + 1,
     labels = ifelse(
       riskStratum == 1,
       "low risk",
       "high risk"
     )
   )
}

runSettings <- createRunSettings(
  runPlpSettings = createRunPlpSettingsArgs(
    analyses = list(
      createRunPlpAnalysesArgs(
        outcomeId = 5402,
        modelSettings = PatientLevelPrediction::setCoxModel(),
        matchingSettings = createMatchOnPsArgs(maxRatio = 10),
        executeSettings = PatientLevelPrediction::createDefaultExecuteSettings(),
        timepoint = 365 * 5
      ),
      createRunPlpAnalysesArgs(
        outcomeId = 5403,
        modelSettings = PatientLevelPrediction::setCoxModel(),
        matchingSettings = createMatchOnPsArgs(maxRatio = 10),
        executeSettings = PatientLevelPrediction::createDefaultExecuteSettings(),
        timepoint = 365 * 5
      ),
      createRunPlpAnalysesArgs(
        outcomeId = 5404,
        modelSettings = PatientLevelPrediction::setCoxModel(),
        matchingSettings = createMatchOnPsArgs(maxRatio = 10),
        executeSettings = PatientLevelPrediction::createDefaultExecuteSettings(),
        timepoint = 365 * 5
      )
    )
  ),
  runCmSettings = RiskStratifiedEstimation::createRunCmSettingsArgs(
    analyses = list(
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix5, "m_1_10", sep = "_"),
        riskStratificationMethod     = "equal",
        riskStratificationThresholds = 4,
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365 * 5
      ),
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix5, "q_25_75", sep = "_"),
        riskStratificationMethod     = "quantile",
        riskStratificationThresholds = c(0, .75, 1),
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365 * 5
      )
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix5, "gl", sep = "_"),
      #   riskStratificationMethod     = "custom",
      #   riskStratificationThresholds = guideline5,
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
      #   timePoint                    = 365 * 5,
      #   stratificationOutcomes       = 5403
      # )
    ),
		psSettings = createCreatePsArgs(
			control = Cyclops::createControl(
				threads       = 2,
				maxIterations = 5e3
			)
		),
    fitOutcomeModelsThreads = 4,
    balanceThreads          = 4,
    negativeControlThreads  = 4,
    createPsThreads         = 4,
		runRiskStratifiedNcs    = FALSE
  )
)

 getDataSettings <- createGetDataSettings(
   getPlpDataSettings = createGetPlpDataArgs(washoutPeriod = 365),
   getCmDataSettings = createGetCmDataArgs(washoutPeriod = 365)
)

# getDataSettings <- createGetDataSettings(
#   plpDataFolder = "results/ter_bis_itt_50_truven_ccae_v2008/Data/plpData",
#   cohortMethodDataFolder = "results/ter_bis_itt_50_truven_ccae_v2008/Data/cmData"
# )

analysisSettings <- createAnalysisSettings(
  analysisId              = analysisId,
  databaseName            = database,
  treatmentCohortId       = 6889,
  comparatorCohortId      = 6888,
  outcomeIds              = 5402:5404,
  analysisMatrix          = matrix(1, ncol = 3, nrow = 3),
  mapOutcomes             = mapOutcomes,
  mapTreatments           = mapTreatments,
  saveDirectory           = "results",
  verbosity               = "INFO",
  negativeControlOutcomes = negativeControls,
  balanceThreads          = 1,
  negativeControlThreads  = 2
)



runRiskStratifiedEstimation(
  connectionDetails  = connectionDetails,
  analysisSettings   = analysisSettings,
  databaseSettings   = databaseSettings,
  getDataSettings    = getDataSettings,
  covariateSettings  = covariateSettings,
  populationSettings = populationSettings,
  runSettings        = runSettings
)
  