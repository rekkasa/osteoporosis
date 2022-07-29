options(
  andromedaTempFolder = "tmp"
)

library(RiskStratifiedEstimation)
library(tidyverse)

connectionDetails <- createConnectionDetails(
  dbms = 'redshift',
  server = "ohda-prod-1.cldcoxyrkflo.us-east-1.redshift.amazonaws.com/truven_ccae",
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


database <- "truven_ccae_v2008"

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

negativeControls <- readr::read_csv(
  "negativeControls.csv",
  col_types = "dc"
) %>%
  filter(outcomeId != 4119796) %>%
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
  databaseName           = "ccae",
	cdmDatabaseSchema      = "cdm_truven_ccae_v2008",
	cohortDatabaseSchema   = "scratch_arekkas",
	outcomeDatabaseSchema  = "scratch_arekkas",
	resultsDatabaseSchema  = "scratch_arekkas",
	exposureDatabaseSchema = "scratch_arekkas",
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
			riskWindowEnd                  = 365,
			minTimeAtRisk                  = 1,
			removeSubjectsWithPriorOutcome = TRUE,
			includeAllOutcomes             = TRUE
	  ),
		populationCmSettings = CohortMethod::createCreateStudyPopulationArgs(
			removeDuplicateSubjects = "keep first",
			riskWindowStart         = 1,
			riskWindowEnd           = 365
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

runSettings <- createRunSettings(
  runPlpSettings = createRunPlpSettingsArgs(
    modelSettings = PatientLevelPrediction::setCoxModel(),
    executeSettings = PatientLevelPrediction::createDefaultExecuteSettings(),
    matchingSettings = createMatchOnPsArgs(maxRatio = 4),
    timepoint = 365 * 5
  ),
  runCmSettings = RiskStratifiedEstimation::createRunCmSettingsArgs(
    analyses = list(
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix, "m_1_10", sep = "_"),
        riskStratificationMethod     = "equal",
        riskStratificationThresholds = 4,
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365
      ),
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix, "q_25_75", sep = "_"),
        riskStratificationMethod     = "quantile",
        riskStratificationThresholds = c(0, .75, 1),
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365
      ),
      RiskStratifiedEstimation::createRunCmAnalysesArgs(
        label                        = paste(prefix, "gl", sep = "_"),
        riskStratificationMethod     = "custom",
        riskStratificationThresholds = guideline,
        psMethod                     = "matchOnPs",
        effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 10),
        timePoint                    = 365,
        stratificationOutcomes       = 5403
      )
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix2, "m_1_10", sep = "_"),
      #   riskStratificationMethod     = "equal",
      #   riskStratificationThresholds = 4,
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 4),
      #   timePoint                    = 730
      # ),
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix2, "q_25_75", sep = "_"),
      #   riskStratificationMethod     = "quantile",
      #   riskStratificationThresholds = c(0, .75, 1),
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 4),
      #   timePoint                    = 730
      # ),
      # RiskStratifiedEstimation::createRunCmAnalysesArgs(
      #   label                        = paste(prefix2, "gl", sep = "_"),
      #   riskStratificationMethod     = "custom",
      #   riskStratificationThresholds = guideline2,
      #   psMethod                     = "matchOnPs",
      #   effectEstimationSettings     = createMatchOnPsArgs(maxRatio = 4),
      #   timePoint                    = 730,
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
    createPsThreads         = 4
  )
)

#  getDataSettings <- createGetDataSettings(
#    getPlpDataSettings = createGetPlpDataArgs(washoutPeriod = 365),
#    getCmDataSettings = createGetCmDataArgs(washoutPeriod = 365)
# )

getDataSettings <- createGetDataSettings(
  plpDataFolder = "results/ter_bis_itt_50_truven_ccae_v2008/Data/plpData",
  cohortMethodDataFolder = "results/ter_bis_itt_50_truven_ccae_v2008/Data/cmData"
)


analysisSettings <- createAnalysisSettings(
  analysisId              = paste("ter_bis_itt", "50", database, sep = "_"),
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
  balanceThreads          = 2,
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

