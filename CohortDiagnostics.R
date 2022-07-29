connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = 'redshift',
  server = "ohda-prod-1.cldcoxyrkflo.us-east-1.redshift.amazonaws.com/optum_extended_dod",
  port = 5439,
  user = Sys.getenv("OHDA_USER"),
  password = Sys.getenv("OHDA_PASSWORD"),
  extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory",
  pathToDriver = Sys.getenv("REDSHIFT_DRIVER")
)

cdmDatabaseSchema <- "cdm_optum_extended_dod_v2050"
cohortDatabaseSchema <- "scratch_arekkas"
# cohortTable <- "osteoporosis_exposures"
cohortTable <- "osteoporosis_diagnostics"
cohortIds <- c(6888, 6889)
# cohortIds <- 5402:5404

exportFolder <- "diagnostics/optum_extended_dod"

baseUrl <- "https://epi.jnj.com:8443/WebAPI"
ROhdsiWebApi::authorizeWebApi(baseUrl, "windows") 

cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  baseUrl = baseUrl,
  cohortIds = cohortIds,
  generateStats = TRUE
)

cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
CohortGenerator::createCohortTables(
  connectionDetails = connectionDetails,
  cohortTableNames = cohortTableNames,
  cohortDatabaseSchema = cohortDatabaseSchema,
  incremental = FALSE
)

CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  incremental = FALSE
)

CohortDiagnostics::executeDiagnostics(
  cohortDefinitionSet,
  connectionDetails = connectionDetails,
  cohortTable = cohortTable,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  exportFolder = exportFolder,
  databaseId = "optum_extended_dod",
  minCellCount = 5
)


CohortGenerator::dropCohortStatsTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames
)

CohortDiagnostics::createMergedResultsFile("diagnostics")

CohortDiagnostics::launchDiagnosticsExplorer()
