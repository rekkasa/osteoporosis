library(DatabaseConnector)
library(SqlRender)
library(CohortDiagnostics)

database <- "truven_ccae"
databaseVersion <- "v2008"
scratchSchema <- Sys.getenv("SCRATCH_SCHEMA")

# connectionDetails <- createConnectionDetails(
#   dbms = 'redshift',
#   server = "ohda-prod-1.cldcoxyrkflo.us-east-1.redshift.amazonaws.com/optum_extended_dod",
#   port = 5439,
#   user = Sys.getenv("OHDA_USER"),
#   password = Sys.getenv("OHDA_PASSWORD"),
#   extraSettings = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory",
#   pathToDriver = Sys.getenv("REDSHIFT_DRIVER")
# )

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

connection <- connect(connectionDetails)

sql <- SqlRender::readSql("add_table.sql")

renderTranslateExecuteSql(
  connection = connection,
  sql = sql,
  target_database_schema = scratchSchema,
  target_cohort_table = "osteoporosis_exposures"
)

cdmDatabaseSchema <- paste(
  "cdm",
  database,
  databaseVersion,
  sep = "_"
)
cohortDatabaseSchema <- scratchSchema
cohortTable <- "osteoporosis_exposures"
# cohortTable <- "osteoporosis_outcomes"
cohortIds <- c(6888, 6889)
# cohortIds <- 5402:5404

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

# exportFolder <- "export"
# executeDiagnostics(
#   cohortDefinitionSet,
#   connectionDetails = connectionDetails,
#   cohortTable = cohortTable,
#   cohortDatabaseSchema = cohortDatabaseSchema,
#   cdmDatabaseSchema = cdmDatabaseSchema,
#   exportFolder = exportFolder,
#   databaseId = "ccae",
#   minCellCount = 5
# )

CohortGenerator::dropCohortStatsTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames
)

# preMergeDiagnosticsFiles(exportFolder)
# launchDiagnosticsExplorer(
#   "D:\\Users\\ARekkas\\OneDrive - JNJ\\Documents\\osteoporosis\\export", 
#   dataFile = "PreMerged.RData"
# )


sql <- SqlRender::readSql("NegativeControls.sql")
sql <- SqlRender::render(
  sql,
  cdm_database_schema = cdmDatabaseSchema,
  target_database_schema = scratchSchema,
  target_cohort_table = "osteoporosis_outcomes",
  outcome_ids = readr::read_csv("negativeControls.csv") %>% pull(outcomeId)
)
sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
DatabaseConnector::executeSql(
  connection = connect(connectionDetails),
  sql = sql
)


sql <- "SELECT COHORT_DEFINITION_ID, COUNT(SUBJECT_ID) FROM scratch_arekkas.osteoporosis_exposures GROUP BY COHORT_DEFINITION_ID"
pp <- DatabaseConnector::querySql(connection = connect(connectionDetails), sql) %>% arrange(COHORT_DEFINITION_ID)

