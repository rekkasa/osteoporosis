delete <- function(table, connectionDetails) {
  sql <- SqlRender::readSql("drop_table.sql")
  sql <- SqlRender::render(sql, scratch = "scratch_arekkas", cohort_table = table)
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(connection = connect(connectionDetails), sql = sql)
}

tableNames <- c(
  "outcomes_rsee_framework"
)

lapply(tableNames, delete, connectionDetails = connectionDetails)


