# @file Achilles_v4
#
# Copyright 2017 Observational Health Data Sciences and Informatics
#
# This file is part of Achilles
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author Observational Health Data Sciences and Informatics
# @author Martijn Schuemie
# @author Patrick Ryan
# @author Vojtech Huser
# @author Chris Knoll
# @author Ajit Londhe

#' execution of data quality rules (for v4)
#'
#' @description
#' \code{achillesHeel_v4} executes data quality rules (or checks) on pre-computed analyses (or measures).
#'
#' @details
#' \code{achillesHeel_v4} contains number of rules (authored in SQL) that are executed againts achilles results tables.
#' 
#' @param connectionDetails  An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param oracleTempSchema    For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database. 
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param sourceName		string name of the database, as recorded in results
#' @param vocabDatabaseSchema		string name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' 
#' @return nothing is returned
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="sql server", server="RNDUSRDHIT07.jnj.com")
#'   achillesHeel <- achilles(connectionDetails, cdmDatabaseSchema="mycdm", resultsDatabaseSchema="scratch", vocabDatabaseSchema="vocabulary")
#' }
#' @export
achillesHeel_v4 <- function (connectionDetails, 
                             cdmDatabaseSchema, 
                             oracleTempSchema = cdmDatabaseSchema,
                             resultsDatabaseSchema = cdmDatabaseSchema,
                             vocabDatabaseSchema = cdmDatabaseSchema){
  
  inputFolder <- "v4"
  heelFile <- "AchillesHeel_v4.sql"
  heelSql <- loadRenderTranslateSql(sqlFilename = paste(inputFolder, heelFile, sep = "/"),
                                    packageName = "Achilles",
                                    dbms = connectionDetails$dbms,
                                    oracleTempSchema = oracleTempSchema,
                                    cdm_database_schema = cdmDatabaseSchema,
                                    # results_database = resultsDatabase,
                                    results_database_schema = resultsDatabaseSchema,
                                    # vocab_database = vocabDatabase,
                                    vocab_database_schema = vocabDatabaseSchema
  );
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  writeLines("Executing Achilles Heel. This could take a while");
  DatabaseConnector::executeSql(connection = connection, sql = heelSql)
  DatabaseConnector::disconnect(connection = connection);
  writeLines(paste("Done. Achilles Heel results can now be found in",resultsDatabaseSchema))
}

#new function to extract Heel resutls now when there are extra columns from inside R
#' @export
fetchAchillesHeelResults <- function (connectionDetails, resultsDatabaseSchema){
  connectionDetails$schema = resultsDatabaseSchema
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  
  sql <- "SELECT * FROM ACHILLES_heel_results"
  sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
  res <- DatabaseConnector::querySql(connection = connection, sql = sql)
  res
}

#' The main Achilles analysis (for v4)
#'
#' @description
#' \code{achilles_v4} creates descriptive statistics summary for an entire OMOP CDM instance.
#'
#' @details
#' \code{achilles_v4} creates descriptive statistics summary for an entire OMOP CDM instance.
#' 
#' @param connectionDetails  An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param oracleTempSchema    For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database. 
#' @param resultsDatabaseSchema		string name of database schema that we can write results to. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param sourceName		string name of the database, as recorded in results
#' @param analysisIds		(optional) a vector containing the set of Achilles analysisIds for which results will be generated.
#' If not specified, all analyses will be executed. Use \code{\link{getAnalysisDetails}} to get a list of all Achilles analyses and their Ids.
#' @param createTable     If true, new results tables will be created in the results schema. If not, the tables are assumed to already exists, and analysis results will be added
#' @param smallcellcount     To avoid patient identifiability, cells with small counts (<= smallcellcount) are deleted.
#' @param runHeel     Boolean to determine if Achilles Heel data quality reporting will be produced based on the summary statistics.  Default = TRUE
#' @param validateSchema     Boolean to determine if CDM Schema Validation should be run. This could be very slow.  Default = FALSE
#' @param vocabDatabaseSchema		string name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param runCostAnalysis Boolean to determine if cost analysis should be run. Note: only works on CDM v5.0 style cost tables.
#' 
#' @return An object of type \code{achillesResults} containing details for connecting to the database containing the results 
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="sql server", server="RNDUSRDHIT07.jnj.com")
#'   achillesResults <- achilles_v4(connectionDetails, cdmDatabaseSchema="cdm4_sim", resultsDatabaseSchema="scratch", sourceName="TestDB", validateSchema="TRUE", vocabDatabaseSchema="vocabulary")
#'   fetchAchillesAnalysisResults(connectionDetails, "scratch", 106)
#' }
#' @export
achilles_v4 <- function (connectionDetails, 
                         cdmDatabaseSchema, 
                         oracleTempSchema = cdmDatabaseSchema,
                         resultsDatabaseSchema = cdmDatabaseSchema, 
                         sourceName = "", 
                         analysisIds, 
                         createTable = TRUE, 
                         smallcellcount = 5, 
                         runHeel = TRUE,
                         validateSchema = FALSE,
                         vocabDatabaseSchema = cdmDatabaseSchema,
                         runCostAnalysis = FALSE,
                         sqlOnly = FALSE)
{
  cdmVersion <- 4
  inputFolder <- "v4"
  achillesFile <- "Achilles_v4.sql"
  heelFile <- "AchillesHeel_v4.sql"
  
  if (missing(analysisIds))
    analysisIds = getAnalysisDetails()$ANALYSIS_ID
  
  achillesSql <- loadRenderTranslateSql(sqlFilename = paste(inputFolder, achillesFile, sep = "/"),
                                        packageName = "Achilles",
                                        dbms = connectionDetails$dbms,
                                        oracleTempSchema = oracleTempSchema,
                                        cdm_database_schema = cdmDatabaseSchema,
                                        results_database_schema = resultsDatabaseSchema,
                                        source_name = sourceName, 
                                        list_of_analysis_ids = analysisIds,
                                        createTable = createTable,
                                        smallcellcount = smallcellcount,
                                        validateSchema = validateSchema,
                                        vocab_database_schema = vocabDatabaseSchema,
                                        runCostAnalysis = runCostAnalysis
  )
  
  if (sqlOnly)
  {
    outputFolder <- "output";
    
    if (!file.exists(outputFolder))
      dir.create(outputFolder);
    writeSql(achillesSql,paste(outputFolder, achillesFile, sep="/"))
    
    writeLines(paste("Achilles sql generated in: ", paste(outputFolder, achillesFile, sep="/")))
    
    return()
  }
  else
  {
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    writeLines("Executing multiple queries. This could take a while")
    DatabaseConnector::executeSql(connection = connection, sql = achillesSql)
    writeLines(paste("Done. Achilles results can now be found in",resultsDatabaseSchema))
  }
  
  if (runHeel)
  {
    heelSql <- loadRenderTranslateSql(sqlFilename = paste(inputFolder, heelFile, sep = "/"),
                                      packageName = "Achilles",
                                      dbms = connectionDetails$dbms,
                                      oracleTempSchema = oracleTempSchema,
                                      cdm_database_schema = cdmDatabaseSchema,
                                      results_database_schema = resultsDatabaseSchema,
                                      source_name = sourceName, 
                                      list_of_analysis_ids = analysisIds,
                                      createTable = createTable,
                                      smallcellcount = smallcellcount,
                                      vocab_database_schema = vocabDatabaseSchema
    )
    
    writeLines("Executing Achilles Heel. This could take a while")
    DatabaseConnector::executeSql(connection = connection, sql = heelSql)
    writeLines(paste("Done. Achilles Heel results can now be found in",resultsDatabaseSchema))    
    
  }
  else 
  {
    heelSql='HEEL EXECUTION SKIPPED PER USER REQUEST'
  }
  
  DatabaseConnector::disconnect(connection = connection)
  
  resultsConnectionDetails <- connectionDetails
  resultsConnectionDetails$schema = resultsDatabaseSchema
  result <- list(resultsConnectionDetails = resultsConnectionDetails, 
                 resultsTable = "ACHILLES_results",
                 resultsDistributionTable ="ACHILLES_results_dist",
                 analysis_table = "ACHILLES_analysis",
                 sourceName = sourceName,
                 analysisIds = analysisIds,
                 AchillesSql = achillesSql,
                 HeelSql = heelSql, #if runHeel is false -  this assignment fails - causes error of the  whole function  (adding else)
                 call = match.call())
  class(result) <- "achillesResults"
  result
}