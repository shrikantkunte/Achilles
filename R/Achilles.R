# @file Achilles
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


#' The main Achilles analysis (for v5+)
#'
#' @description
#' \code{achilles} creates descriptive statistics summary for an entire OMOP CDM instance.
#'
#' @details
#' \code{achilles} creates descriptive statistics summary for an entire OMOP CDM instance.
#' 
#' @param connectionDetails                An R object of type ConnectionDetails (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	           Fully qualified name of database schema that contains OMOP CDM (including Vocabulary). 
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema. 
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @param scratchDatabaseSchema            Fully qualified name of the database schema that will store all of the intermediate scratch tables, so for example, on SQL Server, 'cdm_scratch.dbo'. 
#'                                         Must be accessible to/from the cdmDatabaseSchema and the resultsDatabaseSchema. Default is resultsDatabaseSchema. 
#'                                         Making this "#" will run Achilles in single-threaded mode and use temporary tables instead of permanent tables.
#' @param sourceName		                   String name of the database, as recorded in results.
#' @param analysisIds		                   (OPTIONAL) A vector containing the set of Achilles analysisIds for which results will be generated. 
#'                                         If not specified, all analyses will be executed. Use \code{\link{getAnalysisDetails}} to get a list of all Achilles analyses and their Ids.
#' @param smallCellCount                   To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.
#' @param cdmVersion                       Define the OMOP CDM version used:  currently supports v5 and above.  Default = "5". v4 support is in \code{\link{achilles_v4}}.
#' @param runHeel                          Boolean to determine if Achilles Heel data quality reporting will be produced based on the summary statistics.  Default = TRUE
#' @param validateCdmSchema                Boolean to determine if CDM Schema Validation should be run. This could be very slow.  Default = FALSE
#' @param runCostAnalysis                  Boolean to determine if cost analysis should be run. Note: only works on CDM v5 and v5.0.1 style cost tables.
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param numThreads                       (optional) Only for v5.x: The number of threads to use to run Achilles in parallel. Default is 1 thread.
#' @param tempAchillesPrefix               (optional) The prefix to use for the scratch Achilles analyses tables. Default is "tmpach"
#' @param dropScratchTables                In multi-threaded mode: TRUE = drop the scratch tables (may take time depending on dbms), FALSE = leave them in place for later removal.
#' 
#' @return                                 An object of type \code{achillesResults} containing details for connecting to the database containing the results 
#' @examples                               \dontrun{
#'                                           connectionDetails <- createConnectionDetails(dbms="sql server", server="some_server")
#'                                           achillesResults <- achilles(connectionDetails = connectionDetails, 
#'                                             cdmDatabaseSchema = "cdm", 
#'                                             resultsDatabaseSchema="results", 
#'                                             scratchDatabaseSchema="scratch",
#'                                             sourceName="Some Source", 
#'                                             cdmVersion = "5.0.1", 
#'                                             runCostAnalysis = TRUE, 
#'                                             numThreads = 10)
#'                                         }
#' @export
achilles <- function(connectionDetails, 
                     cdmDatabaseSchema, 
                     resultsDatabaseSchema = cdmDatabaseSchema, 
                     scratchDatabaseSchema = resultsDatabaseSchema,
                     sourceName = "", 
                     analysisIds, 
                     smallCellCount = 5, 
                     cdmVersion = "5", 
                     runHeel = TRUE,
                     validateCdmSchema = FALSE,
                     runCostAnalysis = FALSE,
                     sqlOnly = FALSE,
                     numThreads = 1,
                     tempAchillesPrefix = "tmpach",
                     dropScratchTables = TRUE)
{
  if (compareVersion(a = cdmVersion, b = "5") < 0)
  {
    stop("Error: Invalid CDM Version number; this function is only for v5 and above. Use achilles_v4 function if CDM is version 4.")
  }
  
  # Establish folder paths --------------------------------------------------------------------------------------------------------
  
  inputFolder <- paste("v5", "analyses", sep = "/")
  outputFolder <- paste("output", paste0("v", cdmVersion), sep = "/")
  
  if (sqlOnly)
  {
    unlink(x = outputFolder, recursive = TRUE, force = TRUE)
    dir.create(path = outputFolder, recursive = TRUE)
  }
  
  # Obtain analyses to run --------------------------------------------------------------------------------------------------------
  
  analysisDetails <- getAnalysisDetails()
  if (!missing(analysisIds))
  {
    analysisDetails <- analysisDetails[analysisDetails$ANALYSIS_ID %in% analysisIds, ]
  }
  
  if (!runCostAnalysis)
  {
    analysisDetails <- analysisDetails[analysisDetails$COST == 0, ]
  }
  
  detailTables <- .getDetailTables(tempAchillesPrefix = tempAchillesPrefix, analysisDetails = analysisDetails)
  
  # Initialize serial execution (use temp tables and 1 thread only) ----------------------------------------------------------------
  
  if (numThreads == 1 | scratchDatabaseSchema == "#")
  {
    numThreads <- 1
    scratchDatabaseSchema <- "#"
    schemaDelim <- "s_"
    connection <- DatabaseConnector::connect(connectionDetails) # first invocation of the connection, to persist throughout to maintain temp tables
  }
  else
  {
    schemaDelim <- "."
  }
  
  # Validate CDM schema (optional) --------------------------------------------------------------------------------------------------
  
  if (validateCdmSchema)
  {
    validateCdmSchema(connectionDetails = connectionDetails, cdmDatabaseSchema = cdmDatabaseSchema, runCostAnalysis = runCostAnalysis, cdmVersion = cdmVersion)
  }
  
  # Create analysis table -------------------------------------------------------------
  
  analysesSqls <- apply(analysisDetails, 1, function(analysisDetail)
  {  
    SqlRender::renderSql("select @analysisId as analysis_id, '@analysisName' as analysis_name,
                         '@stratum1Name' as stratum_1_name, '@stratum2Name' as stratum_2_name,
                         '@stratum3Name' as stratum_3_name, '@stratum4Name' as stratum_4_name,
                         '@stratum5Name' as stratum_5_name", 
                         analysisId = analysisDetail["ANALYSIS_ID"],
                         analysisName = analysisDetail["ANALYSIS_NAME"],
                         stratum1Name = analysisDetail["STRATUM_1_NAME"],
                         stratum2Name = analysisDetail["STRATUM_2_NAME"],
                         stratum3Name = analysisDetail["STRATUM_3_NAME"],
                         stratum4Name = analysisDetail["STRATUM_4_NAME"],
                         stratum5Name = analysisDetail["STRATUM_5_NAME"])$sql
  })  
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "CreateAnalysisTable.sql", sep = "/"), 
                                           packageName = "Achilles", 
                                           dbms = connectionDetails$dbms,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           analysesSqls = paste(analysesSqls, collapse = " \nunion all\n "))
  
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, targetFile = paste(outputFolder, "CreateAnalysisTable.sql", sep = "/"))
  }
  else
  {
    if (numThreads == 1) # connection is already alive
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql)
    }
    else
    {
      connection <- DatabaseConnector::connect(connectionDetails)
      DatabaseConnector::executeSql(connection = connection, sql = sql)
      DatabaseConnector::disconnect(connection)
    }
  }
  
  # Generate cost analyses ----------------------------------------------------------
  
  if (runCostAnalysis)
  {
    distCostAnalysisDetails <- analysisDetails[analysisDetails$COST == 1 & analysisDetails$DISTRIBUTION == 1, ]
    analysisDetails <- analysisDetails[!(analysisDetails$ANALYSIS_ID %in% distCostAnalysisDetails$ANALYSIS_ID), ]
    
    pathToCsv <- system.file("csv", "cost_columns.csv", package = "Achilles")
    costMappings <- read.csv(pathToCsv)
    drugCostMappings <- costMappings[costMappings$DOMAIN == "Drug", ]
    procedureCostMappings <- costMappings[costMappings$DOMAIN == "Procedure", ]
    
    distCostDrugSqls <- apply(X = distCostAnalysisDetails[distCostAnalysisDetails$STRATUM_1_NAME == "drug_concept_id", ],
                              1, function (analysisDetail)
    {
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "CostDistributionTemplate.sql", sep = "/"),
                                               packageName = "Achilles",
                                               dbms = connectionDetails$dbms,
                                               cdmVersion = cdmVersion,
                                               schemaDelim = schemaDelim,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               scratchDatabaseSchema = scratchDatabaseSchema,
                                               costColumn = drugCostMappings[drugCostMappings$V5 == analysisDetail["DISTRIBUTED_FIELD"], ]$V5_0_1,
                                               countValue = analysisDetail["DISTRIBUTED_FIELD"],
                                               domain = "Drug",
                                               domainTable = .getDomainTable(domainId = "Drug"),
                                               analysisId = analysisDetail["ANALYSIS_ID"],
                                               tempAchillesPrefix = tempAchillesPrefix)
    })
    
    distCostProcedureSqls <- apply(X = distCostAnalysisDetails[distCostAnalysisDetails$STRATUM_1_NAME == "procedure_concept_id", ], 
                                                               1, function (analysisDetail)
    {
      
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "CostDistributionTemplate.sql", sep = "/"),
                                               packageName = "Achilles",
                                               dbms = connectionDetails$dbms,
                                               cdmVersion = cdmVersion,
                                               schemaDelim = schemaDelim,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               scratchDatabaseSchema = scratchDatabaseSchema,
                                               costColumn = procedureCostMappings[drugCostMappings$V5 == analysisDetail["DISTRIBUTED_FIELD"], ]$V5_0_1,
                                               countValue = analysisDetail["DISTRIBUTED_FIELD"],
                                               domain = "Procedure",
                                               domainTable = .getDomainTable(domainId = "Procedure"),
                                               analysisId = analysisDetail["ANALYSIS_ID"],
                                               tempAchillesPrefix = tempAchillesPrefix)
    })
    
    if (sqlOnly)
    {
      SqlRender::writeSql(sql = paste(distCostDrugSqls, distCostProcedureSqls, sep = "\n\n"), 
                          targetFile = paste(outputFolder, "DistributedCosts.sql", sep = "/"))
    }
    else
    {
      distCostAnalysisSqls <- c(distCostDrugSqls, distCostProcedureSqls)
      
      if (numThreads == 1)
      {
        for (distCostAnalysisSql in distCostAnalysisSqls)
        {
          DatabaseConnector::executeSql(connection = connection, sql = distCostAnalysisSql)
        }
      }
      else
      {
        cluster <- OhdsiRTools::makeCluster(numberOfThreads = length(distCostAnalysisDetails))
        dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = distCostAnalysisSqls, function(distCostAnalysisSql)
        {
          connection <- DatabaseConnector::connect(connectionDetails)
          DatabaseConnector::executeSql(connection = connection, sql = distCostAnalysisSql)
          DatabaseConnector::disconnect(connection)
        })
        OhdsiRTools::stopCluster(cluster)
      }
    }
  }
  
  if (sqlOnly)
  {
    writeLines("Achilles SQL scripts generating")
  }
  else
  {
    writeLines("Executing multiple queries. This could take a while")
  }
  
  if (numThreads == 1)
  {
    # Single thread: Generate scratch analysis tables -------------------------------------------------------------------
    
    for (analysisId in analysisDetails$ANALYSIS_ID)
    {
      .runAchillesAnalysisId(analysisId = analysisId,
                            connectionDetails = connectionDetails,
                            connection = connection,
                            schemaDelim = schemaDelim,
                            scratchDatabaseSchema = scratchDatabaseSchema,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            cdmVersion = cdmVersion,
                            tempAchillesPrefix = tempAchillesPrefix,
                            detailTables = detailTables,
                            sourceName = sourceName,
                            sqlOnly = sqlOnly) 
    }
    
    # Single thread: Merge analysis tables into 1 final analysis table -----------------------------------------------
    
    for (detailTable in detailTables)
    {
      .mergeAchillesScratchTables(detailTable = detailTable,
                                 connectionDetails = connectionDetails,
                                 connection = connection,
                                 schemaDelim = schemaDelim,
                                 scratchDatabaseSchema = scratchDatabaseSchema,
                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                 cdmVersion = cdmVersion,
                                 tempAchillesPrefix = tempAchillesPrefix,
                                 sqlOnly = sqlOnly,
                                 smallCellCount = smallCellCount)
    }
  }
  else
  {
    # Multi-thread: Generate scratch analysis tables ------------------------------------------------------------------
    
    cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
    dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = analysisDetails$ANALYSIS_ID, 
                                       fun = .runAchillesAnalysisId,
                                       connectionDetails = connectionDetails,
                                       connection = NULL,
                                       schemaDelim = schemaDelim,
                                       scratchDatabaseSchema = scratchDatabaseSchema,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       cdmVersion = cdmVersion,
                                       tempAchillesPrefix = tempAchillesPrefix,
                                       detailTables = detailTables,
                                       sourceName = sourceName,
                                       sqlOnly = sqlOnly)
    OhdsiRTools::stopCluster(cluster)
    
    # Multi-thread: Merge analysis tables into 1 final analysis table -----------------------------------------------
    
    cluster <- OhdsiRTools::makeCluster(numberOfThreads = 2)
    dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = detailTables, fun = .mergeAchillesScratchTables,
                                       connectionDetails = connectionDetails,
                                       connection = NULL,
                                       schemaDelim = schemaDelim,
                                       scratchDatabaseSchema = scratchDatabaseSchema,
                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                       cdmVersion = cdmVersion,
                                       tempAchillesPrefix = tempAchillesPrefix,
                                       sqlOnly = sqlOnly,
                                       smallCellCount = smallCellCount)
    
    OhdsiRTools::stopCluster(cluster)  
  }
  
  # Create PDW indexes (if dbms = "pdw") -----------------------------------------------------------------
  
  if (connectionDetails$dbms == "pdw")
  {
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "CreatePdwIndexes.sql", sep = "/"),
                                             packageName = "Achilles",
                                             dbms = connectionDetails$dbms,
                                             resultsDatabaseSchema = resultsDatabaseSchema)
    
    if (sqlOnly)
    {
      SqlRender::writeSql(sql = sql, targetFile = paste(outputFolder, "CreatePdwIndexes.sql", sep = "/"))
    }
    else
    {
      connection <- DatabaseConnector::connect(connectionDetails)
      DatabaseConnector::executeSql(connection = connection, sql = sql)
      DatabaseConnector::disconnect(connection)
    }
  }
  
  if (sqlOnly)
  {
    writeLines(paste0("Achilles SQL can be found in folder: ", outputFolder))
  }
  else
  {
    writeLines(paste0("Done. Achilles results can now be found in schema ", resultsDatabaseSchema))
  }
  
  # Drop scratch tables -----------------------------------------------
  
  if (numThreads == 1) # Dropping the connection removes the temporary scratch tables if running in serial
  {
    DatabaseConnector::disconnect(connection)
  }
  else if (dropScratchTables) # Drop the scratch tables
  {
    writeLines(paste0("Dropping scratch Achilles tables from schema ", scratchDatabaseSchema))
    
    cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
    dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = analysisDetails$ANALYSIS_ID,
                                       fun = .dropAchillesScratchTable,
                                       connectionDetails = connectionDetails,
                                       scratchDatabaseSchema = scratchDatabaseSchema,
                                       tempAchillesPrefix = tempAchillesPrefix,
                                       detailTables = detailTables,
                                       sqlOnly = sqlOnly)


    OhdsiRTools::stopCluster(cluster)
    writeLines(paste0("Temporary Achilles tables removed from schema ", scratchDatabaseSchema))
  }
  
  # Run Heel? ---------------------------------------------------------------
  
  if (runHeel)
  {
    achillesHeel(connectionDetails = connectionDetails,
                 cdmDatabaseSchema = cdmDatabaseSchema,
                 resultsDatabaseSchema = resultsDatabaseSchema,
                 scratchDatabaseSchema = scratchDatabaseSchema,
                 cdmVersion = cdmVersion,
                 sqlOnly = sqlOnly,
                 numThreads = numThreads,
                 tempHeelPrefix = "tmpheel",
                 dropScratchTables = dropScratchTables)
  }
}


#' execution of data quality rules (for v5 and above)
#'
#' @description
#' \code{achillesHeel} executes data quality rules (or checks) on pre-computed analyses (or measures).
#'
#' @details
#' \code{achillesHeel} contains number of rules (authored in SQL) that are executed againts achilles results tables.
#' 
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	           string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         string name of database schema that we can write final results to. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, 
#'                                         so for example 'results.dbo'.
#' @param scratchDatabaseSchema            (only affects multi-threaded mode) Name of a fully qualified schema that is accessible to/from the resultsDatabaseSchema, that can store all of the scratch tables. Default is resultsDatabaseSchema.
#' @param cdmVersion                       Define the OMOP CDM version used:  currently supports v5 and above.  Default = "5". v4 support is in \code{\link{achilles_v4}}.
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param numThreads                       (optional) Only for v5.x: The number of threads to use to run Achilles in parallel. Default is 1 thread.
#' @param tempHeelPrefix                   (optional) The prefix to use for the "temporary" (but actually permanent) Heel tables. Default is "tmpheel"
#' @param dropScratchTables                In multi-threaded mode: TRUE = drop the scratch tables (may take time depending on dbms), FALSE = leave them in place
#' @param ThresholdAgeWarning              The maximum age to allow in Heel
#' @param ThresholdOutpatientVisitPerc     The maximum percentage of outpatient visits among all visits
#' @param ThresholdMinimalPtMeasDxRx       The minimum percentage of patients with at least 1 Measurement, 1 Dx, and 1 Rx
#' 
#' @return nothing is returned
#' @examples \dontrun{
#'   connectionDetails <- createConnectionDetails(dbms="sql server", server="RNDUSRDHIT07.jnj.com")
#'   achillesHeel <- achillesHeel(connectionDetails = connectionDetails, 
#'                                cdmDatabaseSchema = "cdm", 
#'                                resultsDatabaseSchema = "results", 
#'                                scratchDatabaseSchema = "scratch",
#'                                cdmVersion = "5.0.1",
#'                                numThreads = 10)
#' }
#' @export
achillesHeel <- function(connectionDetails, 
                         cdmDatabaseSchema, 
                         resultsDatabaseSchema = cdmDatabaseSchema,
                         scratchDatabaseSchema = resultsDatabaseSchema,
                         cdmVersion = "5",
                         sqlOnly = FALSE,
                         numThreads = 1,
                         tempHeelPrefix = "tmpheel",
                         dropScratchTables = FALSE,
                         ThresholdAgeWarning = 125,
                         ThresholdOutpatientVisitPerc = 0.43,
                         ThresholdMinimalPtMeasDxRx = 20.5)
{
  if (compareVersion(a = cdmVersion, b = "5") < 0)
  {
    stop("Error: Invalid CDM Version number; this function is only for v5 and above. Use achillesHeel_v4 function if CDM is version 4.")
  }
  
  inputFolder <- paste("v5", "heels", sep = "/")
  outputFolder <- paste("output", paste0("v", cdmVersion), sep = "/")
  if (!dir.exists(outputFolder))
  {
    dir.create(path = outputFolder)
  }
  
  # Initialize serial execution (use temp tables and 1 thread only) ----------------------------------------------------------------
  
  if (numThreads == 1 | scratchDatabaseSchema == "#")
  {
    numThreads <- 1
    scratchDatabaseSchema <- "#"
    schemaDelim <- "s_"
    connection <- DatabaseConnector::connect(connectionDetails) # first invocation of the connection, to persist throughout to maintain temp tables
  }
  else
  {
    schemaDelim <- "."
  }
  
  
  # Sub-functions to handle cluster applied operations----------------------------------------------------------------------------------
  
  writeLines("Executing Achilles Heel. This could take a while")
  
  heelFiles <- list.files(path = paste(system.file(package = 'Achilles'), 
                                       "sql/sql_server", inputFolder, "independents", sep = "/"), 
                          recursive = TRUE, 
                          full.names = TRUE, 
                          all.files = FALSE,
                          pattern = "\\.sql$")
  
  
  # Run parallel queries first --------------------------------------------------------
  
  if (numThreads == 1)
  {
    for (heelFile in heelFiles)
    {
      .runHeelId(heelFile = heelFile,
                connectionDetails = connectionDetails,
                connection = connection,
                schemaDelim = schemaDelim,
                cdmDatabaseSchema = cdmDatabaseSchema,
                resultsDatabaseSchema = resultsDatabaseSchema,
                scratchDatabaseSchema = scratchDatabaseSchema,
                tempHeelPrefix = tempHeelPrefix,
                sqlOnly = sqlOnly,
                inputFolder = inputFolder,
                outputFolder = outputFolder)
    }
  }
  else
  {
    # drop scratch tables if they exist
    
    cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
    dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = heelFiles,
                                       fun = .dropHeelScratchTable,
                                       connectionDetails = connectionDetails, 
                                       scratchDatabaseSchema = scratchDatabaseSchema,
                                       tempHeelPrefix = tempHeelPrefix, 
                                       sqlOnly = sqlOnly)
    OhdsiRTools::stopCluster(cluster)                                   
    
    # run parallel table creates
    
    cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
    dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = heelFiles,
                                       fun = .runHeelId,
                                       connectionDetails = connectionDetails,
                                       connection = NULL,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                       scratchDatabaseSchema = scratchDatabaseSchema,
                                       schemaDelim = schemaDelim,
                                       tempHeelPrefix = tempHeelPrefix,
                                       sqlOnly = sqlOnly,
                                       inputFolder = inputFolder,
                                       outputFolder = outputFolder)
    OhdsiRTools::stopCluster(cluster)
  }
  
  # Merge scratch tables into final tables ----------------------------------------
  
  isDerived <- sapply(heelFiles, function(heelFile) { grepl(pattern = "derived", heelFile) })
  
  derivedSqls <- lapply(X = heelFiles[isDerived], function(heelFile)
  {
    SqlRender::renderSql(sql = 
                "select 
                cast(analysis_id as int) as analysis_id, 
                cast(stratum_1 as varchar(255)) as stratum_1, 
                cast(stratum_2 as varchar(255)) as stratum_2, 
                cast(statistic_value as float) as statistic_value, 
                cast(measure_id as varchar(255)) as measure_id
              from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_@heelName", 
              scratchDatabaseSchema = scratchDatabaseSchema, 
              schemaDelim = ifelse(scratchDatabaseSchema == "#", "s_", "."),
              tempHeelPrefix = tempHeelPrefix,
              heelName = gsub(pattern = ".sql", replacement = "", x = basename(heelFile)))$sql   
  })
  
  derivedSql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "merge_derived.sql", sep = "/"), 
                                           packageName = "Achilles", 
                                           dbms = connectionDetails$dbms,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           derivedSqls = paste(derivedSqls, collapse = " \nunion all\n "))
  
  resultSqls <- lapply(X = heelFiles[!isDerived], function(heelFile)
  {
    renderSql(sql = 
              "select 
              cast(analysis_id as int) as analysis_id, 
              cast(ACHILLES_HEEL_warning as varchar(255)) as ACHILLES_HEEL_warning, 
              cast(rule_id as int) as rule_id, 
              cast(record_count as bigint) as record_count
              from @scratchDatabaseSchema@schemaDelim@tempHeelPrefix_@heelName",
              scratchDatabaseSchema = scratchDatabaseSchema,
              schemaDelim = ifelse(scratchDatabaseSchema == "#", "s_", "."),
              tempHeelPrefix = tempHeelPrefix,
              heelName = gsub(pattern = ".sql", replacement = "", x = basename(heelFile)))$sql   
  })
  
  resultSql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "merge_heel_results.sql", sep = "/"), 
                                           packageName = "Achilles", 
                                           dbms = connectionDetails$dbms,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           resultSqls = paste(resultSqls, collapse = " \nunion all\n "))
  
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = derivedSql, targetFile = paste(outputFolder, "merge_derived.sql", sep = "/"))
    SqlRender::writeSql(sql = resultSql, targetFile = paste(outputFolder, "merge_heel_results.sql", sep = "/"))
  }
  else
  {
    if (numThreads == 1) # connection is already alive
    {
      DatabaseConnector::executeSql(connection = connection, sql = derivedSql)
      DatabaseConnector::executeSql(connection = connection, sql = resultSql)
    }
    else 
    {
      cluster <- OhdsiRTools::makeCluster(numberOfThreads = 2)
      dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = c(derivedSql, resultSql), 
        function(sql)
        {
          connection <- DatabaseConnector::connect(connectionDetails)
          DatabaseConnector::executeSql(connection = connection, sql = sql)  
          DatabaseConnector::disconnect(connection)
        }
      )
      OhdsiRTools::stopCluster(cluster)
    }
  }
  
  # Run serial queries to finish up ---------------------------------------------------
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "Dependents.sql", sep = "/"),
                                               packageName = "Achilles",
                                               dbms = connectionDetails$dbms,
                                               scratchDatabaseSchema = scratchDatabaseSchema,
                                               resultsDatabaseSchema = resultsDatabaseSchema,
                                               ThresholdAgeWarning = ThresholdAgeWarning,
                                               ThresholdOutpatientVisitPerc = ThresholdOutpatientVisitPerc,
                                               ThresholdMinimalPtMeasDxRx = ThresholdMinimalPtMeasDxRx
  )
  
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, targetFile = paste(outputFolder, "Serial.sql", sep = "/"))
  }
  else
  {
    connection <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(connection = connection, sql = sql)
    DatabaseConnector::disconnect(connection)
  }
  
  # Drop scratch tables -----------------------------------------------
  
  if (numThreads == 1) # Dropping the connection removes the temporary scratch tables if running in serial
  {
    DatabaseConnector::disconnect(connection)
  }
  else
  {
    if (dropScratchTables)
    {
      cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
      dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = heelFiles, 
                                         fun = .dropHeelScratchTable,
                                         connectionDetails = connectionDetails,
                                         resultsDatabaseSchema = resultsDatabaseSchema,
                                         tempHeelPrefix = tempHeelPrefix,
                                         sqlOnly = sqlOnly)
      
      OhdsiRTools::stopCluster(cluster)  
    }
  }
  writeLines(paste("Done. Achilles Heel results can now be found in", resultsDatabaseSchema))
}



#' Validate the CDM schema
#' 
#' @details 
#' Runs a validation script to ensure the CDM is valid based on v5.x
#' 
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param cdmDatabaseSchema    	           string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param runCostAnalysis                  Boolean to determine if cost analysis should be run. Note: only works on CDM v5 and v5.0.1 style cost tables.
#' @param cdmVersion                       Define the OMOP CDM version used:  currently supports "5", "5.0.1".  Default = "5". v4 support is in \code{\link{achilles_v4}}.
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' 
#' @export
validateCdmSchema <- function(connectionDetails,
                              cdmDatabaseSchema,
                              runCostAnalysis,
                              cdmVersion,
                              sqlOnly = FALSE)
{
  inputFolder <- paste("v5", "analyses", sep = "/")
  outputFolder <- paste("output", paste0("v", cdmVersion), sep = "/")
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "ValidateCdmSchema.sql", sep = "/"), 
                                           packageName = "Achilles", 
                                           dbms = connectionDetails$dbms,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           runCostAnalysis = runCostAnalysis,
                                           cdmVersion = cdmVersion)
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, targetFile = paste(outputFolder, 
                                                      paste("ValidateSchema", "sql", sep = "."), sep = "/"))
  }
  else
  {
    connection <- DatabaseConnector::connect(connectionDetails)
    tables <- DatabaseConnector::querySql(connection = connection, sql = sql)
    writeLines(paste("CDM Schema is valid:", paste(unlist(tables), collapse = ", "), sep = "\n\n"))
    DatabaseConnector::disconnect(connection)
  }
}

#' Get all analysis details
#' 
#' @details 
#' Get a list of all analyses with their analysis IDs and strata.
#' 
#' @return 
#' A data.frame with the analysis details.
#' 
#' @export
getAnalysisDetails <- function()
{
  pathToCsv <- system.file("csv", "analysisDetails.csv", package = "Achilles")
  analysisDetails <- read.csv(pathToCsv)
  return(analysisDetails)
}


#' Drop all the scratch tables
#' 
#' @details 
#' Drop all possible Achilles and Heel scratch tables
#' 
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param scratchDatabaseSchema            string name of database schema that Achilles scratch tables were written to. 
#' @param tempAchillesPrefix               (optional) The prefix to use for the "temporary" (but actually permanent) Achilles analyses tables. Default is "tmpach"
#' @param tempHeelPrefix                   (optional) The prefix to use for the "temporary" (but actually permanent) Heel tables. Default is "tmpheel"
#' @param numThreads                       (optional) Only for v5.x: The number of threads to use to run Achilles in parallel. Default is 1 thread.
#' 
#' 
#' @export
dropAllScratchTables <- function(connectionDetails, scratchDatabaseSchema, 
                               tempAchillesPrefix = "tmpach", 
                               tempHeelPrefix = "tmpheel", numThreads = 1)
{
  # Drop Achilles Scratch Tables ------------------------------------------------------
  
  inputFolder <- paste("v5", "analyses", sep = "/")
  analysisDetails <- getAnalysisDetails()
  detailTables <- .getDetailTables(tempAchillesPrefix = tempAchillesPrefix, analysisDetails = analysisDetails)
  
  cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
  dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = analysisDetails$ANALYSIS_ID, 
                                     fun = .dropAchillesScratchTable,
                                     connectionDetails = connectionDetails,
                                     scratchDatabaseSchema = scratchDatabaseSchema,
                                     tempAchillesPrefix = tempAchillesPrefix,
                                     detailTables = detailTables,
                                     sqlOnly = FALSE)
  
  OhdsiRTools::stopCluster(cluster)
  
  # Drop Heel Scratch Tables ------------------------------------------------------

  inputFolder <- paste("v5", "heels", sep = "/")
  
  heelFiles <- list.files(path = paste(system.file(package = "Achilles"), 
                                       "sql/sql_server", inputFolder, "independents", sep = "/"), 
                          recursive = TRUE, 
                          full.names = TRUE, 
                          all.files = FALSE,
                          pattern = "\\.sql$")
  
  cluster <- OhdsiRTools::makeCluster(numberOfThreads = numThreads)
  
  dummy <- OhdsiRTools::clusterApply(cluster = cluster, x = heelFiles, 
                                     fun = .dropHeelScratchTable,
                                     connectionDetails = connectionDetails,
                                     scratchDatabaseSchema = scratchDatabaseSchema,
                                     tempHeelPrefix = tempHeelPrefix,
                                     sqlOnly = FALSE)
  
  OhdsiRTools::stopCluster(cluster)
}



#' Drop Achilles Scratch Table
#' 
#' @details 
#' Drops a scratch Achilles table
#' 
#' @param analysisId                       The id of the analysis table to drop
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param scratchDatabaseSchema            string name of database schema that Achilles scratch tables were written to.
#' @param tempAchillesPrefix               (optional) The prefix to use for the scratch Achilles analyses tables. Default is "tmpach"
#' @param details                          A list of Achilles detail tables, as built in \code{\link{.getDetailTables}}
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' 
#' 
.dropAchillesScratchTable <- function(analysisId,
                                     connectionDetails, 
                                     scratchDatabaseSchema, 
                                     tempAchillesPrefix, 
                                     detailTables,
                                     sqlOnly)
{
  for (detailTable in detailTables)
  {
    if (analysisId %in% detailTable$analysisIds)
    {
      sql <- SqlRender::renderSql(sql = "IF OBJECT_ID('@scratchDatabaseSchema.@tablePrefix_@analysisId', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema.@tablePrefix_@analysisId;",
                                  tablePrefix = detailTable$tablePrefix,
                                  scratchDatabaseSchema = scratchDatabaseSchema, 
                                  analysisId = analysisId)$sql
      sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
      
      if (!sqlOnly)
      {
        connection <- DatabaseConnector::connect(connectionDetails)
        DatabaseConnector::executeSql(connection = connection, sql = sql)
        DatabaseConnector::disconnect(connection)
      }
    }
  }
}

#' Drop Heel Scratch Table
#' 
#' @details 
#' Drops a scratch Heel table
#' 
#' @param heelFile                         The fully qualified file path of a heel table to drop
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param scratchDatabaseSchema            string name of database schema that Achilles scratch tables were written to. 
#' @param tempHeelPrefix                   (optional) The prefix to use for the scratch Heel tables. Default is "tmpheel"
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' 
#' 
.dropHeelScratchTable <- function(heelFile,
                                 connectionDetails, 
                                 scratchDatabaseSchema, 
                                 tempHeelPrefix, 
                                 sqlOnly)
{
  sql <- SqlRender::renderSql(sql = "IF OBJECT_ID('@scratchDatabaseSchema.@tempHeelPrefix_@heelName', 'U') IS NOT NULL DROP TABLE @scratchDatabaseSchema.@tempHeelPrefix_@heelName;",
                              scratchDatabaseSchema = scratchDatabaseSchema, 
                              tempHeelPrefix = tempHeelPrefix,
                              heelName = gsub(pattern = ".sql", replacement = "", x = basename(heelFile)))$sql
  sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
  
  if (!sqlOnly)
  {
    connection <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(connection = connection, sql = sql)
    DatabaseConnector::disconnect(connection)
  }
}


#' Run an Achilles Id's analysis
#'
#' @details
#' Runs an Achilles Id's analysis
#' 
#' @param analysisId                       The ID of the Achilles analysis to run
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param connection                       A connection object (if this is running in serial mode; otherwise, NULL)
#' @param schemaDelim                      The schema delimiter. If single-threaded mode, this will be set to a string prefix. If multi-threaded mode, this will be a "."
#' @param cdmDatabaseSchema    	           string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param cdmVersion                       Define the OMOP CDM version used:  currently supports v5 and above.  Default = "5". v4 support is in \code{\link{achilles_v4}}.
#' @param scratchDatabaseSchema            (only affects multi-threaded mode) Name of a fully qualified schema that is accessible to/from the resultsDatabaseSchema, that can store all of the scratch tables. Default is resultsDatabaseSchema.
#' @param tempAchillesPrefix               (optional) The prefix to use for the scratch Achilles analyses tables. Default is "tmpach"
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' 
.runAchillesAnalysisId <- function(analysisId, 
                                  connectionDetails,
                                  connection,
                                  schemaDelim,
                                  cdmDatabaseSchema,
                                  cdmVersion,
                                  scratchDatabaseSchema,
                                  tempAchillesPrefix, 
                                  detailTables,
                                  sourceName,
                                  sqlOnly)
{
  inputFolder <- paste("v5", "analyses", sep = "/")
  outputFolder <- paste("output", paste0("v", cdmVersion), sep = "/")
  
  if (scratchDatabaseSchema != "#")
  {
    .dropAchillesScratchTable(analysisId = analysisId,
                          connectionDetails = connectionDetails, 
                          scratchDatabaseSchema = scratchDatabaseSchema, 
                          tempAchillesPrefix = tempAchillesPrefix, 
                          detailTables = detailTables,
                          sqlOnly = sqlOnly)
  }
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, paste(analysisId, "sql", sep = "."), sep = "/"),
                                           packageName = "Achilles",
                                           dbms = connectionDetails$dbms,
                                           scratchDatabaseSchema = scratchDatabaseSchema,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           schemaDelim = schemaDelim,
                                           tempAchillesPrefix = tempAchillesPrefix,
                                           source_name = sourceName,
                                           achilles_version = packageVersion(pkg = "Achilles"),
                                           cdmVersion = cdmVersion,
                                           singleThreaded = (scratchDatabaseSchema == "#"))
  
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, 
                        targetFile = 
                          paste(outputFolder, 
                                paste(paste("analysis", analysisId, sep = "_"), "sql", sep = "."), sep = "/"))
  }
  else
  {
    if (is.null(connection))
    {
      connection <- DatabaseConnector::connect(connectionDetails)
      DatabaseConnector::executeSql(connection = connection, sql = sql)
      DatabaseConnector::disconnect(connection)
    }
    else # connection is already alive
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql)
    }
  }
}


#' Merge Achilles Scratch Tables
#'
#' @details
#' Merges Achilles scratch tables using pre-defined schemas
#' 
#' @param detailTable                      A detailTable object as constructed by \code{.getDetailTables}
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param connection                       A connection object (if this is running in serial mode; otherwise, NULL)
#' @param schemaDelim                      The schema delimiter. If single-threaded mode, this will be set to a string prefix. If multi-threaded mode, this will be a "."
#' @param scratchDatabaseSchema            (only affects multi-threaded mode) Name of a fully qualified schema that is accessible to/from the resultsDatabaseSchema, that can store all of the scratch tables. Default is resultsDatabaseSchema.
#' @param resultsDatabaseSchema		         string name of database schema that we can write final results to. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, 
#'                                         so for example 'results.dbo'.
#' @param cdmVersion                       Define the OMOP CDM version used:  currently supports v5 and above.  Default = "5". v4 support is in \code{\link{achilles_v4}}.
#' @param tempAchillesPrefix               (optional) The prefix to use for the scratch Achilles analyses tables. Default is "tmpach"
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param smallCellCount                   To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.
.mergeAchillesScratchTables <- function(detailTable, 
                                       connectionDetails,
                                       connection,
                                       schemaDelim,
                                       scratchDatabaseSchema,
                                       resultsDatabaseSchema, 
                                       cdmVersion,
                                       tempAchillesPrefix,
                                       sqlOnly,
                                       smallCellCount = 5)
{
  inputFolder <- paste("v5", "analyses", sep = "/")
  outputFolder <- paste("output", paste0("v", cdmVersion), sep = "/")
  
  castedNames <- apply(detailTable$schema, 1, function(field)
  {
    SqlRender::renderSql("cast(@fieldName as @fieldType) as @fieldName", 
                         fieldName = field["FIELD_NAME"],
                         fieldType = field["FIELD_TYPE"])$sql
  })
  detailSqls <- lapply(detailTable$analysisIds, function(analysisId)
  {
    sql <- SqlRender::renderSql(sql = "select @castedNames from @scratchDatabaseSchema@schemaDelim@tablePrefix_@analysisId", 
                                scratchDatabaseSchema = scratchDatabaseSchema,
                                schemaDelim = schemaDelim,
                                castedNames = paste(castedNames, collapse = ", "), 
                                tablePrefix = detailTable$tablePrefix, 
                                analysisId = analysisId)$sql
    
    sql <- SqlRender::translateSql(sql = sql, targetDialect = connectionDetails$dbms)$sql
  })
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste(inputFolder, "MergeAchillesTables.sql", sep = "/"),
                                           packageName = "Achilles",
                                           dbms = connectionDetails$dbms,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           detailType = detailTable$detailType,
                                           detailSqls = paste(detailSqls, collapse = " \nunion all\n "),
                                           fieldNames = paste(detailTable$schema$FIELD_NAME, collapse = ", "),
                                           smallCellCount = smallCellCount)
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, 
                        targetFile = paste(outputFolder, 
                                           paste(paste("Merge", detailTable$detailType, sep = "_"), "sql", sep = "."), sep = "/"))
  }
  else
  {
    if (is.null(connection))
    {
      connection <- DatabaseConnector::connect(connectionDetails)
      DatabaseConnector::executeSql(connection = connection, sql = sql)
      DatabaseConnector::disconnect(connection)
    }
    else # connection is already alive
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql)
    }
  }
}

#' Run a Heel Id's query
#'
#' @details
#' Runs a Heel Id's query
#' 
#' @param heelFile                         An Achilles Heel file to run
#' @param connectionDetails                An R object of type ConnectionDetail (details for the function that contains server info, database type, optionally username/password, port)
#' @param connection                       A connection object (if this is running in serial mode; otherwise, NULL)
#' @param cdmDatabaseSchema    	           string name of database schema that contains OMOP CDM. On SQL Server, this should specifiy both the database and the schema, so for example 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema	           string name of database schema that contains finished Achilles results. 
#' @param scratchDatabaseSchema            string name of database schema that will hold the scratch Heel tables.
#' @param schemaDelim                      The schema delimiter. If single-threaded mode, this will be set to a string prefix. If multi-threaded mode, this will be a "."
#' @param tempHeelPrefix                   (optional) The prefix to use for the scratch Heel tables. Default is "tmpheel"
#' @param sqlOnly                          TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param inputFolder                      Folder where the analysis scripts sit
#' @param outputFolder                     Folder where to write SQL scripts to, if running in sqlOnly mode
#' 
.runHeelId <- function(heelFile, 
                      connectionDetails, 
                      connection,
                      cdmDatabaseSchema,
                      resultsDatabaseSchema,
                      scratchDatabaseSchema,
                      schemaDelim,
                      tempHeelPrefix, 
                      sqlOnly, 
                      inputFolder,
                      outputFolder)
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = gsub(pattern = 
                                                                paste(system.file(package = "Achilles"), 
                                                                              "sql/sql_server/", sep = "/"), 
                                                              replacement = "", x = heelFile),
                                           packageName = "Achilles",
                                           dbms = connectionDetails$dbms,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           scratchDatabaseSchema = scratchDatabaseSchema,
                                           schemaDelim = schemaDelim,
                                           tempHeelPrefix = tempHeelPrefix,
                                           heelName = gsub(pattern = ".sql", replacement = "", x = basename(heelFile)))
  
  if (sqlOnly)
  {
    SqlRender::writeSql(sql = sql, targetFile = paste(outputFolder, basename(heelFile), sep = "/"))
  }
  else
  {
    if (scratchDatabaseSchema != "#")
    {
      connection <- DatabaseConnector::connect(connectionDetails)
      DatabaseConnector::executeSql(connection = connection, sql = sql)
      DatabaseConnector::disconnect(connection)
    }
    else # connection is already alive
    {
      DatabaseConnector::executeSql(connection = connection, sql = sql)
    }
  }
}


#' Get Domain Table
#'
#' @details
#' Get a table name based on domain
#' 
#' @param domainId                  A Domain ID from the OMOP Domain table
#' @return                          Domain Table Name 
.getDomainTable <- function(domainId)
{
  domainId <- tolower(domainId)
  
  if (domainId == "condition")
  {
    return ("condition_occurrence")
  }
  if (domainId == "drug")
  {
    return ("drug_exposure")
  }
  if (domainId == "procedure")
  {
    return ("procedure_occurrence")
  }
  if (domainId == "device")
  {
    return ("device_exposure")
  }
  if (domainId == "measurement")
  {
    return ("measurement")
  }
  if (domainId == "observation")
  {
    return ("observation")
  }
  if (domainId == "procedure")
  {
    return ("procedure_occurrence")
  }
  if (domainId == "specimen")
  {
    return ("specimen")
  }
}

#' Get detail tables for Achilles
#' 
#' @details 
#' Get detail tables for Achilles; these will dictate the fields and analysis_ids per achilles table
#' 
#' @param tempAchillesPrefix          The prefix to use for the scratch Achilles analyses tables. Default is "tmpach"
#' @param analysisDetails             A data.frame with the analysis details.
#' 
#' @return 
#' A list of detail tables
#' 
.getDetailTables <- function(tempAchillesPrefix = "tmpach", analysisDetails)
{
  buildDetailTable <- function(detailType, tablePrefix, schema, analysisIds)
  {
    detailTable <- {}
    detailTable$detailType <- detailType
    detailTable$tablePrefix <- tablePrefix
    detailTable$schema <- schema
    detailTable$analysisIds <- analysisIds
    return(detailTable)
  }
  
  detailTables <- list(buildDetailTable(detailType = "results",
                                        tablePrefix = tempAchillesPrefix, 
                                        schema = read.csv(file = system.file("csv", "schema_achilles_results.csv", package = "Achilles"), 
                                                          header = TRUE),
                                        analysisIds = analysisDetails[analysisDetails$DISTRIBUTION == 0 | analysisDetails$DISTRIBUTION == 2, ]$ANALYSIS_ID), 
                       
                       buildDetailTable(detailType = "results_dist",
                                        tablePrefix = paste(tempAchillesPrefix, "dist", sep = "_"), 
                                        schema = read.csv(file = system.file("csv", "schema_achilles_results_dist.csv", package = "Achilles"), 
                                                          header = TRUE),
                                        analysisIds = analysisDetails[analysisDetails$DISTRIBUTION > 0, ]$ANALYSIS_ID))
  return(detailTables)
}
