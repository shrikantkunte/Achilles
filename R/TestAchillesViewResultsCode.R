#Requires that Achilles has been run first

testAchillesViestResultsCode <- function(){
  #Test on SQL Server: 
  setwd("c:/temp")
  connectionDetailsSqlServer <- createConnectionDetails(dbms="sql server", server="some_sql_server")
  fetchAchillesHeelResults(connectionDetailsSqlServer, resultsDatabaseSchema="cdm_instance_v5.results")
  fetchAchillesAnalysisResults(connectionDetailsSqlServer, resultsDatabaseSchema = "cdm_instance_v5.results", analysisId = 106)
  
  
  pw <- ""
  
  ### Test Achilles heel part ###
  
  #Test on SQL Server
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="sql server", server="some_sql_server")
  fetchAchillesHeelResults(connectionDetails,  resultsDatabaseSchema = "cdm_instance_v5.results")

  #Test on PostgreSQL
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="postgresql", server="localhost/ohdsi", user="postgres",password=pw)
  fetchAchillesHeelResults(connectionDetails, resultsDatabaseSchema = "scratch")

  

  #Test on Oracle 
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="oracle", server="xe", user="system",password="OHDSI2")
  fetchAchillesHeelResults(connectionDetails, resultsDatabaseSchema = "scratch")

  
  ### Test Achilles analysis results view part ###
  #Test on SQL Server
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="sql server", server="some_sql_server")
  fetchAchillesAnalysisResults(connectionDetails,  resultsDatabaseSchema = "cdm_instance_v5.results", analysisId = 106)
  
  #Test on PostgreSQL
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="postgresql", server="localhost/ohdsi", user="postgres",password=pw)
  fetchAchillesAnalysisResults(connectionDetails, resultsDatabaseSchema = "scratch", analysisId = 106)
  
  

  #Test on Oracle
  setwd("c:/temp")
  connectionDetails <- createConnectionDetails(dbms="oracle", server="xe", user="system",password="OHDSI2")
  fetchAchillesAnalysisResults(connectionDetails, resultsDatabaseSchema = "scratch", analysisId = 106)
  
  
  connectionDetails <- createConnectionDetails(dbms="oracle", server="xe", user="system",password=pw)
  for (analysisId in analysesDetails$ANALYSIS_ID){
    results <- fetchAchillesAnalysisResults(connectionDetails, resultsDatabaseSchema = "scratch", analysisId = analysisId)
  }
}