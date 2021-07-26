library(ROhdsiWebApi)
library(dplyr)
library(tidyr)
library(here)
library(readr)


baseurl<-Sys.getenv("DB_baseurl")
webApiUsername<-Sys.getenv("DB_webApiUsername")
webApiPassword<-Sys.getenv("DB_webApiPassword")

authorizeWebApi(baseurl,
                authMethod="ad",
                webApiUsername = webApiUsername,
                webApiPassword = webApiPassword)
ROhdsiWebApi::insertCohortDefinitionSetInPackage(
  fileName = "ExposureCohorts/CohortsToCreate.csv", 
  baseurl, 
  jsonFolder = "ExposureCohorts/json", sqlFolder = "ExposureCohorts/sql", 
  rFileName = "R/CreateCohorts.R", insertTableSql = TRUE, 
  insertCohortCreationR = FALSE, generateStats = FALSE, packageName) 

