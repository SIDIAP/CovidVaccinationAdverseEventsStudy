library(ROhdsiWebApi)
library(dplyr)
library(tidyr)
library(here)
library(readr)


baseurl<-Sys.getenv("DB_baseurl")
webApiUsername<-Sys.getenv("DB_webApiUsername")
webApiPassword<-Sys.getenv("DB_webApiPassword")

# Vaccinated cohorts
authorizeWebApi(baseurl,
                authMethod="ad",
                webApiUsername = webApiUsername,
                webApiPassword = webApiPassword)
ROhdsiWebApi::insertCohortDefinitionSetInPackage(
  fileName = "VaccinatedCohorts/CohortsToCreate.csv", 
  baseurl, 
  jsonFolder = "VaccinatedCohorts/json", sqlFolder = "VaccinatedCohorts/sql", 
  rFileName = "R/CreateCohorts.R", insertTableSql = TRUE, 
  insertCohortCreationR = FALSE, generateStats = FALSE, packageName) 

# Covid cohorts
authorizeWebApi(baseurl,
                authMethod="ad",
                webApiUsername = webApiUsername,
                webApiPassword = webApiPassword)
ROhdsiWebApi::insertCohortDefinitionSetInPackage(
  fileName = "CovidCohorts/CohortsToCreate.csv", 
  baseurl, 
  jsonFolder = "CovidCohorts/json", sqlFolder = "CovidCohorts/sql", 
  rFileName = "R/CreateCohorts.R", insertTableSql = TRUE, 
  insertCohortCreationR = FALSE, generateStats = FALSE, packageName) 

# General population cohorts
authorizeWebApi(baseurl,
                authMethod="ad",
                webApiUsername = webApiUsername,
                webApiPassword = webApiPassword)
ROhdsiWebApi::insertCohortDefinitionSetInPackage(
  fileName = "GeneralPopCohorts/CohortsToCreate.csv", 
  baseurl, 
  jsonFolder = "GeneralPopCohorts/json", sqlFolder = "GeneralPopCohorts/sql", 
  rFileName = "R/CreateCohorts.R", insertTableSql = TRUE, 
  insertCohortCreationR = FALSE, generateStats = FALSE, packageName) 
