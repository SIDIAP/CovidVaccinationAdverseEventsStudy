

#install.packages("renv") # if not already installed, install renv from CRAN
# renv::restore() # this should prompt you to install the various packages required for the study
# renv::activate()

# packages
library(SqlRender)
library(DatabaseConnector)
library(FeatureExtraction)
library(here)
library(lubridate)
library(stringr)
library(ggplot2)
library(DBI)
library(dbplyr)
library(dplyr)
library(tidyr)
library(kableExtra)
library(RSQLite)
library(rmarkdown)
library(tableone)
library(scales)
library(forcats)
library(epiR)
library(RPostgreSQL)
# please load the above packages 
# you should have them all available, with the required version, after
# having run renv::restore above

output.folder<-here::here("output")
# the path to a folder (that exists) where the results from this analysis will be saved

oracleTempSchema<-NULL

# If you haven´t already, save database details to .Renviron by running:
# usethis::edit_r_environ()

server<-"10.80.192.22/postgres20v3fwd"
server_dbi<-"postgres20v3fwd"

user<-Sys.getenv("DB_USER")
password<- Sys.getenv("DB_PASSWORD")
port<-Sys.getenv("DB_PORT") 
host<-Sys.getenv("DB_HOST") 



connectionDetails <-DatabaseConnector::downloadJdbcDrivers("postgresql", here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server =server,
                                                                user = user,
                                                                password = password,
                                                                port = port ,
                                                                pathToDriver = here::here())
db <- dbConnect(RPostgreSQL::PostgreSQL(),
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)



# sql dialect used with the OHDSI SqlRender package
targetDialect <-"postgresql" 
# schema that contains the OMOP CDM with patient-level data
cdm_database_schema<-"omop20v3fwd"
# schema that contains the vocabularie
vocabulary_database_schema<-"omop20v3fwd" 
# schema where a results table will be created 
results_database_schema<-"results20v3fwd"

# Tables to be created in your results schema for this analysis
# You can keep the above names or change them
# Note, any existing tables in your results schema with the same name will be overwritten
cohortTableExposures<-"CovVaxExposures"
cohortTableOutcomes <-"CovVaxOutcomes"
cohortTableProfiles<-"CovVaxProfiles"

db.name<-"SIDIAP"
# This is the name/ acronym for your database (to be used in the titles of reports, etc) 

create.outcome.cohorts<-FALSE
# if you have already created the outcome cohorts, you can set this to FALSE to skip instantiating these cohorts again

run.vax.cohorts<-TRUE
run.covid.cohorts<-TRUE
run.general.pop.cohorts<-TRUE

# run the analysis
start<-Sys.time()
source(here("RunAnalysis.R"))
Sys.time()-start





