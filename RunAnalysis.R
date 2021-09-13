source(here("Functions", "Functions.R"))

# link to db tables -----
person_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".person")))
observation_period_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".observation_period")))
observation_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".observation")))
visit_occurrence_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".visit_occurrence")))
condition_occurrence_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".condition_occurrence")))
drug_exposure_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".drug_exposure")))
drug_era_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".drug_era")))
concept_ancestor_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       vocabulary_database_schema,
                       ".concept_ancestor")))
concept_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       vocabulary_database_schema,
                       ".concept")))


# specifications ----
cond.codes<-c("434621", 
                  "4098292", 
                  "4125650",  
                  "317009",   
                  "313217",   
                  "443392", 
                  "201820",
                  "433736",
                  "321588",
                  "316866",
                  "4030518",
                  "255573",
                  "4182210")
cond.names<-c("autoimmune_disease",
              "antiphospholipid_syndrome",
              "thrombophilia",
              "asthma",
              "atrial_fibrillation",
              "malignant_neoplastic_disease",
              "diabetes_mellitus",
              "obesity",
              "heart_disease",
              "hypertensive_disorder",
              "renal_impairment",
              "copd",
              "dementia")
# these are the conditions we´ll extract for our table 1

drug.codes<-c("21603933", 
                  "21603991",
                  "21602722",
                  "21600961",
                  "21601853",
                  "21601386",
                  "21602472",
                  "21603831",
                  "21602471")
drug.names<-c("antiinflamatory_and_antirheumatic", 
                  "coxibs",
                  "corticosteroids",
                  "antithrombotic",
                  "lipid_modifying",
                  "antineoplastic_immunomodulating",
                  "hormonal_contraceptives",
                  "tamoxifen",
                  "sex_hormones_modulators")
# these are the medications we´ll extract for our table 1

# instantiate outcome tables -----
cohort.sql<-list.files(here("OutcomeCohorts", "sql"))
cohort.sql<-cohort.sql[cohort.sql!="CreateCohortTable.sql"]
outcome.cohorts<-tibble(id=1:length(cohort.sql),
                        file=cohort.sql,
                        name=str_replace(cohort.sql, ".sql", ""))  
if(create.outcome.cohorts=="FALSE"){
print(paste0("- Skipping creating outcome cohorts"))
} else { 
print(paste0("- Getting outcomes"))

  
conn <- connect(connectionDetails)
# create empty cohorts table
print(paste0("Create empty cohort table")) 
sql<-readSql(here("OutcomeCohorts", "sql","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                          sql,
                          cohort_database_schema =  results_database_schema,
                          cohort_table = cohortTableOutcomes)
rm(sql)

for(cohort.i in 1:length(outcome.cohorts$id)){
  
  working.id<-outcome.cohorts$id[cohort.i]

  print(paste0("- Getting outcome: ", 
               outcome.cohorts$name[cohort.i],
              " (", cohort.i, " of ", length(outcome.cohorts$name), ")"))
  sql<-readSql(here("OutcomeCohorts", "sql",
                    outcome.cohorts$file[cohort.i])) 
  sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
  
  sql<-SqlRender::translate(sql, targetDialect = targetDialect)
  renderTranslateExecuteSql(conn=conn, 
                          sql, 
                          cdm_database_schema = cdm_database_schema,
                          vocabulary_database_schema = vocabulary_database_schema,
                          target_database_schema = results_database_schema,
                          results_database_schema = results_database_schema,
                          target_cohort_table = cohortTableOutcomes,
                          target_cohort_id = working.id)  
  }
disconnect(conn)
} 
# link 
outcome_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableOutcomes)))    

# drop any outcome cohorts with less than 5 people
outcome.cohorts<-outcome.cohorts %>% 
  inner_join(outcome_db %>% 
  group_by(cohort_definition_id) %>% 
  tally() %>% 
  collect() %>% 
  filter(n>5) %>% 
  select(cohort_definition_id),
  by=c("id"="cohort_definition_id"))  
# all those instantiated from outcome diagnostics

# specify which events we´ll also combine with thrombocytopenia ------
outcome.cohorts.thromb.10_10<-outcome.cohorts %>% 
  filter(name %in% 
         c("all stroke","CVST","DIC",
           "DVT broad","DVT narrow",
           "VTE broad", "VTE narrow",
           "PE", "hem stroke",
           "hepatic vein" ,"intest infarc",
           "isc stroke", "IVT", "MACE",
           "MI isc stroke", "PE",
           "portal vein","splenic infarc",
           "SVT" , "visc venous","VTE broad" ,"VTE narrow", 
            "splenic vein", "splenic artery",
            "splenic infarc", "hepatic artery", 
                    "intest infarc",
                     "mesen vein", "CAT"))
outcome.cohorts.thromb.10_10$name<-paste0(outcome.cohorts.thromb.10_10$name, " (with thrombocytopenia 10 days pre to 10 days post)")

outcome.cohorts.thromb.42_14<-outcome.cohorts %>% 
  filter(name %in% 
           c("all stroke","CVST","DIC",
           "DVT broad","DVT narrow",
           "VTE broad", "VTE narrow",
           "PE", "hem stroke",
           "hepatic vein" ,"intest infarc",
           "isc stroke", "IVT", "MACE",
           "MI isc stroke", "PE",
           "portal vein","splenic infarc",
           "SVT" , "visc venous","VTE broad" ,"VTE narrow", 
            "splenic vein", "splenic artery",
            "splenic infarc", "hepatic artery", 
                    "intest infarc",
                     "mesen vein", "CAT"))
outcome.cohorts.thromb.42_14$name<-paste0(outcome.cohorts.thromb.42_14$name, " (with thrombocytopenia 42 days pre to 14 days post)")

# add to outcomes
outcome.cohorts<-bind_rows(outcome.cohorts,
outcome.cohorts.thromb.10_10,
outcome.cohorts.thromb.42_14)



# run analysis ----
# Initiate lists to store output
Patient.characteristcis<-list() # to collect tables of characteristics
IR.summary<-list() # to collect incidence rate summaries
Survival.summary<-list() # to collect incidence

# get study cohorts
study.cohorts<-list()

if(run.vax.cohorts=="TRUE"){
study.cohorts[["vaccinated"]]<-tibble(id=1:6,
                        file=NA,
                        name=c("BNT162b2 first-dose", 
                               "BNT162b2 second-dose", 
                               "ChAdOx1 first-dose",
                               "BNT162b2 first-dose (no prior covid)", 
                               "BNT162b2 second-dose (no prior covid)", 
                               "ChAdOx1 first-dose (no prior covid)")) %>% 
  mutate(type="VaccinatedCohorts")
}

if(run.covid.cohorts=="TRUE"){
study.cohorts[["covid"]]<-tibble(id=1,
                        file=NA,
                        name=c("COVID-19 diagnosis")) %>% 
  mutate(type="CovidCohorts")
}

if(run.general.pop.cohorts=="TRUE"){
  study.cohorts.sql.general.pop<-list.files(here("GeneralPopCohorts", "sql"))
  study.cohorts.sql.general.pop<-study.cohorts.sql.general.pop[study.cohorts.sql.general.pop!="CreateCohortTable.sql"]
  study.cohorts[["general.pop"]]<-tibble(id=1:length(study.cohorts.sql.general.pop),
                                   file=study.cohorts.sql.general.pop,
                                   name=str_replace(study.cohorts.sql.general.pop, ".sql", ""))%>% 
    mutate(type="GeneralPopCohorts")
  
   study.cohorts[["general.pop"]]<-head(study.cohorts[["general.pop"]],1)
}

study.cohorts<- bind_rows(study.cohorts) %>% 
  mutate(id=1:nrow(.))

for(i in 1:length(study.cohorts$id)){ 
# study population -----
working.study.cohort<-  study.cohorts %>% filter(id==i) %>% select(name) %>% pull()
working.study.cohort.type<-  study.cohorts %>% filter(id==i) %>% select(type) %>% pull()
print(paste0("Running analysis for ", working.study.cohort))  
  
# define our study population using the sql in the Cohorts folder
# the accompanying folder has the jsons for reference (which can be imported into Atlas)
  
# The population are identified and added to the cohortTableExposures table in the db
  
# connect to database
conn <- connect(connectionDetails)
# create empty cohorts table
print(paste0("Create empty cohort table")) 
sql<-readSql(here("VaccinatedCohorts", "sql","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                          sql,
                          cohort_database_schema =  results_database_schema,
                          cohort_table = cohortTableExposures)
rm(sql)

# get current exposure population
if(working.study.cohort.type=="VaccinatedCohorts"){

  if(str_detect(working.study.cohort, "BNT162b2")){
    cohort.to.instantiate<-drug_exposure_db %>% 
      filter(drug_concept_id=="37003436") %>% 
      collect()
  } 
  
    if(str_detect(working.study.cohort, "ChAdOx1")){
    cohort.to.instantiate<-drug_exposure_db %>% 
      filter(drug_concept_id=="724905") %>% 
      collect()
    } 
  
cohort.to.instantiate<-cohort.to.instantiate %>% 
      group_by(person_id)%>% 
      arrange(person_id,drug_exposure_start_date) %>% 
      mutate(seq=1:length(person_id))
    
  # get first or second dose depending on working cohort
if(str_detect(working.study.cohort, "first-dose")){
  # nb keep date of second dose so we can use it for censoring
  
cohort.to.instantiate<-cohort.to.instantiate %>% 
      filter(drug_exposure_start_date>=as.Date("27/12/2021", "%d/%m/%y")) %>% 
       select(person_id, drug_exposure_start_date) %>% 
       distinct()%>% # drop any duplicates from same day
      arrange(person_id,drug_exposure_start_date) %>% 
      group_by(person_id) %>% 
      mutate(seq=1:length(person_id))

     # keep first two records
     cohort.to.instantiate<-cohort.to.instantiate  %>% 
      group_by(person_id)%>% 
      mutate(seq.max=max(seq)) %>% 
      filter(seq %in% c(1,2) )
     
     # get time between doses
     cohort.to.instantiate<-cohort.to.instantiate %>% 
      group_by(person_id)%>% 
      mutate(first.dose.date=lead(drug_exposure_start_date,1)) %>% # lead - second dose
      mutate(dose1_dose2=as.numeric(difftime(drug_exposure_start_date,
                                               first.dose.date, 
                                        units="days")))

     # keep first record
      cohort.to.instantiate<-cohort.to.instantiate  %>% 
      filter(seq==1) 
     # quantile(cohort.to.instantiate$dose1_dose2, na.rm=TRUE)
      
  }
  
if(str_detect(working.study.cohort, "second-dose")){
     cohort.to.instantiate<-cohort.to.instantiate %>% 
      filter(drug_exposure_start_date>=as.Date("27/12/2021", "%d/%m/%y")) %>% 
       select(person_id, drug_exposure_start_date) %>% 
       distinct()%>% # drop any duplicates from same day
      arrange(person_id,drug_exposure_start_date) %>% 
      group_by(person_id) %>% 
      mutate(seq=1:length(person_id))
     
     # table(cohort.to.instantiate$seq)
     
     # keep first two records
     cohort.to.instantiate<-cohort.to.instantiate  %>% 
      group_by(person_id)%>% 
      mutate(seq.max=max(seq)) %>% 
      filter(seq.max>=2) %>% 
      filter(seq %in% c(1,2) )
     
     # get time between doses
     cohort.to.instantiate<-cohort.to.instantiate %>% 
      group_by(person_id)%>% 
      mutate(first.dose.date=lag(drug_exposure_start_date,1)) %>%
      mutate(dose1_dose2=as.numeric(difftime(drug_exposure_start_date,
                                               first.dose.date, 
                                        units="days")))
     # keep second record
      cohort.to.instantiate<-cohort.to.instantiate  %>% 
      filter(seq==2) 
     
     # quantile(cohort.to.instantiate$dose1_dose2)
     
   
      #3) exclude if second dose record is less than 18 days after the first
     # sum(cohort.to.instantiate$dose1_dose2<18)
     # sum(cohort.to.instantiate$dose1_dose2>60)
if(str_detect(working.study.cohort, "BNT162b2 second-dose")){      
       cohort.to.instantiate<-cohort.to.instantiate  %>% 
      filter(dose1_dose2>=18) %>% 
      filter(dose1_dose2<=28) 
      
     # prop.table(table(cohort.to.instantiate$dose1_dose2==21))
     # prop.table(table(cohort.to.instantiate$dose1_dose2<21))
     # prop.table(table(cohort.to.instantiate$dose1_dose2>21))
       
       }
if(str_detect(working.study.cohort, "mRNA-1273 second-dose")){      
       cohort.to.instantiate<-cohort.to.instantiate  %>% 
      filter(dose1_dose2>=25) %>% 
      filter(dose1_dose2<=35)   
       
       }      
      
     
     
   }


  
  # exlude if prior covid
  
  if(str_detect(working.study.cohort, "no prior covid")){
  
  # covid diagnosis
covid.diagnosis.codes<-bind_rows(
# codes with descendants
concept_ancestor_db %>%
  filter(ancestor_concept_id  %in% 
           c(37311060,37311061)) %>% 
  select(descendant_concept_id)  %>% 
  select(descendant_concept_id) %>% 
  rename(concept_id=descendant_concept_id)%>% 
  left_join(concept_db) %>% 
  select(concept_id,concept_name, standard_concept,domain_id ) %>% 
  collect() ,
# codes without descendants
concept_db %>% 
  filter(concept_id %in% 
     c(37396171,320651,40479642,
       37016927,4100065,439676 ))%>% 
  select(concept_id,concept_name, standard_concept,domain_id ) %>% 
  collect() ) %>% 
  mutate(cohort="Covid diagnosis")     
working.codes<-covid.diagnosis.codes %>% select(concept_id) %>% pull()  

covid<-condition_occurrence_db  %>% 
      filter(condition_concept_id %in% working.codes) %>% 
      collect()
covid<-covid %>% 
     arrange(person_id, condition_start_date) %>% 
     group_by(person_id) %>% 
     mutate(seq=1:length(person_id)) %>% 
     filter(seq==1) # persons first covid diagnosis
  
# drop if covid prior to vaccinated
covid <-cohort.to.instantiate %>% 
  select(person_id,drug_exposure_start_date) %>% 
  inner_join(covid %>% 
            select(person_id,condition_start_date)) %>% 
  filter(drug_exposure_start_date>=condition_start_date)

cohort.to.instantiate<-cohort.to.instantiate %>% 
  anti_join(covid)

  }
  
  
    # min(cohort.to.instantiate$drug_exposure_start_date)
    # max(cohort.to.instantiate$drug_exposure_start_date)
    # cohort.to.instantiate %>% 
    #   ggplot()+
    #   geom_histogram(aes(drug_exposure_start_date), binwidth = 1)
    
    cohort.to.instantiate<-cohort.to.instantiate %>% 
      mutate(cohort_definition_id=study.cohorts$id[i]) %>% 
      rename("subject_id"="person_id") %>% 
      rename("cohort_start_date"="drug_exposure_start_date")%>% 
      mutate("cohort_end_date"=as.Date("26/05/2021", "%d/%m/%y")) %>% 
      select(cohort_definition_id, subject_id, cohort_start_date,cohort_end_date, dose1_dose2)

insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableExposures),
            data=cohort.to.instantiate,
            createTable = FALSE,
            progressBar=TRUE)
    
    
 
  
} else if (working.study.cohort.type=="CovidCohorts"){

if(str_detect(working.study.cohort, "COVID-19 diagnosis")){

# covid diagnosis
covid.diagnosis.codes<-bind_rows(
# codes with descendants
concept_ancestor_db %>%
  filter(ancestor_concept_id  %in% 
           c(37311060,37311061)) %>% 
  select(descendant_concept_id)  %>% 
  select(descendant_concept_id) %>% 
  rename(concept_id=descendant_concept_id)%>% 
  left_join(concept_db) %>% 
  select(concept_id,concept_name, standard_concept,domain_id ) %>% 
  collect() ,
# codes without descendants
concept_db %>% 
  filter(concept_id %in% 
     c(37396171,320651,40479642,
       37016927,4100065,439676 ))%>% 
  select(concept_id,concept_name, standard_concept,domain_id ) %>% 
  collect() ) %>% 
  mutate(cohort="Covid diagnosis")     
working.codes<-covid.diagnosis.codes %>% select(concept_id) %>% pull()  

cohort.to.instantiate<-condition_occurrence_db  %>% 
      filter(condition_concept_id %in% working.codes) %>% 
      collect()

cohort.to.instantiate<-cohort.to.instantiate %>% 
     filter(condition_start_date>=as.Date("01/01/2020", "%d/%m/%y"))%>% 
      group_by(person_id) %>% 
      arrange(person_id, condition_start_date) %>% 
      mutate(seq=1:length(person_id)) %>% 
  filter(seq==1) # persons first covid diagnosis

# cases between   "01/09/2020" and "01/09/2020" 
#     min(cohort.to.instantiate$condition_start_date)
#     max(cohort.to.instantiate$condition_start_date)
# cohort.to.instantiate %>%
#       ggplot()+
#       geom_histogram(aes(condition_start_date), binwidth = 1)

working.start.date<-dmy("01/09/2020")
working.end.date<-dmy("01/03/2021")
cohort.to.instantiate<-cohort.to.instantiate %>% 
     filter(condition_start_date>=working.start.date) %>% 
     filter(condition_start_date<=working.end.date)  


cohort.to.instantiate<-cohort.to.instantiate %>% 
      mutate(cohort_definition_id=study.cohorts$id[i]) %>% 
      rename("subject_id"="person_id") %>% 
      rename("cohort_start_date"="condition_start_date")%>% 
      mutate("cohort_end_date"=as.Date("26/05/2021", "%d/%m/%y")) %>% 
      select(cohort_definition_id, subject_id, cohort_start_date,cohort_end_date)

insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableExposures),
            data=cohort.to.instantiate,
            createTable = FALSE,
            progressBar=TRUE)

    }
 

} else {
sql<-readSql(here(working.study.cohort.type, "sql",
                  study.cohorts$file[i]))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
renderTranslateExecuteSql(conn=conn,
                          sql,
                          cdm_database_schema = cdm_database_schema,
                          target_database_schema = results_database_schema,
                          target_cohort_table = cohortTableExposures,
                          results_database_schema = results_database_schema,
                          vocabulary_database_schema=vocabulary_database_schema,
                          target_cohort_id = 1)}
disconnect(conn)

# link to the db
cohortTableExposures_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableExposures)))
# cohortTableExposures_db %>% tally()

# Create Pop df ----  

Pop<-person_db %>% 
  inner_join(cohortTableExposures_db %>% 
               select(one_of("subject_id","cohort_start_date", "dose1_dose2")) %>% 
               rename("person_id"="subject_id")) %>% 
  select( one_of("person_id","cohort_start_date", "dose1_dose2",
    "gender_concept_id",
            "year_of_birth", "month_of_birth", "day_of_birth"))  %>%
  left_join(observation_period_db %>% 
     select("person_id",  "observation_period_start_date", "observation_period_end_date") %>%
       distinct()) %>% 
     collect()

# add age -----
Pop$age<- NA
if(sum(is.na(Pop$day_of_birth))==0 & sum(is.na(Pop$month_of_birth))==0){
 # if we have day and month 
Pop<-Pop %>%
  mutate(age=floor(as.numeric((ymd(cohort_start_date)-
                    ymd(paste(year_of_birth,
                                month_of_birth,
                                day_of_birth, sep="-"))))/365.25))
} else { 
Pop<-Pop %>% 
  mutate(age= year(cohort_start_date)-year_of_birth)
}

# only those aged 20 or older ----
Pop<-Pop %>% 
  filter(age>=20)

# age groups ----
Pop<-Pop %>% 
  mutate(age_gr=ifelse(age>=20 &  age<=44,  "20-44",
                ifelse(age>=45 & age<=54,  "45-54",
                ifelse(age>=55 & age<=64,  "55-64",
                ifelse(age>=65 & age<=74, "65-74", 
                ifelse(age>=75 & age<=84, "75-84",      
                ifelse(age>=85, ">=85",
                       NA))))))) %>% 
  mutate(age_gr= factor(age_gr, 
                   levels = c("20-44","45-54", "55-64",
                              "65-74", "75-84",">=85")))
table(Pop$age_gr, useNA = "always")

# wider age groups
Pop<-Pop %>% 
  mutate(age_gr2=ifelse(age>=20 &  age<=44,  "20-44",
                 ifelse(age>=45 & age<=64,  "45-64",    
                 ifelse(age>=65, ">=65",
                       NA)))) %>% 
  mutate(age_gr2= factor(age_gr2, 
                   levels = c("20-44", "45-64",">=65")))
table(Pop$age_gr2, useNA = "always")

# another alternative set of age groups
Pop<-Pop %>% 
  mutate(age_gr3=  ifelse(age>=20 &  age<=29,  "20-29",
                ifelse(age>=30 &  age<=39,  "30-39",
                ifelse(age>=40 &  age<=49,  "40-49",
                ifelse(age>=50 &  age<=59,  "50-59",
                ifelse(age>=60 & age<=69,  "60-69",
                ifelse(age>=70 & age<=79,  "70-79",      
                ifelse(age>=80, ">=80",
                       NA)))))))) %>% 
  mutate(age_gr3= factor(age_gr3, 
                   levels = c("20-29","30-39","40-49", "50-59",
                              "60-69", "70-79",">=80")))
table(Pop$age_gr3, useNA = "always")


# add gender -----
#8507 male
#8532 female
Pop<-Pop %>% 
  mutate(gender= ifelse(gender_concept_id==8507, "Male",
                 ifelse(gender_concept_id==8532, "Female", NA ))) %>% 
  mutate(gender= factor(gender, 
                   levels = c("Male", "Female")))
table(Pop$gender, useNA = "always")

# if missing age or gender, drop ----
Pop<-Pop %>% 
  filter(!is.na(age))

Pop<-Pop %>% 
  filter(!is.na(gender))



# add prior observation time -----
Pop<-Pop %>%  
  mutate(prior_obs_days=as.numeric(difftime(cohort_start_date,
                                          observation_period_start_date,
                                        units="days"))) %>% 
  mutate(prior_obs_years=prior_obs_days/365.25)


# add medea -----
MEDEA<-observation_db %>% 
  filter(observation_source_value	=="medea") %>% 
  collect()

# U1 is quintile 1 of MEDEA which is the least deprived areas, 
# U5 is quintile 5 and represent the most deprived areas. "R" is or rural areas for which we cannot
# calculate MEDEA. And "U" means a person is assigned to a urban area but the quintile of MEDEA is missing.

# no individuals have more than one record
length(unique(MEDEA$person_id))/ nrow(MEDEA)

# drop values "" as missing
MEDEA<-MEDEA %>% 
  filter(value_as_string %in% c("R", "U1", "U2", "U3", "U4", "U5") )

# dates
# hist(year(MEDEA$observation_date))

# add to person
Pop<-Pop %>% 
  left_join(MEDEA %>% 
              select(person_id, value_as_string) %>% 
              rename(medea=value_as_string)  )
rm(MEDEA)

prop.table(table(Pop$medea, useNA = "always"))




# condition history ------

# add the concept ids of interest to the cohortTableProfiles table in the results 
# schema in the cdm
# these will be the code of interest and all of its descendants

# get any instances over prior history
# for everyone in the database (everyone in the condition_occurrence table),
# left_join to population of interest we´ve established

print(paste0("-- Getting codes for conditions"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableProfiles),
            data=data.frame(condition_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(cond.codes)){ # add codes for each condition
working.code<-cond.codes[n]
working.name<-cond.names[n]
sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableProfiles, " (condition_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}

#link to table
cohortTableProfiles_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableProfiles)))
print(paste0("-- Getting conditions for study population"))

#people with at least one of the conditions
cond.persons <- condition_occurrence_db %>%
  select(person_id, condition_concept_id, condition_start_date) %>% 
  inner_join(cohortTableProfiles_db ,
             by=c("condition_concept_id"="concept_id")) %>% 
  inner_join(cohortTableExposures_db %>% 
               select(subject_id, cohort_start_date) %>% 
               rename("person_id"="subject_id"),
             by = "person_id") %>% 
  filter(condition_start_date < cohort_start_date) %>% 
  select(person_id, condition_id) %>% 
  distinct() %>% 
  collect() 

# add each condition to pop
for(n in 1:length(cond.codes)){# add each to Pop
working.code<-cond.codes[n]
working.name<-cond.names[n]

working.persons <- cond.persons %>% 
  filter(condition_id==n) %>% 
  select(person_id) %>% 
  mutate(working.cond=1)

if(nrow(working.persons)>0){
Pop<-Pop %>%
  left_join(working.persons,
             by = "person_id") %>% 
  rename(!!working.name:="working.cond")
} else {
 Pop$working.cond<-0
 Pop<-Pop %>% 
  rename(!!working.name:="working.cond")
}

}
disconnect(conn)

#to zero if absent
Pop<-Pop %>%
  mutate(across(all_of(cond.names), ~ replace_na(.x, 0)))


# medication history ----
# 183 days prior to four days prior index date

print(paste0("-- Getting codes for medications"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableProfiles),
            data=data.frame(drug_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(drug.codes)){ # add codes for each condition
working.code<-drug.codes[n]
working.name<-drug.names[n]
sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableProfiles, " (drug_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}

#link to table
cohortTableProfiles_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableProfiles)))
print(paste0("-- Getting  medications for study population"))

med.persons <- drug_era_db %>%
        filter(drug_era_start_date>as.Date("2015-01-01")) %>%  
  select(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date) %>% 
  inner_join(cohortTableProfiles_db ,
             by=c("drug_concept_id"="concept_id")) %>% 
  inner_join(cohortTableExposures_db %>% 
               select(subject_id, cohort_start_date) %>% 
               rename("person_id"="subject_id"),
             by = "person_id") %>% 
  filter(drug_era_start_date < cohort_start_date) %>% 
  select(person_id, drug_id, drug_era_start_date, drug_era_end_date,
         cohort_start_date) %>% 
  distinct() %>% 
  collect() %>%
  filter( ( drug_era_start_date<=(cohort_start_date-days(4))
          & drug_era_start_date>=(cohort_start_date-days(183)) ) |
           ( drug_era_end_date<=(cohort_start_date-days(4))
                 & drug_era_end_date>=(cohort_start_date-days(183) ) )) %>% 
  select(person_id, drug_id) %>% 
  distinct() 



for(n in 1:length(drug.codes)){# extract condition info and add to Pop
working.code<-drug.codes[n]
working.name<-drug.names[n]
working.persons <- med.persons %>% 
  filter(drug_id==n) %>% 
  select(person_id) %>% 
  mutate(working.drug=1)

if(nrow(working.persons)>0){
Pop<-Pop %>%
  left_join(working.persons,
             by = "person_id") %>% 
  rename(!!working.name:="working.drug")
} else {
 Pop$working.drug<-0
 Pop<-Pop %>% 
  rename(!!working.name:="working.drug")
}

}
disconnect(conn)

#to zero if absent
Pop<-Pop %>%
  mutate(across(all_of(drug.names), ~ replace_na(.x, 0)))


# composite condition and medication measure -----
c.names<-c("autoimmune_disease",
           "antiphospholipid_syndrome",
           "thrombophilia",
           "atrial_fibrillation",
           "malignant_neoplastic_disease",
           "diabetes_mellitus",
           "obesity",
           "renal_impairment")
d.names<-c("antiinflamatory_and_antirheumatic", 
           "coxibs",
           "corticosteroids",
           "hormonal_contraceptives",
           "tamoxifen",
           "sex_hormones_modulators")

Pop$cond.comp<-rowSums(Pop %>% select(all_of(c.names))) 
Pop$drug.comp<-rowSums(Pop %>% select(all_of(d.names))) 
Pop$cond.drug.comp<-rowSums(Pop %>% select(all_of(c(c.names,  d.names)))) 

Pop<-Pop %>% 
  mutate(cond.comp=ifelse(cond.comp==0, 0,1))%>% 
  mutate(drug.comp=ifelse(drug.comp==0, 0,1))%>% 
  mutate(cond.drug.comp=ifelse(cond.drug.comp==0, 0,1))


# summarise characteristics -----
get.summary.characteristics<-function(working.data, working.name){
summary.characteristics<-bind_rows( 
data.frame(Overall=t(working.data %>% 
  mutate(index_year=year(cohort_start_date)) %>% 
  summarise(n=nice.num.count(length(person_id)),
    age=paste0(nice.num.count(median(age)),  " [",
                     nice.num.count(quantile(age,probs=0.25)),  " to ",
                     nice.num.count(quantile(age,probs=0.75)),   "]" ), 
     age.20_29=paste0(nice.num.count(sum(age_gr3=="20-29")),
                      " (",  nice.num((sum(age_gr3=="20-29")/length(person_id))*100),  "%)"), 
     age.30_39=paste0(nice.num.count(sum(age_gr3=="30-39")),
                      " (",  nice.num((sum(age_gr3=="30-39")/length(person_id))*100),  "%)"), 
    age.40_49=paste0(nice.num.count(sum(age_gr3=="40-49")),
                     " (",  nice.num((sum(age_gr3=="40-49")/length(person_id))*100),  "%)"), 
    age.50_59=paste0(nice.num.count(sum(age_gr3=="50-59")),
                     " (",  nice.num((sum(age_gr3=="50-59")/length(person_id))*100),  "%)"), 
    age.60_69=paste0(nice.num.count(sum(age_gr3=="60-69")),
                     " (",  nice.num((sum(age_gr3=="60-69")/length(person_id))*100),  "%)"), 
    age.70_79=paste0(nice.num.count(sum(age_gr3=="70-79")),
                     " (",  nice.num((sum(age_gr3=="70-79")/length(person_id))*100),  "%)"), 
    age.80u=paste0(nice.num.count(sum(age_gr3==">=80")),
                     " (",  nice.num((sum(age_gr3==">=80")/length(person_id))*100),  "%)"), 
    sex.male=paste0(nice.num.count(sum(gender=="Male")),
                    " (",  nice.num((sum(gender=="Male")/length(person_id))*100),"%)"),
  prior_obs_years=paste0(nice.num(median(prior_obs_years)),
                     " [", nice.num(quantile(prior_obs_years,probs=0.25)), 
                     " to ", nice.num(quantile(prior_obs_years,probs=0.75)),   "]" ),
  medea.R=paste0(nice.num.count(sum(medea=="R", na.rm = T)),
                    " (",  nice.num((sum(medea=="R", na.rm = T)/length(person_id))*100),"%)"),
  medea.U1=paste0(nice.num.count(sum(medea=="U1", na.rm = T)),
                    " (",  nice.num((sum(medea=="U1", na.rm = T)/length(person_id))*100),"%)"),
  medea.U2=paste0(nice.num.count(sum(medea=="U2", na.rm = T)),
                    " (",  nice.num((sum(medea=="U2", na.rm = T)/length(person_id))*100),"%)"),
  medea.U3=paste0(nice.num.count(sum(medea=="U3", na.rm = T)),
                    " (",  nice.num((sum(medea=="U3", na.rm = T)/length(person_id))*100),"%)"),
  medea.U4=paste0(nice.num.count(sum(medea=="U4", na.rm = T)),
                    " (",  nice.num((sum(medea=="U4", na.rm = T)/length(person_id))*100),"%)"),
  medea.U5=paste0(nice.num.count(sum(medea=="U5", na.rm = T)),
                    " (",  nice.num((sum(medea=="U5", na.rm = T)/length(person_id))*100),"%)"),
  medea.Miss=paste0(nice.num.count(sum(is.na(medea))),
                    " (",  nice.num((sum(is.na(medea))/length(person_id))*100),"%)"),
  ))),
# and all the conds and medications
data.frame(Overall=t(working.data %>% 
  summarise_at(.vars = all_of(c(cond.names, drug.names, "cond.comp", "drug.comp", "cond.drug.comp")), 
               .funs = function(x, tot){
  paste0(nice.num.count(sum(x)), " (", nice.num((sum(x)/tot)*100), "%)")
} , tot=nrow(working.data))) ))
summary.characteristics$Overall<-as.character(summary.characteristics$Overall)

rownames(summary.characteristics)<-str_to_sentence(rownames(summary.characteristics))
rownames(summary.characteristics)<-str_replace(rownames(summary.characteristics),
                                      "Sex.male", "Sex: Male")
rownames(summary.characteristics)<-str_replace(rownames(summary.characteristics),
                                      "Prior_obs_years", "Years of prior observation time")
rownames(summary.characteristics)<-str_replace_all(rownames(summary.characteristics) , "_", " ")

#obscure any counts less than 5
summary.characteristics$Overall<-
  ifelse(str_sub(summary.characteristics$Overall, 1, 2) %in%  c("1 ","2 ", "3 ","4 "),
              "<5",summary.characteristics$Overall)
summary.characteristics$var<-row.names(summary.characteristics)
row.names(summary.characteristics)<-1:nrow(summary.characteristics)

summary.characteristics <-summary.characteristics %>% 
  mutate(var=ifelse(var=="Cond.comp",
                    "One or more condition of interest", var )) %>% 
  mutate(var=ifelse(var=="Drug.comp",
                    "One or more medication of interest", var )) %>% 
  mutate(var=ifelse(var=="Cond.drug.comp",
                    "One or more condition/ medication of interest", var ))



summary.characteristics %>% 
  select(var, Overall) %>% 
  rename(!!working.name:=Overall)



}

# overall
Pop.summary.characteristics<-get.summary.characteristics(Pop, "Overall")
# overall, age_gr2
Pop.summary.characteristics.age_gr2.1<-get.summary.characteristics(Pop %>% 
                                                        filter(age_gr2=="20-44"), 
                                                         "Overall")

Pop.summary.characteristics.age_gr2.2<-get.summary.characteristics(Pop %>% 
                                                           filter(age_gr2=="45-64"), 
                                                         "Overall")
Pop.summary.characteristics.age_gr2.3<-get.summary.characteristics(Pop %>% 
                                                           filter(age_gr2==">=65"), 
                                                         "Overall")


if(working.study.cohort.type== "VaccinatedCohorts"){
# before March 2021
Pop.summary.characteristics.before.march<-get.summary.characteristics(
  Pop %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
  "Overall")
Pop.summary.characteristics.before.march.age_gr2.1<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="20-44"),
                                                                   "Overall")
Pop.summary.characteristics.before.march.age_gr2.2<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="45-64"),
                                                                   "Overall")
Pop.summary.characteristics.before.march.age_gr2.3<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2==">=65"),
                                                                   "Overall")


# from March 2021
Pop.summary.characteristics.from.march<-get.summary.characteristics(
  Pop %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
  "Overall")
Pop.summary.characteristics.from.march.age_gr2.1<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="20-44"),
                                                                   "Overall")
Pop.summary.characteristics.from.march.age_gr2.2<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="45-64"),
                                                                   "Overall")
Pop.summary.characteristics.from.march.age_gr2.3<-get.summary.characteristics(Pop %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2==">=65"),
                                                                   "Overall")
}

# those with a year of prior history
Pop.summary.characteristics.with.history<-get.summary.characteristics(
                                   Pop %>% filter(prior_obs_years>=1),
                                          "Overall")


# overall, age_gr2
Pop.summary.characteristics.age_gr2.1.with.history<-get.summary.characteristics(Pop %>% filter(prior_obs_years>=1) %>% 
                                                                     filter(age_gr2=="20-44"), 
                                                                   "Overall")
Pop.summary.characteristics.age_gr2.2.with.history<-get.summary.characteristics(Pop %>% filter(prior_obs_years>=1) %>% 
                                                                     filter(age_gr2=="45-64"), 
                                                                   "Overall")
Pop.summary.characteristics.age_gr2.3.with.history<-get.summary.characteristics(Pop %>% filter(prior_obs_years>=1) %>% 
                                                                     filter(age_gr2==">=65"), 
                                                                   "Overall")


if(working.study.cohort.type== "VaccinatedCohorts"){
# before March 2021
Pop.summary.characteristics.before.march.with.history<-get.summary.characteristics(
  Pop%>%filter(prior_obs_years>=1) %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
  "Overall")
Pop.summary.characteristics.before.march.age_gr2.1.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="20-44"),
                                                                   "Overall")
Pop.summary.characteristics.before.march.age_gr2.2.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="45-64"),
                                                                   "Overall")
Pop.summary.characteristics.before.march.age_gr2.3.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2==">=65"),
                                                                   "Overall")


# from March 2021
Pop.summary.characteristics.from.march.with.history<-get.summary.characteristics(
  Pop %>%filter(prior_obs_years>=1) %>% 
    filter(cohort_start_date>=as.Date("2021-03-01")),
  "Overall")
Pop.summary.characteristics.from.march.age_gr2.1.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="20-44"),
                                                                   "Overall")
Pop.summary.characteristics.from.march.age_gr2.2.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2=="45-64"),
                                                                   "Overall")
Pop.summary.characteristics.from.march.age_gr2.3.with.history<-get.summary.characteristics(Pop%>%filter(prior_obs_years>=1) %>%
                                                                           filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                     filter(age_gr2==">=65"),
                                                                   "Overall")
}


# results for each outcome of interest -----

# for(j in 1:1){ # for each outcome of interest
for(j in 1:length(outcome.cohorts$id)){ # for each outcome of interest
working.outcome<-outcome.cohorts$id[j]
working.outcome.name<-outcome.cohorts$name[j]

print(paste0("- Getting ", working.outcome.name,
        " (", j, " of ", length(outcome.cohorts$id), ")"))
working.Pop<-Pop 
  
# drop time-varying covariates, we will get them again for the index date of the event 
working.Pop<-working.Pop %>% 
  select(-age, age_gr, age_gr2, age_gr2, prior_obs_years) %>% 
  select(-all_of(cond.names)) %>% 
  select(-all_of(drug.names))

# event of interest
if(working.outcome.name=="imm throm"){
  # for imm throm, either this specific cohort based on diagnosis codes or HIT (which included drug eras)
 ids<-c(outcome.cohorts %>% 
          filter(name=="HIT") %>% 
          select(id) %>% pull(),
        working.outcome)
  working.outcomes<-outcome_db %>%
    filter(cohort_definition_id %in% 
             ids) %>%
    select(subject_id, cohort_start_date) %>% 
    collect()
} else {
working.outcomes<-outcome_db %>%
  filter(cohort_definition_id %in% working.outcome) %>%
  select(subject_id, cohort_start_date) %>% 
  collect()
}

# thrombocytopenia window
if(str_detect(working.outcome.name, "(with thrombocytopenia 10 days pre to 10 days post)")){
  thrombocyt.id<-outcome.cohorts %>% 
                   filter(name=="thrombocyt") %>% 
                   select(id) %>% pull() 
  thromb.outcomes<-outcome_db %>%
    filter(cohort_definition_id ==thrombocyt.id) %>% 
    select(subject_id, cohort_start_date) %>% 
    rename("thromb.date"="cohort_start_date") %>% 
    collect()
  # find any outcomes with thrombocytopenia also observed
  working.outcomes<-working.outcomes %>% 
    inner_join(thromb.outcomes)
  # find any outcomes with thrombocytopenia in the time window
  working.outcomes$dtime<-as.numeric(difftime(working.outcomes$thromb.date,
                                              working.outcomes$cohort_start_date, units="days"))
  working.outcomes<-working.outcomes %>% 
    filter(dtime>=(-10)) %>% 
    filter(dtime<=10)
  
  working.outcomes<-working.outcomes %>% 
    select(-dtime) %>% 
    select(-thromb.date)
}

if(str_detect(working.outcome.name, "(with thrombocytopenia 42 days pre to 14 days post)")){
  thrombocyt.id<-outcome.cohorts %>% 
    filter(name=="thrombocyt") %>% 
    select(id) %>% pull() 
  thromb.outcomes<-outcome_db %>%
    filter(cohort_definition_id ==thrombocyt.id) %>% 
    select(subject_id, cohort_start_date) %>% 
    rename("thromb.date"="cohort_start_date") %>% 
    collect()
  # find any outcomes with thrombocytopenia also observed
  working.outcomes<-working.outcomes %>% 
    inner_join(thromb.outcomes)
  # find any outcomes with thrombocytopenia in the time window
  working.outcomes$dtime<-as.numeric(difftime(working.outcomes$thromb.date,
                                              working.outcomes$cohort_start_date, units="days"))
  working.outcomes<-working.outcomes %>% 
    filter(dtime>=(-42)) %>% 
    filter(dtime<=14)
  
  working.outcomes<-working.outcomes %>% 
    select(-dtime) %>% 
    select(-thromb.date)
}

# any occurrences
if(working.outcomes %>%  
  inner_join(working.Pop %>% 
               select(person_id) %>% 
               rename("subject_id"="person_id")) %>% 
  tally() %>% 
  pull()==0){
  print(paste0("No occurrences of ", working.outcome.name, " among study population"))
} else {
  # continue only if occurrences of outcome

# drop anyone with the outcome in the year prior to the index, including index date itself ------
washout.outcomes<-working.outcomes %>% 
  inner_join(working.Pop %>% 
               select(person_id,cohort_start_date) %>% 
               rename("subject_id"="person_id") %>% 
               rename("Pop_cohort_start_date"="cohort_start_date")) %>% 
  filter(cohort_start_date>= (Pop_cohort_start_date-years(1))) %>% 
  filter(cohort_start_date<= Pop_cohort_start_date) # nb including index date itself

working.Pop<-working.Pop %>% 
  anti_join(washout.outcomes %>% 
  select(subject_id) %>% 
  distinct(),
  by=c("person_id"="subject_id"))  
  
# history of outcome event -----
history.outcomes<-working.outcomes %>%  
  inner_join(working.Pop %>% 
               select(person_id,cohort_start_date) %>% 
               rename("subject_id"="person_id") %>% 
               rename("Pop_cohort_start_date"="cohort_start_date")) %>% 
  filter(cohort_start_date< (Pop_cohort_start_date-years(1))) 
working.Pop<-working.Pop %>% 
  left_join(history.outcomes  %>% 
  select(subject_id) %>% 
  distinct() %>% 
  mutate(history_outcome=1),
  by=c("person_id"="subject_id"))
working.Pop<-working.Pop %>% 
  mutate(history_outcome=ifelse(is.na(history_outcome),0,1))


# first event after index date up to  -----

# for vaccinated: outcomes over 21 days from index
if(working.study.cohort.type=="VaccinatedCohorts"){
f_u.outcome<-working.outcomes %>%  
  inner_join(working.Pop %>% 
               select(person_id,cohort_start_date) %>% 
               rename("subject_id"="person_id") %>% 
               rename("Pop_cohort_start_date"="cohort_start_date"))  %>% 
  filter(cohort_start_date> Pop_cohort_start_date) %>% 
  filter(cohort_start_date<= (Pop_cohort_start_date+days(21))) 
}

# for covid: outcomes over 90 days from index
if(working.study.cohort.type=="CovidCohorts"){
  f_u.outcome<-working.outcomes %>%  
    inner_join(working.Pop %>% 
                 select(person_id,cohort_start_date) %>% 
                 rename("subject_id"="person_id") %>% 
                 rename("Pop_cohort_start_date"="cohort_start_date"))  %>% 
    filter(cohort_start_date> Pop_cohort_start_date) %>% 
    filter(cohort_start_date<= (Pop_cohort_start_date+days(90))) 
}


if(working.study.cohort.type=="GeneralPopCohorts"){
  f_u.outcome<-working.outcomes %>%  
    inner_join(working.Pop %>% 
                 select(person_id,cohort_start_date) %>% 
                 rename("subject_id"="person_id") %>% 
                 rename("Pop_cohort_start_date"="cohort_start_date"))  %>% 
    filter(as.Date(cohort_start_date)> as.Date(Pop_cohort_start_date)) %>% 
    filter(as.Date(cohort_start_date)<= as.Date(dmy(paste0("31-12-","2019"))))
           }

if(nrow(f_u.outcome)>=1){ 
f_u.outcome<-f_u.outcome %>% 
  group_by(subject_id) %>%
  arrange(cohort_start_date) %>% 
  mutate(seq=1:length(subject_id)) %>% 
  filter(seq==1) %>% 
  select(subject_id,cohort_start_date)  %>% 
  rename("f_u.outcome_date"="cohort_start_date") %>% 
  mutate(f_u.outcome=1)
working.Pop<-working.Pop %>% 
  left_join(f_u.outcome,
  by=c("person_id"="subject_id"))
working.Pop<-working.Pop %>% 
  mutate(f_u.outcome=ifelse(is.na(f_u.outcome),0,1))
# sum(working.Pop$f_u.outcome)

# TAR -----
# VaccinatedCohorts: censor at first of outcome, second dose end of observation period, 21 days
if(working.study.cohort.type=="VaccinatedCohorts"){

if("dose1_dose2" %in% names(working.Pop)==FALSE){
  # create if it doesn´t exist, eg cause our cohort is a second dose cohort
  working.Pop$dose1_dose2<-NA
} 
  
working.Pop$dose.in.tar<-ifelse(!is.na(working.Pop$dose1_dose2) & working.Pop$dose1_dose2>-21,
                                1,0 )
# table(working.Pop$dose.in.tar, useNA = "always")
  
working.Pop<-working.Pop %>%
  mutate(f_u.outcome_date=if_else(f_u.outcome==1,
                                  f_u.outcome_date, 
                          if_else(dose.in.tar==1,
                                  cohort_start_date+days(abs(dose1_dose2)), 
                               if_else(observation_period_end_date < (cohort_start_date+days(21)),
                                       observation_period_end_date, 
                                       (cohort_start_date+days(21)) ))))
         }


# CovidCohorts: censor at first of outcome, end of observation period, 90 days
if(working.study.cohort.type=="CovidCohorts"){
  working.Pop<-working.Pop %>%
    mutate(f_u.outcome_date=if_else(f_u.outcome==1,
                                    f_u.outcome_date, 
                                    if_else(observation_period_end_date < (cohort_start_date+days(90)),
                                            observation_period_end_date, 
                                            (cohort_start_date+days(90)) )))}
# GeneralPopCohorts: censor at first of outcome, end of observation period, end date
if(working.study.cohort.type=="GeneralPopCohorts"){
  working.Pop<-working.Pop %>%
    mutate(f_u.outcome_date=if_else(f_u.outcome==1,
                                    f_u.outcome_date, 
                                    if_else(observation_period_end_date < as.Date(dmy(paste0("31-12-","2019"))),
                                            observation_period_end_date, 
                                            as.Date(dmy(paste0("31-12-","2019"))) )))
  }



working.Pop<-working.Pop %>% 
  mutate(f_u.outcome.days=as.numeric(difftime(f_u.outcome_date,
                                              cohort_start_date, 
                                                   units="days")))
# quantile(working.Pop$f_u.outcome.days)
# working.Pop %>% 
#   group_by(f_u.outcome) %>% 
#   summarise(min(f_u.outcome.days),
#             median(f_u.outcome.days),
#             max(f_u.outcome.days))

# require minimum of 1 day of time at risk ----
working.Pop<-working.Pop %>% 
  filter(f_u.outcome.days>0)

# characteristics ----
working.Pop.w.outcome<-working.Pop %>% filter(f_u.outcome==1) 

# add age and gender -----
working.Pop.w.outcome$age<- NA
if(sum(is.na(working.Pop.w.outcome$day_of_birth))==0 & sum(is.na(working.Pop.w.outcome$month_of_birth))==0){
 # if we have day and month 
working.Pop.w.outcome<-working.Pop.w.outcome %>%
  mutate(age=floor(as.numeric((ymd(cohort_start_date)-
                    ymd(paste(year_of_birth,
                                month_of_birth,
                                day_of_birth, sep="-"))))/365.25))
} else { 
working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  mutate(age= year(cohort_start_date)-year_of_birth)
}

working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  mutate(age_gr=ifelse(age>=20 &  age<=44,  "20-44",
                ifelse(age>=45 & age<=54,  "45-54",
                ifelse(age>=55 & age<=64,  "55-64",
                ifelse(age>=65 & age<=74, "65-74", 
                ifelse(age>=75 & age<=84, "75-84",      
                ifelse(age>=85, ">=85",
                       NA))))))) %>% 
  mutate(age_gr= factor(age_gr, 
                   levels = c("20-44","45-54", "55-64",
                              "65-74", "75-84",">=85")))
table(working.Pop.w.outcome$age_gr, useNA = "always")

# wider age groups
working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  mutate(age_gr2=ifelse(age>=20 &  age<=44,  "20-44",
                 ifelse(age>=45 & age<=64,  "45-64",    
                 ifelse(age>=65, ">=65",
                       NA)))) %>% 
  mutate(age_gr2= factor(age_gr2, 
                   levels = c("20-44", "45-64",">=65")))
table(working.Pop.w.outcome$age_gr2, useNA = "always")

# another alternative set of age groups
working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  mutate(age_gr3=  ifelse(age>=20 &  age<=29,  "20-29",
                ifelse(age>=30 &  age<=39,  "30-39",
                ifelse(age>=40 &  age<=49,  "40-49",
                ifelse(age>=50 &  age<=59,  "50-59",
                ifelse(age>=60 & age<=69,  "60-69",
                ifelse(age>=70 & age<=79,  "70-79",      
                ifelse(age>=80, ">=80",
                       NA)))))))) %>% 
  mutate(age_gr3= factor(age_gr3, 
                   levels = c("20-29","30-39","40-49", "50-59",
                              "60-69", "70-79",">=80")))
table(working.Pop.w.outcome$age_gr3, useNA = "always")


# add prior observation time -----
working.Pop.w.outcome<-working.Pop.w.outcome %>%  
  mutate(prior_obs_days=as.numeric(difftime(f_u.outcome_date,
                                          observation_period_start_date,
                                        units="days"))) %>% 
  mutate(prior_obs_years=prior_obs_days/365.25)


# condition history -----
print(paste0("-- Getting codes for conditions"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableProfiles),
            data=data.frame(condition_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(cond.codes)){ # add codes for each condition
working.code<-cond.codes[n]
working.name<-cond.names[n]
sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableProfiles, " (condition_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}

#link to table
cohortTableProfiles_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableProfiles)))
print(paste0("-- Getting  conditions"))

cond.persons <- outcome_db %>% 
  filter(cohort_definition_id==working.outcome) %>% 
  select(subject_id, cohort_start_date) %>% 
  distinct() %>% 
  left_join(condition_occurrence_db,
            by=c("subject_id"="person_id")
            ) %>% 
  rename("person_id"="subject_id")  %>%
  select(person_id, condition_concept_id, condition_start_date) %>% 
  inner_join(cohortTableProfiles_db ,
             by=c("condition_concept_id"="concept_id")) %>% 
  inner_join(cohortTableExposures_db %>% 
               select(subject_id, cohort_start_date) %>% 
               rename("person_id"="subject_id"),
             by = "person_id") %>% 
  filter(condition_start_date < cohort_start_date) %>% 
  select(person_id, condition_id) %>% 
  distinct() %>% 
  collect() 


for(n in 1:length(cond.codes)){# extract condition info and add to working.Pop.w.outcome
working.code<-cond.codes[n]
working.name<-cond.names[n]
working.persons <-   cond.persons %>% 
  filter(condition_id==n) %>% 
  select(person_id) %>% 
  mutate(working.cond=1)

if(nrow(working.persons)>0){
working.Pop.w.outcome<-working.Pop.w.outcome %>%
  left_join(working.persons,
             by = "person_id") %>% 
  rename(!!working.name:="working.cond")
} else {
 working.Pop.w.outcome$working.cond<-0
 working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  rename(!!working.name:="working.cond")
}

}
disconnect(conn)


#to zero if absent
working.Pop.w.outcome<-working.Pop.w.outcome %>%
  mutate(across(all_of(cond.names), ~ replace_na(.x, 0)))

# medication history -----
print(paste0("-- Getting codes for medications"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableProfiles),
            data=data.frame(drug_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(drug.codes)){ # add codes for each condition
working.code<-drug.codes[n]
working.name<-drug.names[n]
sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableProfiles, " (drug_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}


print(paste0("-- Getting  medications"))
cohortTableProfiles_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", cohortTableProfiles)))

med.persons <- outcome_db %>% 
  filter(cohort_definition_id==working.outcome) %>% 
  select(subject_id, cohort_start_date) %>% 
  distinct() %>% 
  left_join(drug_era_db,
            by=c("subject_id"="person_id")
            ) %>% 
  rename("person_id"="subject_id") %>%
        filter(drug_era_start_date>as.Date("2015-01-01")) %>%  
  filter(drug_era_start_date < cohort_start_date) %>% 
  inner_join(cohortTableProfiles_db ,
             by=c("drug_concept_id"="concept_id")) %>% 
  select(person_id,drug_id, drug_era_start_date, drug_era_end_date, cohort_start_date) %>% 
  distinct() %>% 
  collect() %>%
  filter(drug_era_start_date<=(cohort_start_date-days(4))
          & drug_era_start_date>=(cohort_start_date-days(183)) |
           drug_era_end_date<=(cohort_start_date-days(4))
                 & drug_era_end_date>=(cohort_start_date-days(183))) %>% 
  select(person_id, drug_id)%>% 
  distinct()

for(n in 1:length(drug.codes)){# extract condition info and add to Pop
working.code<-drug.codes[n]
working.name<-drug.names[n]
working.persons <-med.persons %>% 
  filter(drug_id==n) %>% 
  select(person_id) %>% 
  mutate(working.drug=1)

if(nrow(working.persons)>0){
working.Pop.w.outcome<-working.Pop.w.outcome %>%
  left_join(working.persons,
             by = "person_id") %>% 
  rename(!!working.name:="working.drug")
} else {
 working.Pop.w.outcome$working.drug<-0
 working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  rename(!!working.name:="working.drug")
}

}
disconnect(conn)

#to zero if absent
working.Pop.w.outcome<-working.Pop.w.outcome %>%
  mutate(across(all_of(drug.names), ~ replace_na(.x, 0)))


# composite condition and medication measure -----
working.Pop.w.outcome$cond.comp<-rowSums(working.Pop.w.outcome %>% select(all_of(c.names))) 
working.Pop.w.outcome$drug.comp<-rowSums(working.Pop.w.outcome %>% select(all_of(d.names))) 
working.Pop.w.outcome$cond.drug.comp<-rowSums(working.Pop.w.outcome %>% select(all_of(c(c.names,  d.names)))) 

working.Pop.w.outcome<-working.Pop.w.outcome %>% 
  mutate(cond.comp=ifelse(cond.comp==0, 0,1))%>% 
  mutate(drug.comp=ifelse(drug.comp==0, 0,1))%>% 
  mutate(cond.drug.comp=ifelse(cond.drug.comp==0, 0,1))
# summarise characteristics -----
# overall
Pop.summary.characteristics<-left_join(Pop.summary.characteristics,
  get.summary.characteristics(working.Pop.w.outcome, working.outcome.name),
  by="var")

Pop.summary.characteristics.age_gr2.1<-left_join(Pop.summary.characteristics.age_gr2.1,
                                                              get.summary.characteristics(working.Pop.w.outcome %>% 
                                                                                  filter(age_gr2=="20-44"), 
                                                                                  working.outcome.name),
                                                              by="var")
Pop.summary.characteristics.age_gr2.2<-left_join(Pop.summary.characteristics.age_gr2.2,
                                                              get.summary.characteristics(working.Pop.w.outcome %>%  
                                                                                  filter(age_gr2=="45-64"), 
                                                                                  working.outcome.name),
                                                              by="var")
Pop.summary.characteristics.age_gr2.3<-left_join(Pop.summary.characteristics.age_gr2.3,
                                                              get.summary.characteristics(working.Pop.w.outcome %>%
                                                                                  filter(age_gr2==">=65"), 
                                                                                  working.outcome.name),
                                                              by="var")
if(working.study.cohort.type== "VaccinatedCohorts"){
# before march
Pop.summary.characteristics.before.march<-left_join(Pop.summary.characteristics.before.march,
                                               get.summary.characteristics(working.Pop.w.outcome%>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")),
                                                                           working.outcome.name),
                                               by="var")
Pop.summary.characteristics.before.march.age_gr2.1<-left_join(Pop.summary.characteristics.before.march.age_gr2.1,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2=="20-44"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.before.march.age_gr2.2<-left_join(Pop.summary.characteristics.before.march.age_gr2.2,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01"))%>%
                                                                               filter(age_gr2=="45-64"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.before.march.age_gr2.3<-left_join(Pop.summary.characteristics.before.march.age_gr2.3,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2==">=65"),
                                                                             working.outcome.name),
                                                 by="var")

# from march
Pop.summary.characteristics.from.march<-left_join(Pop.summary.characteristics.from.march,
                                               get.summary.characteristics(working.Pop.w.outcome%>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")),
                                                                           working.outcome.name),
                                               by="var")
Pop.summary.characteristics.from.march.age_gr2.1<-left_join(Pop.summary.characteristics.from.march.age_gr2.1,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2=="20-44"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.from.march.age_gr2.2<-left_join(Pop.summary.characteristics.from.march.age_gr2.2,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01"))%>%
                                                                               filter(age_gr2=="45-64"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.from.march.age_gr2.3<-left_join(Pop.summary.characteristics.from.march.age_gr2.3,
                                                 get.summary.characteristics(working.Pop.w.outcome %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2==">=65"),
                                                                             working.outcome.name),
                                                 by="var")



}






#with.history
# overall
Pop.summary.characteristics.with.history<-left_join(Pop.summary.characteristics.with.history,
                                       get.summary.characteristics(working.Pop.w.outcome, working.outcome.name),
                                       by="var")
Pop.summary.characteristics.age_gr2.1.with.history<-left_join(Pop.summary.characteristics.age_gr2.1.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1)   %>% 
                                                                               filter(age_gr2=="20-44"), 
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.age_gr2.2.with.history<-left_join(Pop.summary.characteristics.age_gr2.2.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1)   %>%  
                                                                               filter(age_gr2=="45-64"), 
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.age_gr2.3.with.history<-left_join(Pop.summary.characteristics.age_gr2.3.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1)   %>%
                                                                               filter(age_gr2==">=65"), 
                                                                             working.outcome.name),
                                                 by="var")

if(working.study.cohort.type== "VaccinatedCohorts"){
  # before march
Pop.summary.characteristics.before.march.with.history<-left_join(Pop.summary.characteristics.before.march.with.history,
                                               get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1)%>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")),
                                                                           working.outcome.name),
                                               by="var")
Pop.summary.characteristics.before.march.age_gr2.1.with.history<-left_join(Pop.summary.characteristics.before.march.age_gr2.1.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2=="20-44"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.before.march.age_gr2.2.with.history<-left_join(Pop.summary.characteristics.before.march.age_gr2.2.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01"))%>%
                                                                               filter(age_gr2=="45-64"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.before.march.age_gr2.3.with.history<-left_join(Pop.summary.characteristics.before.march.age_gr2.3.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date<as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2==">=65"),
                                                                             working.outcome.name),
                                                 by="var")

# from march
Pop.summary.characteristics.from.march.with.history<-left_join(Pop.summary.characteristics.from.march.with.history,
                                               get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1)%>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")),
                                                                           working.outcome.name),
                                               by="var")
Pop.summary.characteristics.from.march.age_gr2.1.with.history<-left_join(Pop.summary.characteristics.from.march.age_gr2.1.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2=="20-44"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.from.march.age_gr2.2.with.history<-left_join(Pop.summary.characteristics.from.march.age_gr2.2.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01"))%>%
                                                                               filter(age_gr2=="45-64"),
                                                                             working.outcome.name),
                                                 by="var")
Pop.summary.characteristics.from.march.age_gr2.3.with.history<-left_join(Pop.summary.characteristics.from.march.age_gr2.3.with.history,
                                                 get.summary.characteristics(working.Pop.w.outcome %>% filter(prior_obs_years>=1) %>%
                                                                             filter(cohort_start_date>=as.Date("2021-03-01")) %>%
                                                                               filter(age_gr2==">=65"),
                                                                             working.outcome.name),
                                                 by="var")
}





# time splits -----

# VaccinatedCohorts: splitting follow up time: w1, 0_14, 0_218 days 
if(working.study.cohort.type== "VaccinatedCohorts"){
a<- survSplit(Surv(f_u.outcome.days, f_u.outcome) ~., 
                 working.Pop,
                   cut=c(7, 14, 21), 
           episode ="timegroup", 
           end="tend")
working.Pop.w1<-a %>% filter(timegroup==1)
working.Pop.w2<-a %>% filter(timegroup==2)
working.Pop.w3<-a %>% filter(timegroup==3)
rm(a)
}

# CovidCohorts: splitting follow up time: 0_14, 0_28, 0_60, 28_90
if(working.study.cohort.type== "CovidCohorts"){
  a<- survSplit(Surv(f_u.outcome.days, f_u.outcome) ~., 
                working.Pop,
                cut=c(14, 21, 90), 
                episode ="timegroup", 
                end="tend")
  working.Pop.w1<-a %>% filter(timegroup==1)
  working.Pop.w2<-a %>% filter(timegroup==2)
  working.Pop.w3<-a %>% filter(timegroup==3)
  rm(a)
}

# Not used for GeneralPopCohorts
if(working.study.cohort.type== "GeneralPopCohorts"){
working.Pop.w1<-NULL
working.Pop.w2<-NULL
working.Pop.w3<-NULL
}

# IRs  ------
get.IR.summaries<-function(working.data,
                           working.data.w1,
                           working.data.w2,
                           working.data.w3,
                           value.prior.obs.required,
                           value.pop.type,
                           value.working.outcome,
                           value.working.outcome.name,
                           value.working.study.cohort){
  
  working.IR.summary<-list()
  
  # overall
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "Full",";","overall")]]<-working.data %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="overall") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
  
  if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","overall")]]<-working.data.w1 %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="overall") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","overall")]]<-working.data.w2 %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="overall") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","overall")]]<-working.data.w3 %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="overall") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
  }
  
  
# by gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "Full",";","gender")]]<-working.data %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","gender")]]<-working.data.w1 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","gender")]]<-working.data.w2 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","gender")]]<-working.data.w3 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}



# by gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "Full",";","gender")]]<-working.data %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","gender")]]<-working.data.w1 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","gender")]]<-working.data.w2 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","gender")]]<-working.data.w3 %>%  
  group_by(gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}




# by cond.comp
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","cond.comp")]]<-working.data %>%  
  group_by(cond.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="cond.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","cond.comp")]]<-working.data.w1 %>%  
    group_by(cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","cond.comp")]]<-working.data.w2 %>%  
    group_by(cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","cond.comp")]]<-working.data.w3 %>%  
    group_by(cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}


# by drug.comp
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","drug.comp")]]<-working.data %>%  
  group_by(drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","drug.comp")]]<-working.data.w1 %>%  
    group_by(drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","drug.comp")]]<-working.data.w2 %>%  
    group_by(drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","drug.comp")]]<-working.data.w3 %>%  
    group_by(drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}



# by cond.drug.comp
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","cond.drug.comp")]]<-working.data %>%  
  group_by(cond.drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="cond.drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","cond.drug.comp")]]<-working.data.w1 %>%  
    group_by(cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","cond.drug.comp")]]<-working.data.w2 %>%  
    group_by(cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","cond.drug.comp")]]<-working.data.w3 %>%  
    group_by(cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}



# by age and gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "Full",";","age_gr_gender")]]<-working.data %>%  
  group_by(age_gr, gender) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr_gender")]]<-working.data.w1 %>%  
  group_by(age_gr, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>%  
   mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","age_gr_gender")]]<-working.data.w2 %>%  
  group_by(age_gr, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr_gender")]]<-working.data.w3 %>%  
  group_by(age_gr, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}



# by age (fewer groups) and gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "Full",";","age_gr2_gender")]]<-working.data %>%  
  group_by(age_gr2, gender) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr2_gender")]]<-working.data.w1 %>%  
  group_by(age_gr2, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","gender")]]<-working.data.w2 %>%  
  group_by(age_gr2, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
 mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr2_gender")]]<-working.data.w3 %>%  
  group_by(age_gr2, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
 mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}


# by age (thrid definition) and gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                    "Full",";","age_gr3_gender")]]<-working.data %>%  
  group_by(age_gr3, gender) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr3_gender")]]<-working.data.w1 %>%  
  group_by(age_gr3, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","age_gr3_gender")]]<-working.data.w2 %>%  
  group_by(age_gr3, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr3_gender")]]<-working.data.w3 %>%  
  group_by(age_gr3, gender) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}



# by age 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                    "Full",";","age_gr")]]<-working.data %>%  
  group_by(age_gr) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr")]]<-working.data.w1 %>%  
  group_by(age_gr) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","age_gr")]]<-working.data.w2 %>%  
  group_by(age_gr) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr")]]<-working.data.w3 %>%  
  group_by(age_gr) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}

# by age (fewer groups) and gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                    "Full",";","age_gr2")]]<-working.data %>%  
  group_by(age_gr2) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr2")]]<-working.data.w1 %>%  
  group_by(age_gr2) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","age_gr2")]]<-working.data.w2 %>%  
  group_by(age_gr2) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr2")]]<-working.data.w3 %>%  
  group_by(age_gr2) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)
}


# by age (thrid definition) and gender
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                    "Full",";","age_gr3")]]<-working.data %>%  
  group_by(age_gr3) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
   mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)

if(is.null(working.data.w1)!=TRUE){ 
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w1",";","age_gr3")]]<-working.data.w1 %>%  
  group_by(age_gr3) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w1") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w2",";","age_gr3")]]<-working.data.w2 %>%  
  group_by(age_gr3) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w2") %>% 
  mutate(pop=value.working.study.cohort)
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                   "w3",";","age_gr3")]]<-working.data.w3 %>%  
  group_by(age_gr3) %>% 
  summarise(n=length(person_id),
            days=sum(tend-tstart),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="w3") %>% 
  mutate(pop=value.working.study.cohort)

}



# by age_gr, sex, and condition
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr_gender_cond.comp")]]<-working.data %>%  
  group_by(age_gr, gender, cond.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender_cond.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr_gender_cond.comp")]]<-working.data.w1 %>%  
    group_by(age_gr, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr_gender_cond.comp")]]<-working.data.w2 %>%  
    group_by(age_gr, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr_gender_cond.comp")]]<-working.data.w3 %>%  
    group_by(age_gr, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}

# by age_gr, sex, and medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr_gender_drug.comp")]]<-working.data %>%  
  group_by(age_gr, gender, drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender_drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr_gender_drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr_gender_drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr_gender_drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}


# by age_gr, sex, and condition/ medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr_gender_cond.drug.comp")]]<-working.data %>%  
  group_by(age_gr, gender, cond.drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr_gender_cond.drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr_gender_cond.drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr_gender_cond.drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr_gender_cond.drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}




# by age_gr2, sex, and condition
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr2_gender_cond.comp")]]<-working.data %>%  
  group_by(age_gr2, gender, cond.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender_cond.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr2_gender_cond.comp")]]<-working.data.w1 %>%  
    group_by(age_gr2, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr2_gender_cond.comp")]]<-working.data.w2 %>%  
    group_by(age_gr2, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr2_gender_cond.comp")]]<-working.data.w3 %>%  
    group_by(age_gr2, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}

# by age_gr2, sex, and medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr2_gender_drug.comp")]]<-working.data %>%  
  group_by(age_gr2, gender, drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender_drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr2_gender_drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr2, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr2_gender_drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr2, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr2_gender_drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr2, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}


# by age_gr2, sex, and condition/ medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr2_gender_cond.drug.comp")]]<-working.data %>%  
  group_by(age_gr2, gender, cond.drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr2_gender_cond.drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr2_gender_cond.drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr2, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr2_gender_cond.drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr2, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr2_gender_cond.drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr2, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr2_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}








# by age_gr3, sex, and condition
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr3_gender_cond.comp")]]<-working.data %>%  
  group_by(age_gr3, gender, cond.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender_cond.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr3_gender_cond.comp")]]<-working.data.w1 %>%  
    group_by(age_gr3, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr3_gender_cond.comp")]]<-working.data.w2 %>%  
    group_by(age_gr3, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr3_gender_cond.comp")]]<-working.data.w3 %>%  
    group_by(age_gr3, gender, cond.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}

# by age_gr3, sex, and medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr3_gender_drug.comp")]]<-working.data %>%  
  group_by(age_gr3, gender, drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender_drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr3_gender_drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr3, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr3_gender_drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr3, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr3_gender_drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr3, gender, drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}


# by age_gr3, sex, and condition/ medication
working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                           "Full",";","age_gr3_gender_cond.drug.comp")]]<-working.data %>%  
  group_by(age_gr3, gender, cond.drug.comp) %>% 
  summarise(n=length(person_id),
            days=sum(f_u.outcome.days),
            years=(days/365.25),
            events=sum(f_u.outcome)) %>% 
  mutate(ir_100000=(events/years)*100000) %>% 
  mutate(strata="age_gr3_gender_cond.drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(time.window="Full") %>% 
  mutate(pop=value.working.study.cohort)
if(is.null(working.data.w1)!=TRUE){ 
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w1",";","age_gr3_gender_cond.drug.comp")]]<-working.data.w1 %>%  
    group_by(age_gr3, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>%  
    mutate(time.window="w1") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w2",";","age_gr3_gender_cond.drug.comp")]]<-working.data.w2 %>%  
    group_by(age_gr3, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w2") %>% 
    mutate(pop=value.working.study.cohort)
  working.IR.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                             "w3",";","age_gr3_gender_cond.drug.comp")]]<-working.data.w3 %>%  
    group_by(age_gr3, gender, cond.drug.comp) %>% 
    summarise(n=length(person_id),
              days=sum(tend-tstart),
              years=(days/365.25),
              events=sum(f_u.outcome)) %>% 
    mutate(ir_100000=(events/years)*100000) %>% 
    mutate(strata="age_gr3_gender_cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(time.window="w3") %>% 
    mutate(pop=value.working.study.cohort)
}











# return
  bind_rows(working.IR.summary,.id = NULL)
}
# no prior obs
# all pop
IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "pop.all")]]<-
get.IR.summaries(working.data=working.Pop,
                 working.data.w1=working.Pop.w1,
                 working.data.w2=working.Pop.w2,
                 working.data.w3=working.Pop.w3,
                 value.prior.obs.required="No",
                 value.pop.type="All",
                 value.working.outcome=working.outcome,
                 value.working.outcome.name=working.outcome.name,
                 value.working.study.cohort=working.study.cohort)

if(working.study.cohort.type=="VaccinatedCohorts"){
# no prior obs
  # before march
  IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "before.march")]]<-
    get.IR.summaries(working.data=working.Pop%>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w1=working.Pop.w1 %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w2=working.Pop.w2 %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w3=working.Pop.w3 %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     value.prior.obs.required="No",
                     value.pop.type="Before March",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)

  # from march
  IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "from.march")]]<-
    get.IR.summaries(working.data=working.Pop%>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w1=working.Pop.w1 %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w2=working.Pop.w2 %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w3=working.Pop.w3 %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     value.prior.obs.required="No",
                     value.pop.type="From March",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)

}


# prior obs
# all pop
if(is.null(working.Pop.w1)!=TRUE){ 
IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "pop.all")]]<-
get.IR.summaries(working.data=working.Pop %>% 
                              filter(prior_obs_years>=1),
                 working.data.w1=working.Pop.w1 %>% 
                              filter(prior_obs_years>=1),
                 working.data.w2=working.Pop.w2 %>% 
                              filter(prior_obs_years>=1),
                 working.data.w3=working.Pop.w3 %>% 
                              filter(prior_obs_years>=1),
                 value.prior.obs.required="Yes",
                 value.pop.type="All",
                 value.working.outcome=working.outcome,
                 value.working.outcome.name=working.outcome.name,
                 value.working.study.cohort=working.study.cohort)
} else {
  IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "pop.all")]]<-
    get.IR.summaries(working.data=working.Pop %>% 
                       filter(prior_obs_years>=1),
                     working.data.w1=working.Pop.w1,
                     working.data.w2=working.Pop.w2,
                     working.data.w3=working.Pop.w3,
                     value.prior.obs.required="Yes",
                     value.pop.type="All",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort) 
  
}

if(working.study.cohort.type=="VaccinatedCohorts"){
  # prior obs
    # before march
  IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "before.march")]]<-
    get.IR.summaries(working.data=working.Pop%>%
                       filter(prior_obs_years>=1)%>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w1=working.Pop.w1 %>%
                       filter(prior_obs_years>=1)%>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w2=working.Pop.w2%>%
                       filter(prior_obs_years>=1) %>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     working.data.w3=working.Pop.w3 %>%
                       filter(prior_obs_years>=1)%>%
    filter(cohort_start_date<as.Date("2021-03-01")),
                     value.prior.obs.required="Yes",
                     value.pop.type="Before March",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)

  # from march
  IR.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "from.march")]]<-
    get.IR.summaries(working.data=working.Pop%>%
                       filter(prior_obs_years>=1)%>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w1=working.Pop.w1%>%
                       filter(prior_obs_years>=1) %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w2=working.Pop.w2%>%
                       filter(prior_obs_years>=1) %>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     working.data.w3=working.Pop.w3 %>%
                       filter(prior_obs_years>=1)%>%
    filter(cohort_start_date>=as.Date("2021-03-01")),
                     value.prior.obs.required="Yes",
                     value.pop.type="From March",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)
  
  }

# Summarise survival ------

get.Surv.summaries<-function(working.data,
                           value.prior.obs.required,
                           value.pop.type,
                           value.working.outcome,
                           value.working.outcome.name,
                           value.working.study.cohort, 
                           working.times){
  # add to working.Survival.summary
    working.Survival.summary<-list()
  
#tidy
s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ 1, 
                data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                 "overall")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group="overall")%>% 
  mutate(strata="overall") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ gender, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","gender")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)


s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ cond.comp, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","cond.comp")]]<-data.frame(
                                   time=s$time,
                                   n.risk=s$n.risk,
                                   n.event= s$n.event,
                                   surv=s$surv,
                                   lower=s$lower,
                                   upper=s$upper,
                                   group=s$strata)%>% 
  mutate(strata="cond.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ drug.comp, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","drug.comp")]]<-data.frame(
                                   time=s$time,
                                   n.risk=s$n.risk,
                                   n.event= s$n.event,
                                   surv=s$surv,
                                   lower=s$lower,
                                   upper=s$upper,
                                   group=s$strata)%>% 
  mutate(strata="drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ cond.drug.comp, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","cond.drug.comp")]]<-data.frame(
                                   time=s$time,
                                   n.risk=s$n.risk,
                                   n.event= s$n.event,
                                   surv=s$surv,
                                   lower=s$lower,
                                   upper=s$upper,
                                   group=s$strata)%>% 
  mutate(strata="cond.drug.comp") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr+gender, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr_gender")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required) %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr2+gender, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr2_gender")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr2_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)   %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3+gender, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr3_gender")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr3_gender") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)   %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)  %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr2, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr2")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr2") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)   %>% 
  mutate(pop=value.working.study.cohort)

s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3, 
                   data = working.data), times = working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                 ";","age_gr3")]]<-data.frame(
              time=s$time,
              n.risk=s$n.risk,
              n.event= s$n.event,
              surv=s$surv,
              lower=s$lower,
              upper=s$upper,
              group=s$strata)%>% 
  mutate(strata="age_gr3") %>% 
  mutate(outcome=value.working.outcome) %>% 
  mutate(outcome.name=value.working.outcome.name) %>% 
  mutate(pop.type=value.pop.type) %>% 
  mutate(prior.obs.required=value.prior.obs.required)   %>% 
  mutate(pop=value.working.study.cohort)

bind_rows(working.Survival.summary,.id = NULL)

  
}


if(working.study.cohort.type!="GeneralPopCohorts"){  # not for GeneralPopCohorts 
# no prior obs
# all pop
if(working.study.cohort.type== "VaccinatedCohorts"){
working_times<-c(0, 7, 14, 21)
}
if(working.study.cohort.type== "CovidCohorts"){
working_times<-c(0, 14, 21, 90)
  }
   
   
         
  
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "pop.all")]]<-
get.Surv.summaries(working.data=working.Pop,
                 value.prior.obs.required="No",
                 value.pop.type="All",
                 value.working.outcome=working.outcome,
                 value.working.outcome.name=working.outcome.name,
                 value.working.study.cohort=working.study.cohort,
                 working.times = working_times)

if(working.study.cohort.type== "VaccinatedCohorts"){
  # no prior obs
  #  before March
  Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","no.prior.obs", ";", "before.march")]]<-
    get.Surv.summaries(working.data=working.Pop %>%
                         filter(cohort_start_date<as.Date("2021-03-01")),
                       value.prior.obs.required="No",
                       value.pop.type="Before March",
                       value.working.outcome=working.outcome,
                       value.working.outcome.name=working.outcome.name,
                       value.working.study.cohort=working.study.cohort,
                       working.times = working_times)
    #  from March
  Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","no.prior.obs", ";", "from.march")]]<-
    get.Surv.summaries(working.data=working.Pop %>%
                         filter(cohort_start_date>= as.Date("2021-03-01")),
                       value.prior.obs.required="No",
                       value.pop.type="From March",
                       value.working.outcome=working.outcome,
                       value.working.outcome.name=working.outcome.name,
                       value.working.study.cohort=working.study.cohort,
                       working.times = working_times)

}


# prior obs
# all pop
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "pop.all")]]<-
get.Surv.summaries(working.data=working.Pop %>% 
                              filter(prior_obs_years>=1),
                 value.prior.obs.required="Yes",
                 value.pop.type="All",
                 value.working.outcome=working.outcome,
                 value.working.outcome.name=working.outcome.name,
                 value.working.study.cohort=working.study.cohort,
                 working.times = working_times)

if(working.study.cohort.type== "VaccinatedCohorts"){
  # prior obs
  #  before March
  Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "before.march")]]<-
    get.Surv.summaries(working.data=working.Pop %>%
                         filter(prior_obs_years>=1) %>%
                         filter(cohort_start_date<as.Date("2021-03-01")),
                       value.prior.obs.required="Yes",
                       value.pop.type="Before March",
                       value.working.outcome=working.outcome,
                       value.working.outcome.name=working.outcome.name,
                       value.working.study.cohort=working.study.cohort,
                       working.times = working_times)
    #  from March
  Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","prior.obs", ";", "from.march")]]<-
    get.Surv.summaries(working.data=working.Pop %>%
                         filter(prior_obs_years>=1) %>%
                         filter(cohort_start_date>= as.Date("2021-03-01")),
                       value.prior.obs.required="Yes",
                       value.pop.type="From March",
                       value.working.outcome=working.outcome,
                       value.working.outcome.name=working.outcome.name,
                       value.working.study.cohort=working.study.cohort,
                       working.times = working_times)
  
  

}

}



}
}



pat.ch<-list()
pat.ch[[1]]<-Pop.summary.characteristics %>%
  mutate(pop.type="All") %>% 
  mutate(prior.obs.required="No") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="All")
pat.ch[[2]]<-Pop.summary.characteristics.age_gr2.1 %>%
  mutate(pop.type="All") %>% 
  mutate(prior.obs.required="No") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="20-44")
pat.ch[[3]]<-Pop.summary.characteristics.age_gr2.2 %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="No") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="45-64")
pat.ch[[4]]<- Pop.summary.characteristics.age_gr2.3 %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="No") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2=">=65")
pat.ch[[5]]<-Pop.summary.characteristics.with.history %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="Yes") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="All") 
pat.ch[[6]]<- Pop.summary.characteristics.age_gr2.1.with.history %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="Yes") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="20-44")
pat.ch[[7]]<- Pop.summary.characteristics.age_gr2.2.with.history %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="Yes") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="45-64")
pat.ch[[8]]<-    Pop.summary.characteristics.age_gr2.3.with.history %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="Yes") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2=">=65")
  
 
if(working.study.cohort.type== "VaccinatedCohorts"){
pat.ch[[9]]<- Pop.summary.characteristics.before.march %>%
    mutate(pop.type="before.march") %>%
    mutate(prior.obs.required="No") %>%
    mutate(pop=working.study.cohort) %>%
    mutate(age_gr2="All")
pat.ch[[10]]<-Pop.summary.characteristics.before.march.age_gr2.1 %>%
    mutate(pop.type="before.march") %>%
    mutate(prior.obs.required="No") %>%
    mutate(pop=working.study.cohort) %>%
    mutate(age_gr2="20-44")
pat.ch[[11]]<-Pop.summary.characteristics.before.march.age_gr2.2 %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="45-64")
pat.ch[[12]]<-Pop.summary.characteristics.before.march.age_gr2.3 %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2=">=65")

pat.ch[[13]]<- Pop.summary.characteristics.from.march %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="All")
pat.ch[[14]]<- Pop.summary.characteristics.from.march.age_gr2.1 %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="20-44")
pat.ch[[15]]<- Pop.summary.characteristics.from.march.age_gr2.2 %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="45-64")
pat.ch[[16]]<- Pop.summary.characteristics.from.march.age_gr2.3 %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="No") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2=">=65")


pat.ch[[21]]<- Pop.summary.characteristics.before.march.with.history %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="All")
pat.ch[[22]]<- Pop.summary.characteristics.before.march.age_gr2.1.with.history %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="20-44")
pat.ch[[23]]<- Pop.summary.characteristics.before.march.age_gr2.2.with.history %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="45-64")
pat.ch[[24]]<- Pop.summary.characteristics.before.march.age_gr2.3.with.history %>%
      mutate(pop.type="before.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2=">=65")

    pat.ch[[25]]<- Pop.summary.characteristics.from.march.with.history %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="All")
    pat.ch[[26]]<- Pop.summary.characteristics.from.march.age_gr2.1.with.history %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="20-44")
    pat.ch[[27]]<- Pop.summary.characteristics.from.march.age_gr2.2.with.history %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2="45-64")
    pat.ch[[28]]<- Pop.summary.characteristics.from.march.age_gr2.3.with.history %>%
      mutate(pop.type="from.march") %>%
      mutate(prior.obs.required="Yes") %>%
      mutate(pop=working.study.cohort) %>%
      mutate(age_gr2=">=65")

}

Patient.characteristcis[[paste0(i)]]<-bind_rows(pat.ch)



}

}

IR.summary<-bind_rows(IR.summary, .id = NULL)
IR.summary$db<-db.name

if(run.vax.cohorts==TRUE | run.covid.cohorts==TRUE){
Survival.summary<-bind_rows(Survival.summary, .id = NULL)
Survival.summary$db<-db.name
Survival.summary<-Survival.summary %>% 
  group_by(group, strata, outcome,pop, pop.type,
           outcome.name,prior.obs.required) %>% 
  mutate(cum.n.event=cumsum(n.event))
}

# save ----
save(IR.summary, file = paste0(output.folder, "/IR.summary_", db.name, ".RData"))
save(Patient.characteristcis, file = paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData"))

if(run.vax.cohorts==TRUE | run.covid.cohorts==TRUE){
  save(Survival.summary, file = paste0(output.folder, "/Survival.summary_", db.name, ".RData"))  
}


# # zip results
print("Zipping results to output folder")
unlink(paste0(output.folder, "/OutputToShare_", db.name, ".zip"))
zipName <- paste0(output.folder, "/OutputToShare_", db.name, ".zip")

if(run.vax.cohorts==TRUE | run.covid.cohorts==TRUE){
files<-c(paste0(output.folder, "/IR.summary_", db.name, ".RData"),
         paste0(output.folder, "/Survival.summary_", db.name, ".RData"),
         paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData"))
} else {
  files<-c(paste0(output.folder, "/IR.summary_", db.name, ".RData"),
           paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData")) 
}
files <- files[file.exists(files)==TRUE]
createZipFile(zipFile = zipName,
              rootFolder=output.folder,
              files = files)

print("Done!")
print("-- If all has worked, there should now be a zip folder with your results in the output folder to share")
print("-- Thank you for running the study!")


