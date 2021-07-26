# link to db tables -----
person_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".person")))
observation_period_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       cdm_database_schema,
                       ".observation_period")))
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


# first dose cohorts
start.date<-as.Date("27/12/2021", "%d/%m/%y")
cohort.to.instantiate<-drug_exposure_db %>% 
      filter(drug_concept_id %in% c("37003436","724905","37003518", "739906")) %>% 
      filter(drug_exposure_start_date>={{start.date}}) %>% 
      collect()
cohort.to.instantiate<-cohort.to.instantiate %>% 
      group_by(person_id)%>% 
      arrange(person_id,drug_exposure_start_date) %>% 
      mutate(seq=1:length(person_id))
table(cohort.to.instantiate$seq)
cohort.to.instantiate<-cohort.to.instantiate %>% 
      filter(seq==1)
table(cohort.to.instantiate$drug_concept_id)

# add to cdm results schema
conn <- connect(connectionDetails)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".","CovVaxExposuresAll"),
            data=cohort.to.instantiate %>% 
              select(drug_concept_id, person_id, drug_exposure_start_date),
            createTable = TRUE,
            progressBar=TRUE)
disconnect(conn)    
    
CovVaxExposuresAll_db<-tbl(db, sql(paste0("SELECT * FROM ",
                       results_database_schema,
                       ".", "CovVaxExposuresAll")))


Pop<-person_db %>% 
  inner_join(CovVaxExposuresAll_db) %>% 
  select(person_id,gender_concept_id, 
            year_of_birth, month_of_birth, day_of_birth,
            drug_exposure_start_date, drug_concept_id)  %>% 
  left_join(observation_period_db %>% 
     select("person_id",  "observation_period_start_date", "observation_period_end_date") %>% 
       distinct()) %>% 
     collect() 
 
# add age -----
Pop$age<- NA
if(sum(is.na(Pop$day_of_birth))==0 & sum(is.na(Pop$month_of_birth))==0){
 # if we have day and month 
Pop<-Pop %>%
  mutate(age=floor(as.numeric((ymd(drug_exposure_start_date)-
                    ymd(paste(year_of_birth,
                                month_of_birth,
                                day_of_birth, sep="-"))))/365.25))
} else { 
Pop<-Pop %>% 
  mutate(age= year(drug_exposure_start_date)-year_of_birth)
}


Pop<-Pop %>% 
  filter(age>20)

Pop<-Pop %>% 
      filter(drug_exposure_start_date>=as.Date("27/12/2021", "%d/%m/%y"))

Pop$cohort<-NA
Pop<-Pop %>% 
  mutate(cohort=ifelse(drug_concept_id=="37003436", "BNT162b2", cohort)) %>% 
  mutate(cohort=ifelse(drug_concept_id=="724905", "ChAdOx1", cohort)) %>% 
  mutate(cohort=ifelse(drug_concept_id=="37003518", "mRNA-1273", cohort)) %>% 
  mutate(cohort=ifelse(drug_concept_id=="739906", "Ad26.COV2.S", cohort)) 

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


Pop<-Pop %>% 
  mutate(age_gr2=ifelse(age<=44,  "<44",
                 ifelse(age>=45 & age<=64,  "45-64",    
                 ifelse(age>=55, ">=65",
                       NA)))) %>% 
  mutate(age_gr2= factor(age_gr2, 
                   levels = c("<44", "45-64",">=65")))
table(Pop$age_gr2, useNA = "always")

Pop<-Pop %>% 
  mutate(age_gr3=  ifelse(age>=20 &  age<=29,  "20-39",
                ifelse(age>=30 &  age<=39,  "30-39",
                ifelse(age>=40 &  age<=49,  "40-49",
                ifelse(age>=50 &  age<=59,  "50-59",
                ifelse(age>=60 & age<=69,  "60-69",
                ifelse(age>=70 & age<=79,  "70-79",      
                ifelse(age>=80, ">=80",
                       NA)))))))) %>% 
  mutate(age_gr3= factor(age_gr3, 
                   levels = c("20-39","30-39","40-49", "50-59",
                              "60-69", "70-79",">=80")))
table(Pop$age_gr3, useNA = "always")



# cohort entry by time ------
Pop %>% 
  ggplot(aes(drug_exposure_start_date))+
  facet_wrap(vars(cohort),scales = "free_y")+
  geom_histogram(binwidth = 1,colour="black", fill="grey" )+
  theme_bw()

Pop %>% 
  filter(cohort %in% c("BNT162b2","ChAdOx1") ) %>% 
  ggplot(aes(drug_exposure_start_date))+
  facet_grid(age_gr2 ~ cohort,scales = "free_y")+
  geom_histogram(binwidth = 1,colour="black", fill="grey" )+
  theme_bw()

Pop %>% 
  filter(cohort %in% c("BNT162b2","ChAdOx1") ) %>% 
  ggplot(aes(drug_exposure_start_date))+
  facet_grid(age_gr3 ~ cohort,scales = "free_y")+
  geom_histogram(binwidth = 1,colour="black", fill="grey" )+
  theme_bw()


Pop %>% 
  filter(!cohort %in% c("BNT162b2","ChAdOx1") ) %>% 
  ggplot(aes(drug_exposure_start_date))+
  facet_grid(age_gr3 ~ cohort,scales = "free_y")+
  geom_histogram(binwidth = 1,colour="black", fill="grey" )+
  theme_bw()



# cohorts by age and sex
Pop %>% 
  ggplot(aes(age))+
  facet_wrap(vars(cohort),scales = "free_y")+
  geom_histogram(binwidth = 1,colour="black", fill="grey" )+
  theme_bw()
