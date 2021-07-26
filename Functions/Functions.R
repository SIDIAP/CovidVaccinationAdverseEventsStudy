# printing numbers with 1 decimal place and commas 
nice.num<-function(x) {
  trimws(format(round(x,1),
         big.mark=",", nsmall = 1, digits=1, scientific=FALSE))}
# printing numbers with 2 decimal place and commas 
nice.num2<-function(x) {
  trimws(format(round(x,2),
         big.mark=",", nsmall = 2, digits=2, scientific=FALSE))}
# for counts- without decimal place
nice.num.count<-function(x) {
  trimws(format(x,
         big.mark=",", nsmall = 0, digits=0, scientific=FALSE))}

# tidy up tableone output
TidyTableOne<-function(working.table){
# format 
for(i in 1:ncol({{working.table}})) {

  cur_column <- working.table[, i]
  cur_column <- str_extract(cur_column, '[0-9.]+\\b') %>% 
                as.numeric() 
  cur_column <-nice.num.count(cur_column)
  # add back in
  working.table[, i] <- str_replace(string={{working.table}}[, i], 
                              pattern='[0-9.]+\\b', 
                              replacement=cur_column)    
}

rownames(working.table)<-str_to_sentence(rownames(working.table))
rownames(working.table)<-str_replace(rownames(working.table),
                                      "Prior_obs_years", "Years of prior observation time")
rownames(working.table)<-str_replace(rownames(working.table),
                                      "Age_gr", "Age group")
rownames(working.table)<-str_replace(rownames(working.table),
                                      "Gender", "Sex")
rownames(working.table)<-str_replace(rownames(working.table),
                                      "Hrfs_gr", "Hospital Frailty Risk Score")
rownames(working.table)<-str_replace(rownames(working.table),
                                      "Copd", "COPD")
rownames(working.table)<-str_replace(rownames(working.table) , "_", " ")
rownames(working.table)<-str_replace(rownames(working.table) , "_", " ")
rownames(working.table)<-str_replace(rownames(working.table) , " = 1 ", " ")
rownames(working.table)<-str_replace(rownames(working.table) , "iqr", "IQR")
#return
working.table}
# get tableone
get.patient.characteristics<-function(Population, vars, factor.vars){
summary.table<-data.frame(print(CreateTableOne(
  vars =  vars,
  factorVars=factor.vars,
  includeNA=T,
  data = Population,
  test = F), 
  showAllLevels=F,smd=F,
  nonnormal = vars,
  noSpaces = TRUE,
  contDigits = 1,
  printToggle=FALSE)) 
TidyTableOne(summary.table)
}

# general formatting for figures
gg.general.format<-function(plot){
  plot+
  theme_bw()+
  theme(legend.title = element_blank(),
        axis.text=element_text(size=12),
        axis.title=element_text(size=12,face="bold"),
        legend.text=element_text(size=12)) }

gg.general.format.facet<-function(plot){
  plot+
  theme_bw()+
  scale_y_continuous(label=label_comma(accuracy= 1), position = "right", limits=c(0,NA))+
  theme(panel.spacing = unit(0.6, "lines"),
        legend.title = element_blank(),
        axis.text=element_text(size=12),
        axis.title=element_text(size=12,face="bold"),
        strip.text = element_text(size=12, face="bold"),
        strip.text.y.left = element_text(angle = 0),
        strip.background = element_rect( fill="#f7f7f7"),
      #  axis.title.y.right =  element_text(angle = 0),
        legend.text=element_text(size=12), 
        legend.position = "top") }

gg.general.format.facet.perc<-function(plot){
  plot+
  theme_bw()+
  scale_y_continuous(label=label_percent(accuracy= 1), position = "right", limits=c(0,NA))+
  theme(panel.spacing = unit(0, "lines"),
        legend.title = element_blank(),
        axis.text=element_text(size=14),
        axis.title=element_text(size=14,face="bold"),
        strip.text = element_text(size=14, face="bold"),
        strip.text.y.left = element_text(angle = 0),
        strip.background = element_rect( fill="#f7f7f7"),
      #  axis.title.y.right =  element_text(angle = 0),
        legend.text=element_text(size=14), 
        legend.position = "top") }



