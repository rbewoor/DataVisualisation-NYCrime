#
# Regression testing for Data Story telling project
#
setwd("D:/EverythingD/01SRH-BDBA Acads/RWD/DataStoryTelling-201909")

# load external data

d <-  read.csv("RegR-outCSVFileSHORT-NoKY_CD-Sorted.csv", header=TRUE, na.strings ="MISSING")

# see column names and the dimensions
names(d)
dim(d)
str(d)

# see the scatter plot
pairs(d)

######## for regression

#Model train and test
# trainedModel <- lm(d1NoOut$Y~d1NoOut$X1, data = trainingSet)
# trainedModel <- lm(d1NoOut$Y~d1NoOut$X1 + d1NoOut$X2, data = trainingSet)
# remove an existing variable from the model
#trainedModel1 <- lm(Y~.-Variable_to_be_removed ,data=origIpData1NoOut )
#or
#trainedmodel1 = update(trainedmodel, ~.-Variable_to_be_removed)

## dependant as crimeTotal

### Training and testing set
library(caTools)
sample.split(d$crimeTotal, SplitRatio = 0.80)->splitDF1
subset(d, splitDF1==T)->trainingSet1
subset(d,splitDF1==F)->testSet1

## model 1 :   crimeTotal = countPedes
slm1 <- lm(d$crimeTotal ~ d$countPedes , data = trainingSet1)
summary(slm1)

## model 2 :   crimeTotal = countPortNYnNewark
slm2 <- lm(d$crimeTotal ~ d$countPortNYnNewark , data = trainingSet1)
summary(slm2)

## model 3 :   crimeTotal = countPedes + countPortNYnNewark
mlm3 <- lm(d$crimeTotal ~ d$countPedes + d$countPortNYnNewark , data = trainingSet1)
summary(mlm3)


###
## dependant as sumCrimeStreet.Prem61

### Training and testing set
#library(caTools)
sample.split(d$sumCrimeStreet.Prem61, SplitRatio = 0.80)->splitDF2
subset(d, splitDF2==T)->trainingSet2
subset(d,splitDF2==F)->testSet2

## model 4 :   sumCrimeStreet.Prem61 = countPedes
slm4 <- lm(d$sumCrimeStreet.Prem61 ~ d$countPedes , data = trainingSet2)
summary(slm4)

## model 5 :   sumCrimeStreet.Prem61 = countPortNYnNewark
slm5 <- lm(d$sumCrimeStreet.Prem61 ~ d$countPortNYnNewark , data = trainingSet2)
summary(slm5)


## model 6 :   sumCrimeStreet.Prem61 = countPedes + countPortNYnNewark
mlm6 <- lm(d$sumCrimeStreet.Prem61 ~ d$countPedes + d$countPortNYnNewark , data = trainingSet2)
summary(mlm6)



###
## dependant as countPedes

### Training and testing set
#library(caTools)
sample.split(d$countPedes, SplitRatio = 0.80)->splitDF3
subset(d, splitDF3==T)->trainingSet3
subset(d,splitDF3==F)->testSet3

## model 7 :   countPedes = countPortNYnNewark
slm7 <- lm(d$countPedes ~ d$countPortNYnNewark , data = trainingSet3)
summary(slm7)



##########################################


#### remove a column
#d1 <- d1[ -c(14) ]
#### remove the variable
#rm(d1)
