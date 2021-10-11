# Overview
# - convert XPT to CSV

options(stringsAsFactors=F, scipen = 999)

pkg = 'Hmisc'
if (!require(pkg, character.only = TRUE)) {
  install.packages(pkg)
  library(pkg, character.only = TRUE)
}

files <- list.files(path="../data/xpt", all.files=TRUE, full.names=TRUE)
lapply(files, function(x) {
    brfss <- sasxport.get(x)  # input XPT path
	write.csv(brfss, file=paste("../data/csv/", x, ".csv", sep=""))  # output CSV path
})