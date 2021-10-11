# Overview

This repo accomplishes a couple of objectives:
* scrubs the <b>Behavioral Risk Factor Surveillance System (BRFSS)</b> data collected by the CDC and 
* classifies BRFSS respondents as diabetic or not using a logistic regression classifier and a random forest classifier

# BFRSS Data

For these analyses, I focused on the following features:
* income (e.g., >50k)
* race (e.g., asian/paciific islander)
* state (FIPS code e.g., 55 = Wisconsin)
* age (e.g., 18-24)
* height (inches e.g., 66)
* weight (pounds e.g., 175)
* biological gender (e.g., male)

# Prerequisites
* R 4.1.1 (and RStudio v1.4)
* Python 3.9.0

# Scrubbing the Data

The CDC provides BFRSS data in the XPT format since they use SAS.

I used <code>scrub/convert_xpt_to_csv.R</code> to convert the XPTs to CSVs. Lastly, I used <code>scrub/scrub_data.py</code> to scrub the data and save it as a Parquet.

The size of all of the Parquets (one for every year from 1986 to 2020) amounts to about 48.5 MB so I just provided the data for 2020 here as an example. Similarly, the source XPT and CSV are too big to share here. However, you can obviously download the XPTs straight from the CDC (see link below).

Along with the data, the CDC provides a codebook to define columns and spell out the possible values within those columns and their meanings. For example, the biological gender column changed from "sex" to "sex1" to "sexvar" between 2017 and 2019.

# Classifying Diabetes

I trained two basic models to classify respondents as diabetic or not:
* a logistic regression classifier
* a random forest classifier 

To keep things simple, my overarching goals here were to achieve parameter convergence and create a model I could easily store on disk (i.e., does not take up too much space). Therefore, in training the logistic regression model, I set the maximum iterations high enough to allow the model paramters to converge. In training the random forest model, I limited the maximum depth of the decision trees to ten. Otherwise, the resulting model required too much disk space.

With these practical limitations, the <b>logistic regression classifer</b> provided a more accurate model with an overall <b>accuracy of 85.3%</b> (sensitivity = 13.9%, specificity = 98.0%).

# Resources
* CDC BFRSS<br>
https://www.cdc.gov/brfss/annual_data/annual_data.htm