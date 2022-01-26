# Dependencies

# general
from joblib import dump, load

# data
import numpy as np
import pandas as pd

# model
from sklearn.ensemble import RandomForestClassifier

# Constants

data_dir = "./data/"
model_dir = "./model/"

# Get Data

x_train = np.load(data_dir + "x_train.npy")
y_train = np.load(data_dir + "y_train.npy")

# x_test = np.load(data_dir + "x_test.npy")
# y_test = np.load(data_dir + "y_test.npy")

# x_validate = np.load(data_dir + "x_validate.npy")
# y_validate = np.load(data_dir + "y_validate.npy")

# Train Model
model_rf = RandomForestClassifier(random_state=0, max_depth=10)
model_rf.fit(x_train, y_train)

# Save M odel
dump(model_rf, model_dir + "model_rf.joblib")