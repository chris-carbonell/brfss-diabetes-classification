# Dependencies

# general
from joblib import dump, load

# data
import numpy as np
import pandas as pd
import pyarrow as pa

# preprocessing
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler

# validation
from sklearn.metrics import confusion_matrix
import scikitplot as skplt

# viz
import matplotlib.pyplot as plt

# Constants

data_dir = "./data/"
model_dir = "./model/"

# Funcs

def calc_cm_stats(arr_cm):
    '''
    calc summary stats for confusion matrix
    
    arr_cm (np.array): confusion matrix
    - arr_cm = confusion_matrix(y_test, lr_y_predict, labels=[1,0])
    - e.g., array([ [494086,   9950],
                    [ 77594,  12548]], dtype=int64)
    '''
    
    # https://en.wikipedia.org/wiki/Confusion_matrix
    
    tp = arr_cm[0,0]  # true positive
    fp = arr_cm[1,0]  # false positive
    fn = arr_cm[0,1]  # false negative
    tn = arr_cm[1,1]  # true negative
    
    total = tp + fp + fn + tn
    
    accuracy = (tp + tn) / total  # same as model.score(x_test, y_test)
    
    sensitivity = tp / (tp + fn)  # aka true positive rate, recall, power
    specificity = tn / (tn + fp)  # aka true negative rate, selectivity
    
    return {
        'total': total,
        'tp': tp,
        'fp': fp,
        'fn': fn,
        'tn': tn,
        'accuracy': accuracy,
        'sensitivity': sensitivity,
        'specificity': specificity
    }

if __name__ == "__main__":

	# Get Data

	df_orig = pd.read_parquet(data_dir + "diabetes.parquet")
	df_orig.head()

	# Get Split Data

	x_train = np.load(data_dir + "x_train.npy")
	y_train = np.load(data_dir + "y_train.npy")

	x_test = np.load(data_dir + "x_test.npy")
	y_test = np.load(data_dir + "y_test.npy")

	x_validate = np.load(data_dir + "x_validate.npy")
	y_validate = np.load(data_dir + "y_validate.npy")

	# Get Model

	# get random forest model
	model_rf = load(model_dir + "model_rf.joblib")

	# Stats

	# calc stats
	# "Calculate the posterior probability for each class."
	# "Create a "Likelihood" table by finding relevant probabilities"
	dict_rf_cm_stats = calc_cm_stats(rf_arr_cm)
	print(dict_rf_cm_stats)
	print()

	# confusion matrix
	# "Calculate the posterior probability by converting the dataset into a frequency table"
	rf_y_predict = model_rf.predict(x_test)
	rf_arr_cm = confusion_matrix(y_test, rf_y_predict, labels=[1,0])
	print(rf_arr_cm)
	print()