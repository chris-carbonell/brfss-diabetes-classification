# Overview
# - common tools to help scrub yearly data

# general
import json
import os

# data
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# config
import config_scrub as cs

# Funcs

def get_df_YYYY(year, nrows=None):
    df_YYYY = pd.read_csv(f"./data/brfss{year}.csv", encoding="cp1252", nrows=nrows, low_memory=False)
    if "Unnamed: 0" in df_YYYY.columns:
        df_YYYY.drop("Unnamed: 0", inplace=True, axis=1)
    return df_YYYY

def check_valid_col_mapping(df_YYYY, dict_YYYY_col_names, bool_print = False):
    
    # easy
    # return all(val in df_YYYY.columns for key, val in dict_YYYY_col_names.items())
    
    # document
    bool_valid = True
    bool_replacement = False
    for key, val in dict_YYYY_col_names.items():
        if val not in df_YYYY.columns:
            # identify missing
            bool_valid = False
            
            # attempt to find replacement
            ls_replacements = []
            for col in df_YYYY.columns:
                if col.startswith(val[:-1]):
                    ls_replacements.append(col)
            
            if bool_print:
                if len(ls_replacements) > 0:
                    print(f"{val} ({ls_replacements})")
                else:
                    print(f"{val} (None)")
                
    return bool_valid

def standardize_df_YYYY(df_YYYY, dict_YYYY_col_names):
    
    # drop everything else
    df_YYYY = df_YYYY[[col for col in dict_YYYY_col_names.values()]]

    # rename columns
    dict_rename = {v: k for k, v in dict_YYYY_col_names.items()}
    df_YYYY = df_YYYY.rename(columns=dict_rename)
        
    return df_YYYY

def scrub_df_YYYY_col(year, df_YYYY, col, dict_replace, fill_na):
    
    # replace
    if dict_replace is not None:
        df_YYYY[col] = df_YYYY.loc[:, col].replace(dict_replace)

    # fill_na
    if fill_na is not None:
        df_YYYY[col] = df_YYYY.loc[:, col].fillna(fill_na)
            
    return

def find_col(df, starts_with):
    ls_cols = list(df.columns)
    return [col for col in ls_cols if col.lower().startswith(starts_with)]

def parse_weight(weight):
    '''
    based on 2014

    return weight in lbs
    '''

    if weight < 50:
        return np.nan
    elif weight < 9000:
        return weight
    elif weight >= 9000 and weight < 9999:
        weight_kg = int(float(str(weight)[1:]))
        return weight_kg * 2.20462  # kg to lbs
    else:
        return None

def parse_alcohol(alcday):
    '''
    based on 2020

    return days in past 30 days
    '''
    
    if pd.isna(alcday):
        return np.nan

    ind_alc = str(alcday)[0]
    num_days = int(float(str(alcday)[1:]))

    # get days in pas 30 days
    if ind_alc == "0":
        return 0
    elif ind_alc == "1":
        # 1 = days per week
        return num_days * 4
    elif ind_alc == "2":
        # 2 = days in past 30 days
        return num_days
    else:
        return np.nan