# Overview
# - scrub data
# - only applies to years with modern data

# general
import datetime as dt
import dill
import os
import pathlib
import sys

# config
import config_scrub as cs

# data
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# scrub
import scrub_tools as st

# viz
import matplotlib.pyplot as plt

# Funcs

def print_log(year, message):
    print(f'{dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")}, {year}: {message}')
    return

# get config
dict_col_names = cs.dict_col_names
dict_mapping_default = dict_col_names['default']

# applicable_years
ls_years = [year for year in dict_col_names.keys() if year != "default"]

# scrub
print()
for year in ls_years:

    # log
    print_log(year, f"starting")

    # if data scrubbed already, skip
    output_filename = cs.get_parquet_filename(year)
    output_filename_pkl = cs.get_parquet_filename(year, ".pkl")
    if os.path.exists(output_filename):
        print_log(year, f"skipping: output already exists ({output_filename})")
        print()
        continue
    elif os.path.exists(output_filename_pkl):
        print_log(year, f"skipping: debug awaiting ({output_filename_pkl})")
        print()
        continue
    else:
        pass

    # get data
    print_log(year, f"loading data")
    df_YYYY = st.get_df_YYYY(year)
    print_log(year, f"successfully loaded data")

    # check config
    dict_YYYY_col_names = dict_col_names[str(year)]['fields']
    bool_valid = st.check_valid_col_mapping(df_YYYY, dict_YYYY_col_names, bool_print=True)
    if not bool_valid:
        print_log(year, f"error: config not valid (bool_valid)")
        print()
        continue

    # standardize column names
    print_log(year, f"standardizing column names")
    df_YYYY = st.standardize_df_YYYY(df_YYYY, dict_YYYY_col_names)

    # scrub
    
    # replace and fill_na
    for col in df_YYYY.columns:

        # log
        print_log(year, f"replacing and fill_na-ing ({col})")

        # get year-specific mappings if available

        # dict_replace
        # try year then default otherwise none
        bool_debug = False
        try:
            dict_replace = dict_col_names[str(year)]['scrub']['replace'][col]
            if bool_debug:
                print_log(year, f"dict_replace (dict_col_names)")
        except:
            try:
                dict_replace = dict_mapping_default['scrub']['replace'][col]
                if bool_debug:
                    print_log(year, f"dict_replace (dict_mapping_default)")
            except:
                dict_replace = None
                if bool_debug:
                    print_log(year, f"dict_replace (None)")
        
        # dict_fill_na
        # try year then default otherwise none
        try:
            fill_na = dict_col_names[str(year)]['scrub']['fill_na'][col]
            if bool_debug:
                print_log(year, f"fill_na (dict_col_names)")
        except:
            try:
                fill_na = dict_mapping_default['scrub']['fill_na'][col]
                if bool_debug:
                    print_log(year, f"fill_na (dict_mapping_default)")
            except:
                fill_na = None
                if bool_debug:
                    print_log(year, f"fill_na (None)")

        # scrub
        st.scrub_df_YYYY_col(year, df_YYYY, col, dict_replace, fill_na)

    # calcs

    print_log(year, f"calculating special fields")

    # alcohol
    df_YYYY['alcohol'] = df_YYYY.apply(lambda x: st.parse_alcohol(x['alcohol']), axis=1)

    # weight
    df_YYYY['weight'] = df_YYYY.apply(lambda x: st.parse_weight(x['weight']), axis=1)

    # bmi
    df_YYYY['BMI_calc'] = df_YYYY.apply(lambda x: 703*x['weight']/(x['height']**2), axis=1)
    df_YYYY['BMI'] = pd.cut(
        df_YYYY['BMI_calc'], 
        [0, 15, 16, 18.5, 25, 30, 35, 40, float("inf")], 
        labels=["very severely underweight", "severely underweight", "underweight", "normal", "overweight", "obese class I", "obese class II", "obese class III"],
        right=False, 
        retbins=False
    )
    df_YYYY = df_YYYY.drop(columns='BMI_calc', axis=1)


    # # test
    # df_YYYY.to_csv(output_filename.replace(cs.get_parquet_filename(year, ".csv")), index=False)
    # sys.exit()

    print_log(year, f"finished calculating special fields")

    # visualize
    
    # make dir
    pathlib.Path(f"./viz/{year}/").mkdir(parents=True, exist_ok=True)

    # viz each col
    for col in df_YYYY.columns:

        # log
        print_log(year, f"attempting to creating viz ({col})")

        # set up output
        viz_title = f"{year}_{col}"
        viz_filename = f"./viz/{year}/{viz_title}.png"

        # get ax
        if str(df_YYYY[col].dtype) in ["float64", "int64"]:
            ax = df_YYYY[col].hist()
        elif str(df_YYYY[col].dtype) in ["category", "object"]:
            ax = df_YYYY[col].value_counts().plot(kind="bar")
        else:
            continue

        # build footnote

        int_null_count = df_YYYY[col].isna().sum()
        str_null_count = "{:,}".format(int_null_count)
        flt_null_perc = int_null_count / len(df_YYYY.index)
        str_null_perc = "{:.1%}".format(flt_null_perc)

        footnote = f"* null count = {str_null_count} ({str_null_perc})"

        if str(df_YYYY[col].dtype) in ["float64", "int64"]:
            col_min = round(df_YYYY[col].min(), 2)
            col_max = round(df_YYYY[col].max(), 2)
            col_avg = round(df_YYYY[col].mean(), 2)
            footnote += f"\n* avg = {col_avg}, min = {col_min}, max = {col_max}"

        # save
        ax.set_title(viz_title)
        ax.set_xlabel(col)
        ax.set_ylabel("counts")
        plt.figtext(0.01, 0.01, footnote, horizontalalignment="left")
        ax.figure.tight_layout()
        ax.figure.savefig(viz_filename)
        plt.clf()  # clear the entire figure

    # log
    print_log(year, f"finished creating viz")

    # save data
    
    print_log(year, f"attempting to save data to {output_filename}")
    
    try:
        tbl_YYYY = pa.Table.from_pandas(df_YYYY)
        pq.write_table(tbl_YYYY, output_filename)
        print_log(year, f"successfully saved data to {output_filename}")
    except Exception as e:

        # if error, save df so we can check it out
        print_log(year, f"saving data to {output_filename_pkl}")

        # error text
        with open(output_filename.replace(cs.get_parquet_filename(year, ".txt")), 'w') as file:
            file.write(repr(e))

        # pkl
        with open(output_filename_pkl, 'wb') as file:
            dill.dump(df_YYYY, file)

    print()