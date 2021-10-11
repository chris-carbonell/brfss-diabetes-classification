import numpy as np

# output
def get_parquet_filename(year, extension=".parquet"):
    return f"./clean/brfss_{year}{extension}"

# scrub config
dict_col_names = {
    # default = if not specified by year, use this
    "default": {
        "scrub": {
            "metric": True,
            "replace": {
                "income": {1: "<10k", 2: "10k-15k", 3: "15k-20k", 4: "20k-25k", 5: "25k-35k", 6: "35k-50k", 7: "50k-75k", 8: ">75k", 77: "35k-50k", 99: "35k-50k"},
                "race": {1: "white", 2: "black", 3: "native american", 4: "asian/pacific islander", 5: "asian/pacific islander", 6: "other/multiracial", 7: "other/multiracial", 8: "hispanic", 9: np.nan},
                "age": {1: "18-24", 2: "25-29", 3: "30-34", 4: "35-39", 5: "40-44", 6: "45-49", 7: "50-54", 8: "55-59", 9: "60-64", 10: "65-69", 11: "70-74", 12: "75-79", 13: "80+", 14: np.nan},
                "sex": {1: "male", 2: "female", 7: np.nan, 9: np.nan},
                "state": {1: "AL", 2: "AK", 4: "AZ", 5: "AR", 6: "CA", 8: "CO", 9: "CT", 10: "DE", 11: "DC", 12: "FL", 13: "GA", 15: "HI", 16: "ID", 17: "IL", 18: "IN", 19: "IA", 20: "KS", 21: "KY", 22: "LA", 23: "ME", 24: "MD", 25: "MA", 26: "MI", 27: "MN", 28: "MS", 29: "MO", 30: "MT", 31: "NE", 32: "NV", 33: "NH", 34: "NJ", 35: "NM", 36: "NY", 37: "NC", 38: "ND", 39: "OH", 40: "OK", 41: "OR", 42: "PA", 44: "RI", 45: "SC", 46: "SD", 47: "TN", 48: "TX", 49: "UT", 50: "VT", 51: "VA", 53: "WA", 54: "WV", 55: "WI", 56: "WY", 66: "GU", 72: "PR", 78: "VI"},
                # "height": {7777: np.nan, 9999: np.nan},
                "weight": {7777: np.nan, 9999: np.nan},
                "general_health": {1: "excellent", 2: "very good", 3: "good", 4: "fair", 5: "poor", 7: np.nan, 9: np.nan},
                "doctor": {1: "yes", 2: "yes", 3: "no", 7: np.nan, 9: np.nan},
                "medical_costs": {1: "yes", 2: "no", 7: np.nan, 9: np.nan},
                "checkup": {1: "1 year", 2: "2 years", 3: "5 years", 4: ">5 years", 7: "unknown", 8: "never", 9: np.nan},
                "exercise": {1: "yes", 2: "no", 7: np.nan, 9: np.nan},
                "sleep": {1: "1-3", 2: "1-3", 3: "1-3", 4: "4-6", 5: "4-6", 6: "4-6", 7: "7-8", 8: "7-8", 9: "9-10", 10: "9-10", 11: "11-12", 12: "11-12", 13: ">12", 14: ">12", 15: ">12", 16: ">12", 17: ">12", 18: ">12", 19: ">12", 20: ">12", 21: ">12", 22: ">12", 23: ">12", 24: ">12", 77: "7-8", 99: "7-8"},
                "marital": {1: "married", 2: "divorced", 3: "widowed", 4: "separated", 5: "single", 6: "living together", 9: np.nan},
                "education": {1: "none", 2: "1-8", 3: "9-11", 4: "12/ged", 5: "c1-3", 6: "cg", 9: np.nan},
                "smoking": {1: "yes", 2: "yes", 3: "no", 7: np.nan, 9: np.nan},
                "alcohol": {777: np.nan, 888: 0, 999: np.nan},
                "diabetes": {1: "yes", 2: "yes", 3: "no", 4: "yes", 7: np.nan, 9: np.nan}
            },
            "fill_na": {
                "smoking": "no",
                "alcohol": 0  # 2006-2010 had bad data (probably due to conversion; the drinkany field usually has majority at 0 anyway)
            }
        }
    },

    "2006": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin3",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup",
            "exercise": "exerany2",
            # "sleep": "sleptime",  # not in data
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday4",
            "diabetes": "diabete2"
        },
        "scrub": {
            "replace": {
                "height": {999: np.nan},
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2007": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin3",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",  # not in data
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday4",
            "diabetes": "diabete2"
        },
        "scrub": {
            "replace": {
                "height": {999: np.nan},
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2008": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin3",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",  # not in data
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday4",
            "diabetes": "diabete2"
        },
        "scrub": {
            "replace": {
                "height": {999: np.nan},
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2009": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin3",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday4",
            "diabetes": "diabete2"
        },
        "scrub": {
            "replace": {
                "height": {999: np.nan},
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2010": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin3",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday4",
            "diabetes": "diabete2"
        },
        "scrub": {
            "replace": {
                "height": {999: np.nan},
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2011": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        },
        "scrub": {
            "replace": {
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2012": {
        "fields": {
            "income": "income2",
            "race": "race2",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptime",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        },
        "scrub": {
            "replace": {
                "race": {1:'white', 2:'black', 3:'asian/pacific islander', 4:'asian/pacific islander', 5:'native american', 6:'other/multiracial', 7:'other/multiracial', 8:'hispanic', 9:'refused/unknown'}
            }
        }
    },

    "2013": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2014": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2015": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",  # not in data
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2016": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2017": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2018": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sex1",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete3"
        }
    },

    "2019": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sexvar",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",  # not in data
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete4"
        }
    },

    "2020": {
        "fields": {
            "income": "income2",
            "race": "x.race",
            "state": "x.state",
            "age": "x.ageg5yr",
            "sex": "sexvar",
            "height": "htin4",
            "weight": "weight2",
            "general_health": "genhlth",
            "doctor": "persdoc2",
            "medical_costs": "medcost",
            "checkup": "checkup1",
            "exercise": "exerany2",
            # "sleep": "sleptim1",
            "marital": "marital",
            "education": "educa",
            "smoking": "smokday2",
            "alcohol": "alcday5",
            "diabetes": "diabete4"
        }
    }
}