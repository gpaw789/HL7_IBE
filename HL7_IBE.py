import hl7
import pandas as pd
import os
import pickle

def get_files(directory):
    list_of_files = []
    for files in next(os.walk(directory))[2]:
        list_of_files.append(directory+"\\"+files)
    return list_of_files

def parse_file(directory):
    with open(directory, "rb") as sfile:
        file_str = sfile.read()
    # remove non-unicode strings
    file_str = file_str.replace("\xb0", "")
    return file_str

def gen_single_df(h):
    # poll through each observation and put into df
    MRN = []; Last_name = []; First_name = []; Ward = [];
    Vital = []; Value = []; Unit = []; Message_sent = [];
    for i in range(8, 14, 1):
        try:
            MRN.append(h[1][3][1][0][0])
            Last_name.append(h[1][5][0][1][0])
            First_name.append(h[1][5][0][0][0])
            Ward.append(h[0][12][0])
            Vital.append(h[i][3][0][1][0])
            Value.append(h[i][5][0])
            Unit.append(h[i][6][0][1][0])
            Message_sent.append(h[3][7][0])
        except:
            continue
    if len(MRN) == len(Last_name) == len(First_name) == len(Ward) == len(Vital) == len(Value) == len(Unit) == len(Message_sent):
        pass
    else:
        return 0
    df_single = pd.DataFrame(
        {
            "MRN":MRN, "Last_name":Last_name, "First_name":First_name, "Ward":Ward,
            "Vital":Vital, "Value":Value, "Unit": Unit, "Message_sent":Message_sent
        }
    )
    return df_single

def main():
    # read each file in directory
    list_of_files = get_files(os.getcwd()+"\\test")
    # delete old dataframe pickle
    try:
        os.remove("df.p")
    except:
        pass
    # parse each file
    for file_directory in list_of_files:
        file_str = parse_file(file_directory)
        # parse into hl7
        try:
            h = hl7.parse(file_str)
        except:
            continue    #if skip if invalid
        # generate core pandas df
        df_single = gen_single_df(h)
        # check if its a dataframe, otherwise skip this file
        if type(df_single) == type(0):
            continue
        # combine with main dataframe pickle
        if os.path.exists("df.p"):
            df_main = pickle.load( open("df.p", "rb"))
            print(df_main.head())
            df_main = pd.concat([df_main, df_single])
            print(df_main.head())
            pickle.dump(df_main, open("df.p", "wb"))

        else:
            pickle.dump(df_single, open("df.p", "wb"))

    # save dataframe as csv file
    df_main = pickle.load(open("df.p", "rb"))
    df_main.to_csv("main.csv")


    return 0

main()