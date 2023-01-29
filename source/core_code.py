# import required modules
from pathlib import Path
import logging
import pandas as pd
import numpy as np
import os
import csv
# Set up root logger, and add a file handler to root logger
logging.basicConfig(filename='data_science.log',
                    level=logging.WARNING,
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')

logger = logging.getLogger("data_missing")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
# Set INFO level for handler
handler.setLevel(logging.INFO)
# Create a message format that matches earlier example
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Add our format to our handler
handler.setFormatter(formatter)
# Add our handler to our logger
logger.addHandler(handler)

# assign directory
directory = 'data'


def create_outputs():
    """This function cretas outputs csv files whose rows include data with '00' minutes data."""
    # iterate over files in
    # that directory
    folders = Path(directory).glob('*')
    first_next_month = pd.Series([], dtype=pd.StringDtype())
    for folder in folders:
        try:
            files = Path(str(folder)).glob('*')
            # Creating Empty DataFrame for an output
            out_df = pd.DataFrame(np.empty((0, 701)))

            for file in files:
                df = pd.read_csv(file, on_bad_lines='skip', index_col=False)
                out_df.columns = list(df.columns)
                if not first_next_month.empty and first_next_month['observe_time'][5:7] == str(folder)[10:]   :
                    out_df.loc[0] = first_next_month
                # TO DO :
                # Sorting by column 'observe_time' may required. Default is that they are sorted in decending order
                # df.sort_values(by=['observe_time'])
                for column in df.columns[:1]:

                    time_stamps = df[column].values
                    for idx, time_stamp in enumerate(time_stamps):
                        date = time_stamp[5:10]
                        time = time_stamp[12:25]
                        hour = time_stamp[11:13]
                        minute = time_stamp[14:16]
                        if minute == '00' and date[:2] == str(folder)[10:]:
                            if first_next_month.empty:
                                out_df.loc[len(out_df.index)] = df.iloc[idx]
                            else:
                                out_df.loc[len(out_df.index)+1] = df.iloc[idx]
                        if minute == '00' and date[:2] != str(folder)[10:]:  # The first hour of the next month is in the last file'
                            first_next_month = df.iloc[idx]

            Path("outputs").mkdir(parents=True, exist_ok=True)
            out_df.to_csv('outputs/'+str(folder)[5:]+'.csv', index=False, lineterminator='\n', errors='replace')

        except Exception as e:
            logger.error(f'Failed to prepare data due to {e} for file {file}')
    return None


def find_missed_hour(dir_path, missed_hour):
    for file in os.listdir(dir_path):
        cur_path = os.path.join(dir_path, file)
    # check if it is a file
        if os.path.isfile(cur_path):
            with open(cur_path, 'r') as fp:
                if missed_hour in fp.read():
                    print('string found')
                    with open(cur_path, 'rt') as f:
                        reader = csv.reader(f, delimiter=',')
                        for row in reader:
                            if row[0].find(missed_hour) != -1:
                                return row
    return []


def find_insert_missing_rows():
    """This function search and finds the missing data in the generated outputs and then inserts the missing data."""
    files = Path('outputs').glob('*')
    out_df = pd.DataFrame(np.empty((0, 701)))
    for file in files:
        
        df = pd.read_csv(file, on_bad_lines='skip', index_col=False)

        for column in df.columns[:1]:
            time_stamps = df[column].values
            current_hour = 0
            for idx, time_stamp in enumerate(time_stamps):
                time = time_stamp[12:25]
                hour = time_stamp[11:13]
                minute = time_stamp[14:16]
                if int(hour) != current_hour:
                    dir_path = 'data//' + time_stamp[:7] + '//'
                    date = time_stamp[5:10]
                    while current_hour < int(hour):
                        logger.warning(f'Missing hour {current_hour} in date {date} for file {file}')
                        missed_hour = time_stamp[:10] + ' ' + f'{current_hour - 1:02}' + ':50'
                        if missed_line := find_missed_hour(
                            dir_path, missed_hour
                        ):
                            out_df = df.copy()
                            out_df.loc[idx - .5] = missed_line
                            out_df.sort_index().reset_index(drop=True)
                        current_hour += 1
                    current_hour = int(hour)
                if int(current_hour) < 24:
                    current_hour += 1
                else:
                    current_hour = 0
        out_df.to_csv(
            f'{str(file)}.csv',
            index=False,
            lineterminator='\n',
            errors='replace',
        )
        out_df = pd.DataFrame(np.empty((0, 701)))
    return None


def main():
    logger.info('Data cleaning and creating out puts is starting ... ')
    try:
        #create_outputs()
        logger.info('Inseting the missing data process is starting ... ')
        find_insert_missing_rows()
    except Exception as e:
        logger.error(f'Failed to prepare data due to {e}')
    logger.info('Data processing is complete.')


if __name__ == "__main__":
    main()
