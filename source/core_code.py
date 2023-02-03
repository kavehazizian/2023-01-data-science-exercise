# import required modules
from pathlib import Path
import logging
import pandas as pd
import numpy as np
import os
import csv
from datetime import datetime, timedelta
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
    empty_list = [None]*700
    for folder in folders:
        try:
            files = Path(str(folder)).glob('*')
            # Creating Empty DataFrame for an output
            out_df = pd.DataFrame(np.empty((0, 701)))
            current_hour = 0
            for file in files:
                df = pd.read_csv(file, on_bad_lines='skip', index_col=False)
                out_df.columns = list(df.columns)
                # TO DO :
                # Sorting by column 'observe_time' may required. Default is that they are sorted in decending order
                # df.sort_values(by=['observe_time'])
                for column in df.columns[:1]:

                    time_stamps = df[column].values
                    for idx, time_stamp in enumerate(time_stamps):

                        date_read = datetime.fromisoformat(time_stamp)
                        if date_read.minute == 0 and f'{date_read.month :02}' == str(folder)[10:] or (date_read.hour == 23 and current_hour <= date_read.hour and current_hour > 0):

                            ############################################
                            while current_hour < date_read.hour or (current_hour == 23 and date_read.hour == 23 and date_read.minute != 0):
                                if current_hour == 0:  # first hour of the day is missing
                                    day_before = date_read - timedelta(days=1)
                                    missed_hour = f'{str(day_before.date())} 23:50'
                                else:
                                    missed_hour = f'{str(date_read.date())} ' + f'{current_hour - 1:02}' + ':50'
                                if missed_line := find_missed_hour(
                                    folder, missed_hour
                                ):
                                    if missed_line[0][:10] != str(
                                        date_read.date()
                                    ):  # for the first hour we may need to use the current date to avoid wrong day!
                                        currect_date = date_read.date()
                                    else:
                                        currect_date = None
                                    missed_line = create_missed_line(missed_line, current_hour, currect_date)
                                else:
                                    missed_hour = f'{str(date_read.date())} ' + f'{current_hour:02}' + ':10'
                                    if missed_line := find_missed_hour(folder, missed_hour
                                                                       ):
                                        missed_line = create_missed_line(missed_line, current_hour)
                                    else:
                                        missed_date = [time_stamp[:11] + f'{current_hour:02}' + time_stamp[13::]]
                                        missed_line = missed_date + empty_list
                                        logger.info(f'no data found for {missed_date}')
                                out_df.loc[len(out_df.index)] = missed_line

                                current_hour += 1
                            current_hour = date_read.hour + 1
                            if current_hour > 23:
                                current_hour = 0
                        if date_read.minute == 0 and f'{date_read.month :02}' == str(folder)[10:]:
                            out_df.loc[len(out_df.index)] = df.iloc[idx]

            Path("outputs").mkdir(parents=True, exist_ok=True)
            out_df.to_csv('outputs/'+str(folder)[5:]+'.csv', index=False, lineterminator='\n', errors='replace')

        except Exception as e:
            logger.error(f'Failed to prepare data due to {e} for file {file}')
    return None


def create_missed_line(missed_line, current_hour, correct_date=None):
    """This function creates proper output to be inserted to the dataframe missed row."""
    date_missed = datetime.fromisoformat(missed_line[0])
    d1 = date_missed.replace(hour=current_hour, minute=0, day=correct_date.day if correct_date else date_missed.day)
    missed_line[0] = str(d1)
    return missed_line


def find_missed_hour(dir_path, missed_hour):
    """This function searchs for a given time stamp and returns the corresponding data for that if it exist."""
    for file in os.listdir(dir_path):
        cur_path = os.path.join(dir_path, file)
    # check if it is a file
        if os.path.isfile(cur_path):
            with open(cur_path, 'r') as fp:
                if missed_hour in fp.read():
                    logger.info(f'Data found for {missed_hour} in {dir_path}')
                    with open(cur_path, 'rt') as f:
                        reader = csv.reader(f, delimiter=',')
                        for row in reader:
                            if row[0].find(missed_hour) != -1:
                                return row
    return []


def post_processing():
    """This function search and finds the missing data in the generated outputs and then inserts the missing data."""
    files = Path('outputs').glob('*')
    empty_list = [None]*700
    for file in files:
        df = pd.read_csv(file, on_bad_lines='skip', index_col=False)

        if df.shape[0] not in [720, 744]:  # 30 days or 31 days
            logger.info(f'Some missing data were detected in {file}')
            df['observe_time'] = pd.to_datetime(df['observe_time'])
            my_range = pd.date_range(start=str(df.iloc[10][0]), end=str(df.iloc[df.shape[0]-1][0]), freq='h')

            missing_hours = my_range.difference(df['observe_time'])
            logger.info(f'{missing_hours} were detected in {file}')
            lst = [[str(missed_hour)] + empty_list for missed_hour in missing_hours]
            df_range = pd.DataFrame(lst)
            df_range.columns = df.columns

            df_range['observe_time'] = pd.to_datetime(df_range['observe_time'])
            frames = [df, df_range]

            result = pd.concat(frames)
            result.sort_values(by='observe_time', inplace=True)
            result.to_csv(
                str(file),
                index=False,
                lineterminator='\n',
                errors='replace',
            )
    return None


def main():
    logger.info('Data cleaning and creating out puts is starting ... ')
    try:
        create_outputs()
        logger.info('Inserting the missing data process is starting ... ')
        post_processing()
    except Exception as e:
        logger.error(f'Failed to prepare data due to {e}')
    logger.info('Data processing is complete.')


if __name__ == "__main__":
    main()
