
# import required modules
from pathlib import Path
import logging
import pandas as pd
import numpy as np
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

# iterate over files in
# that directory
folders = Path(directory).glob('*')
for folder in folders:
    try:
        files = Path(str(folder)).glob('*')
        prior_to = ''
        past_to = ''
        # Creating Empty DataFrame for an output
        out_df = pd.DataFrame(np.empty((0, 701)))

        for file in files:
            df = pd.read_csv(file, on_bad_lines='skip', index_col=False)
            out_df.columns = list(df.columns)
            # TO DO :
            # Sorting by column 'observe_time' may required. Default is that they are sorted in decending order
            # df.sort_values(by=['observe_time'])
            for column in df.columns[:1]:
                #print(df[column].values)
                time_stamps = df[column].values
                prior_to_idx = None
                past_to_idx = None
                for idx, time_stamp in enumerate(time_stamps):
                    date = time_stamp[5:10]
                    time = time_stamp[12:25]
                    hour = time_stamp[11:13]
                    minute = time_stamp[14:16]
                    #print(time, date, hour, minute)

                    if minute == '00':  # Assume second is always '00'
                        print('perfeto')
                        out_df.loc[len(out_df.index)] = df.iloc[idx]
                        print(out_df.shape)
                    elif minute == '10':
                        past_to_idx = idx
                    elif minute == '50':
                        prior_to_idx = idx
                    """ else:
                        logger.info(f'Niether prior to or past to data available for {time}') """
        Path("outputs").mkdir(parents=True, exist_ok=True)
        out_df.to_csv('outputs/'+str(folder)[5:]+'.csv', index=False, lineterminator='\n', errors= 'replace')
       
                            #print(df.keys())
                            #df_month=
    except Exception as e:
        logger.error(f'Failed to prepare data due to {e} for file {file}')
