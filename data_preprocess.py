import os
from time import time


import pandas as pd
def convert_logs(
        path = 'Datasets\\Activity Logs', 
        dump_path = 'Datasets\\Activity Logs',
        dump_file_name = 'ParticipantLogs',
        split = 5, 
        chunk_size = 260
    ):
    TIME = time()
    WHOLE = 1010
    VERBOSE_GAP = 10

    assert chunk_size % split == 0
    for chunk in range(1 + WHOLE // chunk_size):
        print('Start processing id %d~%d'%(chunk * chunk_size, (chunk + 1) * chunk_size))
        part_data = [None for _ in range(WHOLE // split + 1)]
        for filename in range(1, 21):
            print('Opening Participant Log %02d'%filename)
            data = pd.read_csv(os.path.join(path, 
                    'ParticipantStatusLogs%d.csv'%filename))
            print('Reading Finished Log %02d'%filename)

            part_start = chunk * chunk_size - split
            part_end = part_start + split
            verbose = VERBOSE_GAP
            for part in range(chunk_size // split):
                part_start = part_end
                part_end += split
                if part_start > WHOLE:
                    break
            
                concat = data.loc[
                    (data['participantId'] < part_end) & (data['participantId'] > part_start - 1)
                ]
                if part_data[part_start // split] is None:
                    part_data[part_start // split] = concat
                else:
                    part_data[part_start // split] = pd.concat(
                        [part_data[part_start // split], concat]
                    )
                
                if part > verbose:
                    print('...Extracting id %04d~%04d Time = %d'%(
                        part_start, part_end - 1, time() - TIME))
                    verbose += VERBOSE_GAP

        print('Start saving......Please check in folder......')
        for i, part in enumerate(part_data):
            if part is not None:
                part.to_csv(
                    os.path.join(dump_path, dump_file_name + '%d.csv'%(i + 1))
                )

                if i % 5 == 0:
                    print('Saving part %03d Time = %d'%(i, time() - TIME))
        print('End saving.\n\n')

        del part_data

if __name__ == '__main__':
    path = 'Datasets\\Activity Logs'
    split = 5
    chunk_size = 260
    convert_logs(
        path = path,
        dump_path = path,
        dump_file_name = 'ParticipantLogs',
        split = split,
        chunk_size = chunk_size
    )
