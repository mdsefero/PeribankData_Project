#Reassembles to text the Peribank dabase, that is necesarily downloaded in partial files, from MedSciNet DB.
#Data cleans for multiple entires and reassembles multiples instances as one the artifacts of SQL output.
#Requires python 3.7+
#!/usr/bin/env python3 

import os, glob, re
import pandas as pd
import multiprocessing


def data_clean (all_filenames):
    datatoprocess = []
    for i in all_filenames:
        df = pd.read_csv(i, sep='|', dtype='object')
        df = df[['Pregnancy ID'] + [col for col in df.columns if col != 'Pregnancy ID']]
        df.drop_duplicates(inplace=True)  #Lose identical entries
        #To numbers for sortig. At beggining so errors before processing. Coerce sets errors to NaN (eg text). 
        df['Pregnancy ID'] = pd.to_numeric(df['Pregnancy ID'], errors='coerce') 
        df.dropna(subset=['Pregnancy ID'], inplace=True) #Lose any entries with missing pregnancy IDs
        df.dropna(axis='columns', how='all', inplace=True) #Lose any features that are universally empty
        df.fillna('', inplace=True)#Deals with later errors, pandas treats NaNs as floats not strings
        #Subset pregnancies with duplicates only
        duplicates = df.duplicated(subset=['Pregnancy ID'], keep=False)
        duplicated_data = (df[duplicates])
        #Get non rudundant list of duplicated pregnancy IDs
        IDs = list(dict.fromkeys(duplicated_data['Pregnancy ID'].tolist()))
        if len(IDs) == 0:
            outdf[i]=df
            print(f"Finished processing: {i}")
        else:
            datatoprocess.append([i,IDs,duplicated_data,df]) 
    return datatoprocess    


def aggregate_duplicates(data):     
    n, i, IDs, duplicated_data, df = 0, data[0], data[1], data[2], data[3] 
    for x in IDs: 
        n+=1
        #Get positions this pregnancy in the df for later ref
        index = df['Pregnancy ID'][df['Pregnancy ID'] == x].index.tolist()
        #Get data needing consoloidation for only this pregnancy
        pregnancy = duplicated_data.loc[duplicated_data['Pregnancy ID']==x] 
        #Assess which columns have variable data contributing to multiple entries
        columns_with_differences = pregnancy[pregnancy.columns[(pregnancy.nunique()!=1).values]]
        #Aggegate the variable columns/data and substitute it in the parent df
        for col in columns_with_differences.columns: 
            series = pregnancy[col]
            val = series.str.cat(sep=',')
            df.loc[index,col] = val #replace values with CSV merged data
        if n % 10000 == 0: print (f"Data for {n} pregnancies processed in {i}") 
    df.drop_duplicates(inplace=True) #Lose redundant, since all entries for a preg now identical  
    return [i, df]


def consolidate():
    #Get file numbers to order the data
    filenames = list(outdf.keys())
    for file in filenames:
        number = int(''.join(re.findall(r'\d+', file)))
        outdf[number] = outdf.pop(file)
    #Combine and save df to single file
    mergeddf = pd.DataFrame(columns=['Pregnancy ID'])
    for key,df in sorted(outdf.items()): 
        print (f"Merging {key}")
        mergeddf = mergeddf.merge(df, on='Pregnancy ID', how = 'outer')
    print (f"Saving merged data frame ...") 
    #Sort by Pregnancy ID so output in order
    mergeddf = mergeddf.astype({'Pregnancy ID':'int'})
    mergeddf.sort_values('Pregnancy ID', inplace=True)
    mergeddf.to_csv("PBDBfinal.txt", sep='|', index=False)


if __name__ == "__main__":
    #Get filenames
    extension = 'txt'
    all_filenames = [i for i in glob.glob('*PeribankDB_*.{}'.format(extension))]        
    #Main code 
    outdf = {}
    datatoprocess = data_clean(all_filenames) 
    pool = multiprocessing.Pool(os.cpu_count() -2)
    for x in pool.imap_unordered(aggregate_duplicates, datatoprocess):
        outdf[x[0]]=x[1]
        print(f"Finished processing: {x[0]}")
    pool.close()
    pool.join()
    consolidate()