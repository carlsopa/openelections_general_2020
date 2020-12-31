import tabula
import pandas as pd
import pdfplumber
import re

pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)
page = 5
page_start = 6
page_end = 5
pages = str(page_start)+'-'+str(page_end)
result = []
row_drop=[]
precinct = []
candidate = []
party = []
votes = []
race_name = []
race=''
continuation = False

def header_split(page):
    with pdfplumber.open(r'marquette_mi_result.pdf') as pdf: 
        page = pdf.pages[page]
        text = page.extract_text()
    lines = text.split('\n')
    pdf.close()
    return(lines[3])

def race_split(page,cnt):
    result_dict = {}
    str = ''
    preprocess_string = ['QualifiedWriteIn','QualifiedWriteI','QualifiedWrit']
    found = False
    with pdfplumber.open(r'marquette_mi_result.pdf') as pdf: 
        page = pdf.pages[page]
        text = page.extract_text()
    lines = text.split('\n')
    for x in lines:
        if 'Voters' in x:
            string = x
    result_string = string.split('  ')[6:]
    str = str.join(result_string).replace(' ','')
    str = str.replace('TotalVotes','TotalVotes ')
    str = str.replace(')',') ')
    for x in preprocess_string:
        if not found:
            if str != str.replace(x,'QualifiedWriteIn '):
                str = str.replace(x,'QualifiedWriteIn ')
                found = True
    for x in str.split(' '):
        if 'recinct' in x:
            x = x.replace('recinct','')
        party = x.find('(')
        qualified = x.find('Qual')
        if party != -1:
            result_dict.update({re.sub(r"(\w)([A-Z])", r"\1 \2", x)[0:party+2]:x[party+1:party+4]})
        elif qualified != -1:
            result_dict.update({re.sub(r"(\w)([A-Z])", r"\1 \2", x)[0:qualified+2]:'Qualified Write In'})
        else:
            result_dict.update({re.sub(r"(\w)([A-Z])", r"\1 \2", x):'null'})
    return result_dict


def duplicate_column_removal(dataframe):
    for i in dataframe.index:
                if dataframe.iloc[i,0] == dataframe.iloc[i,3]:
                    dataframe.at[i,dataframe.columns[3]] = pd.NA
    dataframe.dropna(axis='columns',how='all',inplace=True)
    return(dataframe)

def removal(dataframe):
    row_drop=[]
    dataframe = duplicate_column_removal(dataframe)
    for index, row in dataframe.iterrows():
                if pd.isna(row[0]):
                    for label, content in row.items():
                        if pd.isna(content) == False:
                            row_drop.append(index)
                            try:
                                dataframe.loc[[index+1],[label]] = content
                            except:
                                pass
    dataframe.drop(dataframe.index[row_drop],inplace=True)
    dataframe.dropna(axis='columns',how='all',inplace=True)
    dataframe.dropna(axis='rows',how='all',inplace=True)
    dataframe = dataframe.reset_index(drop=True)
    row_drop=[]
    return(dataframe)

data = tabula.read_pdf('marquette_mi_result.pdf',multiple_tables=True,lattice=True,stream=True,pages=('5-6'))
for x in data:
    df = x
    df.dropna(axis='rows',how='all',inplace=True)
    df.dropna(axis='columns',how='all',inplace=True)
    df = df.reset_index(drop=True)
    if not df.empty:
        if continuation == False:
            for index, row in df.iterrows():
                if df.index.isin([index]).any() and str(df.iloc[index,0]) == 'County':
                    result=[]
                    continuation = True
                    race = header_split(page-1) 
                    df.drop(df.index[0:index+2],inplace=True)
                    df.dropna(axis='rows',how='all',inplace=True)
                    df.dropna(axis='columns',how='all',inplace=True)
                    df = df.reset_index(drop=True)
            df = removal(df)
            for x in range(len(df.columns)):
                df.rename({df.columns[x]:x},axis=1,inplace=True)
            result.append(df)
        else:
            df.drop(df.tail(4).index,inplace=True)
            df = removal(df)
            
            for x in range(len(df.columns)):
                df.rename({df.columns[x]:x},axis=1,inplace=True)
            result.append(df)
            continuation = False
        #test stuff
        try:
            final = pd.concat(result)
            final = final.reset_index(drop=True)
        except:
            final = df
row_index = []
if pd.isna(final.iloc[0,1]):
    final = final[1:]
final = final.reset_index(drop=True)
for index, row in final.iterrows():
    if row[0] == 'Cumulative':
        row_index.append(index)

final.drop(row_index,inplace=True)
final = final.reset_index(drop=True)
row_index = []
if final.at[3,0] == final.at[3,3]:
    final.drop(final.columns[3],axis=1,inplace=True)
for index,row in final.iterrows():
    if pd.isna(row[1]):
        final.loc[index-1,0] = final.loc[index-1,0]+' '+final.loc[index,0]
        row_index.append(index)
final.drop(row_index,inplace=True)
final = final.reset_index(drop=True)
col_count = len(final.columns)-3
final.drop(final.columns[1],axis=1,inplace=True)
final.drop(final.columns[1],axis=1,inplace=True)
split_results = race_split(page-1,col_count)
key_array = []
for x in split_results:
    key_array.append(x)
for index, row in final.iterrows():
    count= 0
    for data in list(final.columns.values):
        
        if data > 0:
            if count < len(key_array):
                candidate.append(key_array[count])
                party.append(split_results[key_array[count]])

                votes.append(row[data])
                precinct.append(row[0])
                race_name.append(race)
            count = count+1

final_result = {'precinct':precinct,'race':race_name,'candidate':candidate,'party':party,'votes':votes}
new = pd.DataFrame(final_result)

race.replace('  ','_')
save = race+'.csv'
new.to_csv(save)
page = page+1