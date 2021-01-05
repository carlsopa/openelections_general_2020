import tabula
import pandas as pd
import numpy as np

new = True
pdf_file = 'Cass MI Results per Precinct Data report.pdf'
county_name = 'Cass'
#unable to get the race from the first page, so we have to manually add it in
race_name = 'Straight Party Ticket'
#lists that hold all the data and then populate the new data frame
county = []
precinct = []
candidate = []
votes = []
race = []
district = []

start_page = 1
end_page = 4

def local_district(race):
    index = 1
    integer = True
    district_numbers = []
    result = 0
    c = ''
    if 'th ' in race:
        race = race[:race.find('th')]
        while integer:
            try:
                district_numbers.append(int(race[-index]))
                index = index + 1
            except:
                integer = False
        for x in district_numbers:
            c = c+str(x)
        result = [race[:-index+1],c]
    else:
        result = [race,np.nan]
    return(result)

while start_page <= end_page:
    data = tabula.read_pdf(pdf_file,multiple_tables=True,lattice=False,stream=False,pages=(start_page))
    new = False
    for x in data:
        df = x
        #since the first page has the candidates in the headers, we need to handle this page differently then the rest.  Here we need to strip out the header data to add to the candidate list
        print(start_page)
        if start_page == 1:
            df.drop(df.columns[0],axis=1,inplace=True)
            skip_rows = []
            candidateList = []

            for index, row in df.iterrows():
                if index <= df.loc[df.isin(['Total']).any(axis=1)].index.tolist()[0]:

                    for i in range(len(df.columns.values)):
                        if i >= 1:
                            candidate.append(df.columns.values[i])
                            votes.append(df.iloc[index,i])
                            precinct.append(df.iloc[index,0])
                            district.append(local_district(race_name)[1])
                            race.append(local_district(race_name)[0])
                        #since two results on the first page we need to treat it as a basic page
                if index >= df.loc[df.isin(['Total']).any(axis=1)].index.tolist()[0]:
                    if row[0] == 'Total' and index != len(df)-1:
                        print(df.iloc[index+1,0])
                        skip_rows.append(index+1)
                        race_name = df.iloc[index+1,0]
                    if row[0] == 'Precinct':
                        skip_rows.append(index)
                        count = 1
                        while count < len(df.columns.values):
                            candidateList.append(row[count])    
                            count = count + 1
                        #single case issue, where the names of the candidates are broken across two lines.  This combines the lines in the candidatelist array.
                        if not pd.notnull(df.iloc[index+1,0]):
                            skip_rows.append(index+1)
                            count = 0
                            while count < len(df.columns.values)-1:
                                if pd.notnull(df.iloc[index+1,count+1]):
                                    candidateList[count] = candidateList[count]+' '+df.iloc[index+1,count+1]
                                count = count + 1   
                    if index not in skip_rows:
                        # print(race_name)
                        for i in range(len(df.columns.values)):
                            if i >= 1:
                                if i == len(df.columns.values)-1:
                                    pass
                                if not pd.notnull(df.iloc[index,i]):
                                    pass
                                else:
                                    if len(candidateList) > 0:
                                        # print(race_name)
                                        district.append(local_district(race_name)[1])
                                        race.append(local_district(race_name)[0])
                                        votes.append(df.iloc[index,i])
                                        candidate.append(candidateList[i-1])
                                        precinct.append(df.iloc[index,0])

        else:
            df.drop(df.columns[0],axis=1,inplace=True)
            #an array of row indexes to skip and not process.
            skip_rows = []
            for index, rows in df.iterrows():
                #check to see if the first value is 'Precinct'.  If it is, then this will add the values of that row into candidatelist array.
                if rows[0] == 'Precinct':
                    skip_rows.append(index)
                    count = 1
                    candidateList = []
                    while count < len(df.columns.values):
                        candidateList.append(rows[count])    
                        count = count + 1
                    #single case issue, where the names of the candidates are broken across two lines.  This combines the lines in the candidatelist array.
                    if not pd.notnull(df.iloc[index+1,0]):
                        skip_rows.append(index+1)
                        count = 0
                        while count < len(df.columns.values)-1:
                            if pd.notnull(df.iloc[index+1,count+1]):
                                candidateList[count] = candidateList[count]+' '+df.iloc[index+1,count+1]
                            count = count + 1   
                #only runs if we have to process the data.
                if rows[0] == 'Total' and index != len(df)-1:
                    skip_rows.append(index+1)
                    race_name = df.iloc[index+1,0]
                    new = True
                if index not in skip_rows:
                    for i in range(len(df.columns.values)):
                        if i >= 1:
                            if i == len(df.columns.values)-1:
                                pass
                            if not pd.notnull(df.iloc[index,i]):
                                pass
                            else:
                                district.append(local_district(race_name)[1])
                                race.append(local_district(race_name)[0])
                                #several pages combined the results into one column, this will split that into two columns and store the data correctly.
                                if ' ' in str(df.iloc[index,i]):
                                    votes.append(df.iloc[index,i].split(' ')[0])
                                    votes.append(df.iloc[index,i].split(' ')[1])
                                    candidate.append(candidateList[-2])
                                    candidate.append(candidateList[-1])
                                    precinct.append(df.iloc[index,0])
                                    district.append(local_district(race_name)[1])
                                    race.append(local_district(race_name)[0])
                                else:
                                    votes.append(df.iloc[index,i])
                                    candidate.append(candidateList[i-1])
                                    precinct.append(df.iloc[index,0])
                

    start_page = start_page+1
for x in precinct:
    county.append(county_name)
final = {'County':county,'Precinct':precinct,'Office':race,'District':district,'Candidate':candidate,'Votes':votes}
new = pd.DataFrame(final)
new.to_csv('20201103__mi__general__cass__precinct.csv')