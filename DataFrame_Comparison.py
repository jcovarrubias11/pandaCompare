import os
import logging
import pandas as pd
import datacompy
import numpy as np

#df1 = pd.read_csv('Day5.csv')
#df2 = pd.read_csv('Day6.csv')

df1 = pd.DataFrame(np.random.randint(low=0, high=100, size=(40, 5)),
                   columns=['a', 'b', 'c', 'd', 'e'])

df2 = pd.DataFrame(np.random.randint(low=0, high=100, size=(40, 5)),
                   columns=['a', 'b', 'c', 'd', 'e'])

str1 = 'Yesterday'
str2 = 'Today'

compare = datacompy.Compare(df1, df2, on_index = '0', abs_tol = 0,rel_tol = 0,df1_name = str1, df2_name = str2)

compare.matches(ignore_extra_columns=False)

with open('update.csv', 'w') as outFile:
            outFile.write(compare.report())

#print ("""##############################
#THIS THAT HAND MADE SCRIPT FOO
###############################\n""")

sample_count = 10

def render(filename, *fields):
    this_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(this_dir, filename)) as file_open:
        return file_open.read().format(*fields)

report = render('header.txt')                                                       
report += '\n'
df_header = pd.DataFrame({                                                          #Creates a Data Frame using Panda
            'DataFrame': [compare.df1_name, compare.df2_name],                      #Returns the names of DataFrame: Yesterday and Toady
            'Columns': [compare.df1.shape[1], compare.df2.shape[1]],                #Returns the number of columns of each dataframe
            'Rows': [compare.df1.shape[0], compare.df2.shape[0]]})                  #Returns the number of rows of each dataframe
report += df_header[['DataFrame', 'Columns', 'Rows']].to_string()                   #Converts them to strings
report += '\n\n'                                                                    #Add spacing for cleanliness

#column summary
report += render(
            'column_summary.txt',                                                   #Gets text from txt file names "column_summary.txt"
            len(compare.intersect_columns()),                                       #Returns how many columns from each DF intersect aka have in commom
            len(compare.df1_unq_columns()),                                         #Returns how many columns are in Yesterday DF and not in Todays DF
            len(compare.df2_unq_columns()),                                         #Returns how many columns are in Today DF and not in Yestedays DF
            compare.df1_name, compare.df2_name)                                     #Inputs name of Data Frames into text file
report += '\n\n'                                                                    #Add spacing for cleanliness

#row summary
if compare.on_index:
            match_on = 'index'                                                      #if parameter on_index equals true
else:
            match_on = ', '.join(compare.join_columns)                              #else join everything by commas
report += render(
            'row_summary.txt',                                                      #Gets text from txt file names "row_summary.txt"
            match_on,                                                               #Outputs choice to match on
            compare.abs_tol,                                                        #Outputs Absolute tolerance
            compare.rel_tol,                                                        #Outputs Relative Tolerance
            compare.intersect_rows.shape[0],                                        #Returns the numbers rows in common
            compare.df2_unq_rows.shape[0],                                          #Returns the number of rows in Yesterdays DF but not in Todays DF
            compare.df1_unq_rows.shape[0],                                          #Returns the number of rows in Todays DF but not in Yesterdays DF
            compare.intersect_rows.shape[0] - compare.count_matching_rows(),        #Returns the number of rows with some compared columns unequal 
            compare.count_matching_rows(),                                          #Returns the number of rows with all columns that are equal
            compare.df1_name, compare.df2_name,                                     #Inputs name of Data Frames into text file
            'Yes' if compare._any_dupes else 'No')                                  #Returns if there are Duplicates
report += '\n\n'                                                                    


cnt_intersect = compare.intersect_rows.shape[0]
report += render(                                                                   
            'column_comparison.txt',                                                #Gets text from txt file names "column_comparison.txt"
            len([col for col in compare.column_stats if col['unequal_cnt'] > 0]),   #Returns the number of columns compared with some values unequal
            len([col for col in compare.column_stats if col['unequal_cnt'] == 0]),  #Returns the number of columns compared with all values equal
            sum([col['unequal_cnt'] for col in compare.column_stats])               #Returns the total number of values which compare unequal
            )                                                                       

match_stats = []
match_sample = []
any_mismatch = False
for column in compare.column_stats:
    if not column['all_match']:
        any_mismatch = True
        match_stats.append({
           'Column': column['column'],
           '{} dtype'.format(compare.df1_name): column['dtype1'],
           '{} dtype'.format(compare.df2_name): column['dtype2'],
           '# Unequal': column['unequal_cnt'],
           'Max Diff': column['max_diff'],
           '# Null Diff': column['null_diff']
           })
        if column['unequal_cnt'] > 0:
            match_sample.append(compare.sample_mismatch(
                column['column'], sample_count, for_display=True))
report += '\n\n'

if any_mismatch:
    report += 'Columns with Unequal Values or Types\n'
    report += '------------------------------------\n'
    report += '\n'
    df_match_stats = pd.DataFrame(match_stats)
    df_match_stats.sort_values('Column', inplace=True)
    #Have to specify again for sorting
    report += df_match_stats[[
        'Column', '{} dtype'.format(compare.df1_name),
        '{} dtype'.format(compare.df2_name),
        '# Unequal', 'Max Diff', '# Null Diff']].to_string()
    report += '\n\n'

    report += 'Sample Rows with Unequal Values\n'
    report += '-------------------------------\n'
    report += '\n'
    for sample in match_sample:
        report += sample.to_string()
        report += '\n\n'

if compare.df1_unq_rows.shape[0] > 0:
    report += 'Sample Rows Only in {} (First 10 Columns)\n'.format(
        compare.df1_name)
    report += '---------------------------------------{}\n'.format(
        '-' * len(compare.df1_name))
    report += '\n'
    columns = compare.df1_unq_rows.columns[:10]
    unq_count = min(sample_count, compare.df1_unq_rows.shape[0])
    report += compare.df1_unq_rows.sample(
        unq_count)[columns].to_string()
    report += '\n\n'

if compare.df2_unq_rows.shape[0] > 0:
    report += 'Sample Rows Only in {} (First 10 Columns)\n'.format(
        compare.df2_name)
    report += '---------------------------------------{}\n'.format(
        '-' * len(compare.df2_name))
    report += '\n'
    columns = compare.df2_unq_rows.columns[:10]
    unq_count = min(sample_count, compare.df2_unq_rows.shape[0])
    report += compare.df2_unq_rows.sample(
        unq_count)[columns].to_string()

    report += '\n\n'

#print (report)









#SHOW THE FUNCTIONS OF "COMPARE" CLASS
#dir = dir(datacompy.Compare)
#print (dir)


