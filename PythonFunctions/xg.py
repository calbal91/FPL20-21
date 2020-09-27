#Standard data manipulations
import pandas as pd
import numpy as np

#SQL
import sqlite3
#Including custom functions, stored elsewhere in the repo
from PythonFunctions.sqlfunctions import *

#We need some SQL functionality for the classes
#Connect to the database 'fpl.db' (fantasy premier league!)
conn = sqlite3.connect('Data/20_21fpl.db')

#Instantiate a cursor
c = conn.cursor()

#Create a dataframe of all known shots
shots_19_20 = sql('select * from shot_detail_19_20')
shots_20_21 = sql('select * from shots_detail')

df_all_shots = pd.concat([shots_19_20, shots_20_21])



#We now need a few functions that will calculate our XG and XGA metrics

#Create some custom columns for our shot detail dataframe.
def solo(x):
    if x=='N/A':
        return 'Solo'
    else:
        return 'Assisted'
    
def central(x):
    if x=='the centre':
        return 'Central'
    else:
        return 'NonCentral'

def strike(x):
    if x=='header':
        return 'Header'
    else:
        return 'Strike'

#Turn ShotPosition into camel case
def camel(x):
    x = x.title()
    return x.replace(' ', '')


def shot_classifier(df):
    '''
    takes a shot_details dataframe, and adds a new column that
    defines what kind of shot it was.
    '''
    
    df['Solo'] = df['AssistedBy'].map(solo)
    df['Central'] = df['ShotSide'].map(central)
    df['Strike'] = df['ShotType'].map(strike)
    df['ShotPosition'] = df['ShotPosition'].map(camel)
    
    df['ShotClass'] = df['ShotPosition'] + df['Solo'] + df['Central'] + df['Strike']
    
    #Merge some categories together and otherwise tidy
    df['ShotClass'] = df['ShotClass'].map(lambda x: 'Penalty' if x[:3]=='Pen' else x)
    df['ShotClass'] = df['ShotClass'].map(lambda x: x.replace('Central','') if x[:4]=='Very' else x)
    df['ShotClass'] = df['ShotClass'].map(lambda x: 'LongRange' if x[:4]=='Long' else x)
    df['ShotClass'] = df['ShotClass'].map(lambda x: x.replace('Solo','').replace('Assisted','')
                                          if 'Header' in x else x)
    
    return df



def xg_prob_constructor(df=df_all_shots):
    
    '''
    Takes a shot_df and calculates the probability
    that each type of shot results in a goal (returns a dataframe
    with these statistics).
    '''
    
    #construct the shot table
    df_shots = shot_classifier(df)
    
    #Get a count of each shot type
    df_shots = df.groupby('ShotClass').count()[['Player']]
    df_shots.columns = ['Total']

    #Get a count of the shots on target / goals scored
    df_shots['OnTarget'] = df.loc[df['ShotOutcome'].isin(['Goal','Saved'])].groupby(
        'ShotClass').count()[['Player']]
    df_shots['Goals'] = df.loc[df['ShotOutcome']=='Goal'].groupby('ShotClass').count()[['Player']]

    df_shots.fillna(0, inplace=True)

    #Goal ratio
    df_shots['TotalOnTarget'] = df_shots['OnTarget'] / df_shots['Total']
    df_shots['TotalXG'] = df_shots['Goals'] / df_shots['Total']
    df_shots['OnTargetXG'] = df_shots['Goals'] / df_shots['OnTarget']
    df_shots.fillna(0, inplace=True)
    
    return df_shots


#Create a stock 
df_xg = xg_prob_constructor(df=df_all_shots)


def xg_col_constructor(df, df_xg=df_xg):
    
    '''
    Takes a shot dataframe, df, and a dataframe of shot classes and their
    associated XGs, df_xg (as created by xg_prob_constructor)
    and creates new columns about the XG of each shot
    '''
    
    #Identify the shot type of each
    df = shot_classifier(df)
    
    #Create a blank list
    xg_column = []
    
    #Iterate through each shot
    for row in df.itertuples():
        row_outcome = row.ShotOutcome
        row_class = row.ShotClass

        if row_outcome in ['Goal', 'Saved']:
            row_xg = df_xg.loc[row_class, 'OnTargetXG']
        else:
            row_xg = df_xg.loc[row_class, 'TotalXG']

        xg_column.append(row_xg)

    df['XG'] = xg_column
    
    return df