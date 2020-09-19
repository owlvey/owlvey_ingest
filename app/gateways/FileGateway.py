import os
import pandas as pd
import numpy as np

class FileGateway: 

    def __init__(self):
        super().__init__()
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.base_directory = os.path.join(dir_path,  './../../')

    def read_data(self):
        headers = ['Source', 'start', 'end', 'total', 'ava', 'exp', 'lat']
        data = pd.read_csv("./data/backup.csv", sep=";", names=headers, parse_dates=['start', 'end'])

        data['year'] = data['start'].dt.strftime('%Y') 
        data['weekday'] = data['start'].dt.strftime('%a')
        data['month'] = data['start'].dt.strftime('%m')
        data['month_name'] = data['start'].dt.strftime('%b')
        data['week'] = data['start'].dt.strftime('%V')
        data['day'] = data['start'].dt.strftime('%e')
        data['weekday'] = data['start'].dt.strftime('%a')
        data['hour'] = data['start'].dt.strftime('%h')
        data = data.astype({"day": int})
        data['fortnight'] = np.where(data["day"] <= 15, 0, 1)
        return data
    
    def read_metadata(self):
        users = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Users')
        squads = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Squads')
        members = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Members')
        sources = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Sources')
        indicators = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Indicators')
        features = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Features')
        journeyMaps = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='JourneyMaps')
        journeys = pd.read_excel('./data/owlvey-metadata-backup.xlsx', sheet_name='Journeys')

        journeys = journeys.drop(columns=['Organization', 'Product', 'Avatar', 'Description', 'Leaders'])
        journeys.rename(columns = {'Name':'Journey'}, inplace = True) 
       
        journeyMaps = journeyMaps.drop(columns=['Organization', 'Product'])
        journeyMaps.rename(columns = {'Service':'Journey'}, inplace = True) 
       
        sources = sources.drop(columns=['Organization', 'Product', 'Description', 'Avatar',
                                        'GoodDefinitionAvailability',
                                        'TotalDefinitionAvailability', 'GoodDefinitionLatency',
                                        'TotalDefinitionLatency', 'GoodDefinitionExperience',
                                        'TotalDefinitionExperience', 'Percentile', 'Tags'])
        sources.rename(columns = {'Name':'Source'}, inplace = True) 
        features = features.drop(columns=['Organization', 'Product', 'Avatar', 'Description'])
        features.rename(columns = {'Name':'Feature'}, inplace = True) 
        indicators = indicators.drop(columns=['Organization', 'Product'])
        return journeys, journeyMaps, features, indicators, sources

    def write_hourly(self, frame):
        target = os.path.join(self.base_directory, './output/hourly_data.csv')
        frame.to_csv(target, sep=";")
    
    def write_daily(self, frame):
        target = os.path.join(self.base_directory, './output/daily_data.csv')
        frame.to_csv(target, sep=";")
    
    def write_fortnight(self, frame):
        target = os.path.join(self.base_directory, './output/fortnight_data.csv')
        frame.to_csv(target, sep=";")
    
    def write_month(self, frame):
        target = os.path.join(self.base_directory, './output/month_data.csv')
        frame.to_csv(target, sep=";")
    
    def write_slo_group(self, frame):
        target = os.path.join(self.base_directory, './output/slo_daily_data.csv')
        frame.to_csv(target, sep=";", index=False)

    
