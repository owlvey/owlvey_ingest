from app.gateways.FileGateway import FileGateway
import numpy as np


class IngestComponent:

    def __init__(self, file_gateway: FileGateway):
        self.file_gateway = file_gateway
        self.data = None
        self.journeys = None
        self.journeyMaps = None
        self.indicators = None
        self.features = None
        self.sources = None
        self.output_daily = None
        self.output_month = None
        self.output_slo_month = None
        self.output_hourly = None

    def generate_hourly(self): 
        self.output_hourly = self.data.groupby(
            ['Source', 'year', 'month', 'month_name', 'week', 'weekday', 'day', 'hour']).agg({
            'total': 'sum',
            'ava': 'sum',
            'exp': 'sum',
            'lat': 'mean'
        })
        self.output_hourly['ava_prop'] = self.output_hourly['ava'].divide(self.output_hourly['total'])
        self.output_hourly['exp_prop'] = self.output_hourly['exp'].divide(self.output_hourly['total'])
        self.output_hourly.replace([np.inf, -np.inf], 0)

    def generate_daily(self):
        self.output_daily = self.data.groupby(
            ['Source', 'year', 'month', 'month_name', 'week', 'weekday', 'day']).agg({
            'total': 'sum',
            'ava': 'sum',
            'exp': 'sum',
            'lat': 'mean'
        })
        self.output_daily['ava_prop'] = self.output_daily['ava'].divide(self.output_daily['total'])
        self.output_daily['exp_prop'] = self.output_daily['exp'].divide(self.output_daily['total'])
        self.output_daily.replace([np.inf, -np.inf], 0)
       
    def generate_month(self):
        self.output_month = self.output_daily.groupby(
            ['Source', 'year', 'month', 'month_name']).agg({
            'total': 'sum',
            'ava': 'sum',
            'exp': 'sum',
            'lat': 'mean',
            'ava_prop': 'mean', 
            'exp_prop': 'mean'
        })
        self.output_month.replace([np.inf, -np.inf], 0)

    def generate_outputs(self):
        self.data = self.file_gateway.read_data()
        self.journeys, self.journeyMaps, self.features, self.indicators, self.sources = self.file_gateway.read_metadata()
        
        merged = self.journeys.merge(self.journeyMaps, left_on='Journey', 
            right_on='Journey', how='left')

        merged = merged.merge(self.features, left_on='Feature', 
            right_on='Feature', how='left')

        merged = merged.merge(self.indicators, left_on='Feature', 
            right_on='Feature', how='left')

        merged = merged.merge(self.sources, left_on='Source', 
            right_on='Source', how='left')
        
        self.generate_hourly()
        self.generate_daily()
        self.generate_month()

        self.output_slo_month = merged.merge(self.output_month, 
            left_on='Source', 
            right_on='Source', how='left')

        self.file_gateway.write_hourly(self.output_hourly)
        self.file_gateway.write_daily(self.output_daily)
        self.file_gateway.write_month(self.output_month)
        self.file_gateway.write_group_month(self.output_slo_month)
        
        

    