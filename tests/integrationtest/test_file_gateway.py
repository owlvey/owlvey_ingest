import unittest
import os
from app.gateways.FileGateway import FileGateway



class FileGatewayTest(unittest.TestCase):

    def __init__(self, methodName):
        super().__init__(methodName)
    
    def test_read_data(self):
        gateway = FileGateway()
        df = gateway.read_data()
        partial = df['hour']
        print(partial.head())
    
if __name__ == "__main__":
    unittest.main()