import os
import sys
from app.components.IngestComponent import IngestComponent
from app.gateways.FileGateway import FileGateway

if __name__ == "__main__":
    component = IngestComponent(FileGateway())
    component.generate_outputs()
   