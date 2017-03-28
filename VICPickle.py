import pickle
import csv
import binascii
mydict = {'s3_base': 'https://bucket.servirglobal.net.s3.amazonaws.com//regions/himalaya/data/eodata/vic/grid/', 'extract_Folder': 'E:\Temp\VIC_Extract\\', 'mosaicDS': 'E:\SERVIR\Data\himalaya\VIC\\', 'finalTranslateFolder': 'E:\servir\data\himalaya\VIC\VIC_Data', 'logFileDir': 'D:\Logs\ETL_Logs\VIC','current_AdminDirURL':'http://localhost:6080/arcgis/admin','current_Username':'YOUR_USERNAME','current_Password':'YOUR_PASSWORD','current_FolderName':'Himalaya','HKH_VIC':'MapServer'}
output = open('config.pkl', 'wb')
pickle.dump(mydict, output)
output.close()