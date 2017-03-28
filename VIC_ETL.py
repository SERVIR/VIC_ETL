# Last Modified By: Githika Tondapu

import os
import shutil
import urllib
import urllib2
import pickle
import logging
import arcpy
import datetime
import gc
import json

basePath = r'https://bucket.servirglobal.net.s3.amazonaws.com//regions/himalaya/data/eodata/vic/grid/'
baseOutputPath = r'E:\SERVIR\Data\Himalaya\VIC'

# Naming is YYYYmmdd.basin.txt (will be changing to .tiff)
# Get the last processed Date, add a day then return
def getNextDate(currentMosaic):
    logging.info("Getting Last Date from : " + currentMosaic)
    try:
        Expression = "Name IS NOT NULL" #No names should be NULL, this is just precautionary
        rows=arcpy.UpdateCursor(currentMosaic, '', '', '', "Date ASC") 
        theDate = None
        for r in rows:
            theDate = r.Date
        if theDate == None:
            return datetime.datetime.strptime("20020101", "%Y%m%d")
        rows = None
        del rows
        return theDate + datetime.timedelta(days=1)
    except Exception, e:
        logging.error('Error occured in GetStartDateFromdb, %s' % e)
        return None

# Download data from https://bucket.servirglobal.net.s3.amazonaws.com//regions/himalaya/data/eodata/vic/grid/{basin}/{variable}/Daily/{file_name}
def downloadData(basin, dataType, dateString):
    a=urllib.urlopen(basePath + basin +'/' + dataType + '/Daily/' + dateString + '.'+basin+'.txt')
    if a.getcode() == 404:
        response = urllib2.urlopen(basePath + basin +'/' + dataType + '/Daily/' + dateString + '.'+basin+'.tif')
    else:
        response = urllib2.urlopen(basePath + basin +'/' + dataType + '/Daily/' + dateString + '.'+basin+'.txt')
    logging.info('Downloaded : ' + basePath + basin +'/' + dataType + '/Daily/' + dateString + '.'+basin+'.tif')
    data = response.read()
    with open(myConfig['extract_Folder'] + dateString + "." + basin +"_"+dataType +".tif", "wb") as code:
        code.write(data)
    a = None
    response = None
    data = None
    del a,response,data
    
    return True

# Restarting the ArcGIS Services (Stop and start)
def refreshService():
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file) #store the data from config.pkl file
    pkl_file.close()
    current_Description = "VIC Mosaic Dataset Service"
    current_AdminDirURL = myConfig['current_AdminDirURL']
    current_Username = myConfig['current_Username']
    current_Password = myConfig['current_Password']
    current_FolderName = myConfig['current_FolderName']
    current_ServiceName = myConfig['current_ServiceName']
    current_ServiceType = myConfig['current_ServiceTypecm']
    # Try and stop each service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
        tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the current service
        stopParams = urllib.urlencode({"token":token,"f":"json"})
        stopResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/stop?",stopParams).read()
        stopResponseJSON = json.loads(stopResponse)
        stopStatus = stopResponseJSON["status"]

        if stopStatus <> "success":
            logging.warning("HKH_VIC Stop Service: Unable to stop service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+stopStatus)
        else:
            logging.info("HKH_VIC Stop Service: Service: " + str(current_ServiceName) + " has been stopped.")

    except Exception, e:
        logging.error("HKH_VIC Stop Service: ERROR, Stop Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))
    # Try and start each service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
        tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the current service
        startParams = urllib.urlencode({"token":token,"f":"json"})
        startResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/start?",startParams).read()
        startResponseJSON = json.loads(startResponse)
        startStatus = startResponseJSON["status"]

        if startStatus == "success":
            logging.info("HKH_VIC start Service: Started service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType))
        else:
            logging.warning("HKH_VIC start Service: Unable to start service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+startStatus)
    except Exception, e:
        logging.error("HKH_VIC start Service: ERROR, Start Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))

# Clean the extract folder E:\Temp\VIC_Extract\
def cleanExtractFolder():
    folder = myConfig['extract_Folder']
    filelist = os.listdir(folder)
    for the_file in filelist:
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception, e:
            logging.error('Error occured in cleanExtractFolder removing files, %s' % e)
    try:
        shutil.rmtree(folder +'info', ignore_errors = True)
    except Exception, e:
            logging.error('Error occured in cleanExtractFolder removing info folder, %s' % e)
    try:
        shutil.rmtree(folder +'temp', ignore_errors = True)
    except Exception, e:
            logging.error('Error occured in cleanExtractFolder removing temp folder, %s' % e)
    try:
        filelist = None
        folder = None
        del filelist,folder
    except Exception, e:
            logging.error('Error occured in cleanExtractFolder removing temp folder, %s' % e)

# Download data to E:\SERVIR\Data\Himalaya\VIC\VIC_Data 
print "VIC ETL just started"
pkl_file = open('config.pkl', 'rb')
myConfig = pickle.load(pkl_file) #store the data from config.pkl file
pkl_file.close()
logDir = myConfig['logFileDir']
logging.basicConfig(filename=logDir+ '\VIC_log_'+datetime.date.today().strftime('%Y-%m-%d')+'.log',level=logging.DEBUG, format='%(asctime)s: %(levelname)s --- %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info('VIC ETL started')
mosaicDS = myConfig['mosaicDS']
sheds = ['brahmaputra', 'ganges', 'indus', 'meghna']
dataTypes = ['Evaporation', 'Precip', 'Runoff','Soil_Moisture'] 
arcpy.CheckOutExtension("Spatial")
sr = arcpy.SpatialReference('WGS 1984') 
arcpy.env.workspace = myConfig['finalTranslateFolder']  #E:\SERVIR\Data\\Himalaya\VIC\VIC_Data
for dataType in dataTypes:
    for shed in sheds:
        mosaicDS =  myConfig['mosaicDS'] + shed +'.gdb\\' + dataType
        actualDate = getNextDate(mosaicDS)
        randomStop = datetime.datetime.now()
        while (actualDate <  randomStop):
            dateToProcessed = actualDate.strftime('%Y%m%d');
            try:
                downloadData(shed, dataType, dateToProcessed)
                currentRaster = myConfig['extract_Folder'] + dateToProcessed + "."+shed+"_"+dataType+".tif"              
                print dateToProcessed
                out_raster = myConfig['finalTranslateFolder'] +"\\"+ dateToProcessed + "."+shed+"_"+dataType+"_referenced.tif"
                arcpy.CopyRaster_management(currentRaster, out_raster)
                currentRaster = None
                arcpy.DefineProjection_management(out_raster, sr) # Overwrite the coordinate system information to WGS 1984.
                logging.info('Begin Raster Management')                
                arcpy.AddRastersToMosaicDataset_management(mosaicDS, "Raster Dataset", out_raster,"UPDATE_CELL_SIZES", "NO_BOUNDARY", "NO_OVERVIEWS","2", "#", "#", "#", "#", "NO_SUBFOLDERS","EXCLUDE_DUPLICATES", "BUILD_PYRAMIDS", "CALCULATE_STATISTICS","NO_THUMBNAILS", "Add Raster Datasets","#")
                theName = dateToProcessed + "."+shed+"_"+dataType+"_referenced"
                out_raster = None                
                logging.info('Begin Update Date field')              
                Expression = "Name= '" + theName + "'" 
                rows=arcpy.UpdateCursor(mosaicDS, Expression) # Establish Read Write access to data in the query expression.
                year = theName[0:4]
                month = theName[4:6]
                day = theName[6:8]
                theStartDate = year + "/" + month + "/" + day
                dt_obj = theStartDate #dt_str.strptime('%Y/%m/%d')
                for r in rows:
                    if r.getValue("name") == theName:
                        r.Date=dt_obj
                        rows.updateRow(r) 
                extract = None
                del rows,extract
                logging.info('Begin Calculate Statistics')
                arcpy.CalculateStatistics_management(mosaicDS,1,1,"#","#","#") #Calculates statistics for the mosaic dataset.
                print arcpy.GetMessages()+ "\n\n" #Print all of the geoprocessing messages returned by the Calculates statistics.
                year = None
                month = None
                day = None
                theStartDate = None
                dt_obj = None
                theName = None
                del year,month,day,theStartDate,dt_obj,theName
            except Exception, e:
                logging.error('Error occured in Processing data, %s for basin %s' % (dataType, shed))
                print arcpy.GetMessages()+ "\n\n"
                logging.error('ArcPy errors, %s ' % (arcpy.GetMessages()))            
            logging.info('Processed: ' + shed + ', ' + dataType + ', ' + dateToProcessed)
            actualDate = actualDate  + datetime.timedelta(days=1)            
            cleanExtractFolder()
            print "gc.collect() - while"
            gc.collect() 
        shed = None
        randomStop = None
        mosaicDS = None	
        del shed,randomStop,mosaicDS
        print "gc.collect() - for"
        gc.collect()
    dataType = None
arcpy.CheckInExtension("Spatial") # Return license to License Manager so that other applications can use it.
cleanExtractFolder()
refreshService()
