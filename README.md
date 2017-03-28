Introduction:
VIC ETL gets the data from s3 bucket for the basins brahmaputra, ganges, indus and meghna, for each of the data types Evaporation, Precip, Runoff and Soil Moisture. It then stores that data into the geodatabase.

Instructions to schedule the task on a machine:
1. Go to VICPickle.py and enter your paths and credentials
2. Save and run ViCPickle.py. This should generate config.pkl file in your folder
3. Search for Administrative tools in your machine
4. Go to Task Scheduler
5. Click on "Create Task" in the right pane and enter details 
5. Schedule the task in Triggers tab
6. Upload the VIC_ETL.bat file in the Actions tab
