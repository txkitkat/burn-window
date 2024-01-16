# burn-window
## FireMaps Dashboard Burn Window Service

## master-netcdf
This python tool is for generating the master netcdf raster of burn windows for california per day from 1979-2020.
Shortly, it will contain a bash script for running installing, then preparing the master netcdf for inputted dates (allowing updates when new year data is available).
For the next service to work, the generated netcdf must be moved to the service directory.
<br>
<br>
First, create a folder called "unmasked" inside the /data folder. Temporarily move the download.sh inside the "unmasked" folder. 
<br>
<br>
Second, run the download.sh script. When running the bash script, wait until all nc files have been downloaded inside the "unmasked" folder which will take a while. 
<br>
<br>
Third, move download.sh out of the "unmasked" folder after finishing the download. Then, run netcdf.py which will take a while. During this process, a temp-window.nc file will appear. At the end, a window.nc file should be produced. Move the generated window.nc file to service/flaskr directory. 


## service
This python service is a basic flask app that utilizes the master netcdf prepared by the master-netcdf tool allowing the frontend to sum the rasters across a given time period. The result is then returned as a blob of content-type x-netcdf which can be downloaded to the client's computer on the frontend. There it can be loaded into a GUI for viewing rasters. 
This service requires the environment to contain the FLASK_APP variable equal to flaskr and FLASK_ENV variable equal to production. Then flask run can be used to run the application.
```
export FLASK_APP=flaskr
export FLASK_ENV=production
flask run