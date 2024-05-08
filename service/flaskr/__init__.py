import os
import glob
import matplotlib
import numpy as np
import xarray
from flask import Flask, request, send_from_directory
from flask_cors import cross_origin
import matplotlib.image
import matplotlib.pyplot as plt
from .county import query_county
import geopandas
from shapely.geometry import mapping
import boto3
import io
from flask_cors import CORS

import datetime
import time

deploying_production = False

# main threading issues with matplotlib
matplotlib.use('Agg')
cali_shape = geopandas.read_file("./flaskr/california_shp/CA_State_TIGER2016.shp")

s3 = boto3.client('s3')

def get_file_from_s3(bucket_name, file_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        return io.BytesIO(response['Body'].read())
    except Exception as e:
        print(f"Error: {e}")
        return None

def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    CORS(app)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Disable caching for image and legend. Currently works on the Google Chrome and Microsoft Edge browsers
    # without the need to checkmark disable cache option.
    @app.after_request
    def add_header(response):
        response.headers["Cache-Control"] = "no-store max-age=0"
        return response

    @app.route('/query', methods=['GET'])
    @cross_origin()
    def make_query():
        cleanup()
        start_date, end_date = request.args.get('start_date'), request.args.get('end_date')
        if start_date is not None and end_date is not None:
            return query(int(start_date), int(end_date))
        return 'failed'
    
    @app.route('/county', methods=['GET'])
    @cross_origin()
    def county():
        start_date, end_date = request.args.get('start_date'), request.args.get('end_date')
        return query_county(int(start_date), int(end_date))

    # Burn resources
    @app.route('/burn_window_image', methods=['GET'])
    @cross_origin()
    def get_burn_image():
        return send_from_directory('.', 'burn_window.svg')
    
    @app.route('/burn_legend', methods=['GET'])
    @cross_origin()
    def get_burn_legend():
        return send_from_directory('.', 'burn_legend.png')
    
    # Temperature resources
    @app.route('/temperature_avg_image', methods=['GET'])
    @cross_origin() 
    def get_temperature_avg_image():
        return send_from_directory('.', 'temperature_avg.svg')

    @app.route('/temperature_avg_legend', methods=['GET'])
    @cross_origin()
    def get_temperature_avg_legend():
        return send_from_directory('.', 'temperature_avg_legend.png')
    
    @app.route('/temperature_max_image', methods=['GET'])
    @cross_origin()
    def get_temperature_max_image():
        return send_from_directory('.', 'temperature_max.svg')
    
    @app.route('/temperature_max_legend', methods=['GET'])
    @cross_origin()
    def get_temperature_max_legend():
        return send_from_directory('.', 'temperature_max_legend.png')

    #Humidity resources
    @app.route('/humidity_min_image', methods=['GET'])
    @cross_origin()
    def get_humidity_min_image():
        return send_from_directory('.', 'humidity_min.svg')

    @app.route('/humidity_min_legend', methods=['GET'])
    @cross_origin()
    def get_humidity_min_legend():
        return send_from_directory('.', 'humidity_min_legend.png')

    return app


def cleanup():
    files_to_clean = glob.glob(f'{os.getcwd()}/burn-window-*.nc')
    for f in files_to_clean:
        try:
            os.remove(f)
        except:
            print("Error while deleting file : ", f)


def query(start_date: int, end_date: int):
    print("Querying against netcdf.")
    process_window_data("window.nc", "burn_window", "burn_legend", 'hot', start_date, end_date)
    process_window_data("temperature_avg.nc", "temperature_avg", "temperature_avg_legend", 'copper', start_date, end_date)
    process_window_data("temperature_max.nc", "temperature_max", "temperature_max_legend", 'copper', start_date, end_date)
    process_window_data("humidity_min.nc", "humidity_min", "humidity_min_legend", 'Purples', start_date, end_date)
    return 'success'
    
def process_window_data(file_name, window_plot_file_name, legend_file_name, colormap, start_date, end_date):

    # Check if in deployment
    if deploying_production:
        # Fetch a file from S3
        bucket_name = 'fire-map-dashboard-geospatial-data'
        data_bytes = get_file_from_s3(bucket_name, file_name)
    else:
        data_bytes = "./flaskr/" + file_name

    unx_offset = time.mktime(datetime.datetime(1979,1,1).timetuple()) #- (8*60*60)
#    print(start_date)
    start_time_unx = start_date*24*60*60 + unx_offset
    end_time_unx = end_date*24*60*60 + unx_offset

#    print(start_time_unx)
    start_dt = datetime.datetime.utcfromtimestamp(start_time_unx)
    end_dt = datetime.datetime.utcfromtimestamp(end_time_unx)

    #Which files do we start/stop at
    start_year, end_year = start_dt.year, end_dt.year
    start_file = start_year - ((start_year+1)%5)
    end_file = end_year - ((end_year +1)%5)+5
    
    #How many days are the first and last days from the beginning of their file
    first_data_offset = time.mktime(datetime.datetime(start_file,1,1).timetuple())
    last_data_offset = time.mktime(datetime.datetime(end_file-5, 1, 1).timetuple())

    first_idx = int((start_time_unx - first_data_offset)/(24*60*60))
    last_idx = int((end_time_unx - last_data_offset)/(24*60*60))

    #print(start_dt)
    #print(end_dt)
    #print(start_year)
    #print(end_year)
    #print(first_idx)
    #print(last_idx)


    flattened_data = None
    total_days = 0

    print(f"Scanning files from {start_file} to {end_file}")
    for file in range(start_file, end_file, 5):
       print(f"Opening file {file}-{file+5}")
       # current_data = xarray.open_dataset(data_bytes[:-3]+f"_{file}_{file+5}.nc", engine="h5netcdf").astype(float)
       with  xarray.open_dataset(data_bytes[:-3]+f"_{file}_{file+5}.nc", engine="h5netcdf") as current_dataset:
           current_data = current_dataset.__xarray_dataarray_variable__

           #Assume we're scanning the entire file, unless we've got the first or last file to be scanned
           start_idx, end_idx = 0,  current_data.shape[0]
           if file == start_file:
                start_idx = first_idx
           if file == end_file - 5:
                end_idx = last_idx
           total_days += end_idx - start_idx

           first_file = False
           if flattened_data is None:
                first_file = True
                #don't bother calculating this more than once; format is relic of old code
                flattened_data =  xarray.DataArray(coords=[current_data.coords['lat'][:], current_data.coords['lon'][:]],
                                                dims=['lat', 'lon'])

           #print(flattened_data)

           #flattened_data is a shell with no time; build it as we go


           #old team's code for reference: assumes whole dataset

           #with xarray.open_dataset(data_bytes, engine="h5netcdf") as environmental_dataset:
           # environmental_dataset = result_data
            #environmental_data = environmental_dataset.__xarray_dataarray_variable__
            #flattened_data = xarray.DataArray(coords=[environmental_data.coords['lat'][:], environmental_data.coords['lon'][:]],
            #                                   dims=['lat', 'lon'])
           environmental_data = current_data

            # Check if you want burn window or temperature
           if file_name == "window.nc":
                # Sum data between a period of time (in days)
                file_data = np.sum(environmental_data.data[start_idx:end_idx + 1, :, :], axis=0)
                if first_file:
                    flattened_data.data = file_data
                else:
                    flattened_data.data += file_data

           elif file_name == "temperature_avg.nc":
                # Average data between a period of time (in days)
                # We sum for now and then divide by # of days on the last one

                file_data = np.sum(environmental_data.data[start_idx:end_idx + 1, :, :], axis=0)
                if first_file:
                    flattened_data.data = file_data
                else:
                    flattened_data.data += file_data


                #flattened_data = flattened_data.where(flattened_data != 0, np.nan)
           elif file_name == "temperature_max.nc":
                file_data = np.max(environmental_data.data[start_idx:end_idx + 1, :, :], axis=0)
                if first_file:
                    flattened_data.data = file_data
                else:
                    flattened_data.data = np.maximum(flattened_data.data, file_data)


                #flattened_data = flattened_data.where(flattened_data != 0, np.nan)
           elif file_name == "humidity_min.nc":
                file_data = np.min(environmental_data.data[start_idx:end_idx + 1, :, :], axis=0)

            
                if first_file:
                    print("humidity size", environmental_data.data[start_idx:end_idx + 1, :, :].size)
                    flattened_data.data = file_data
                else:
                    flattened_data.data = np.minimum(flattened_data.data, file_data)

                #flattened_data = flattened_data.where(flattened_data != 0, np.nan)
       

    if file_name == "temperature_avg.nc":
        flattened_data /= total_days

    if file_name != "window.nc":
        flattened_data = flattened_data.where(flattened_data != 0, np.nan)


    # Clip data to the outline of California using shapefile
    flattened_data = flattened_data.rio.set_spatial_dims(x_dim='lon', y_dim='lat')
    flattened_data.rio.write_crs("EPSG:4326", inplace=True)
    area_in_window = flattened_data.rio.clip(cali_shape.geometry.apply(mapping), cali_shape.crs, drop=True)

    # Create duplicate and clip again
    duplicate = xarray.DataArray(
        data=area_in_window.where(area_in_window.notnull(), np.nan),
        coords=area_in_window.coords,  # Use the same coordinates as area_in_window
        dims=["lat", "lon"]
    )

    duplicate = duplicate.rio.set_spatial_dims(x_dim='lon', y_dim='lat')
    duplicate.rio.write_crs("EPSG:4326", inplace=True)
    duplicate_clipped = duplicate.rio.clip(cali_shape.geometry.apply(mapping), cali_shape.crs, drop=True)

    # Create Legend and Layer Map
    fig, ax = plt.subplots()
    fig.patch.set_visible(False)
    ax.axis('off')
    plt.ioff()
        
    plt.imshow(duplicate_clipped, cmap=colormap)
    fig.savefig("./flaskr/" + window_plot_file_name + '.svg', format='svg', dpi=1500)
    allow_svg_to_stretch("./flaskr/" + window_plot_file_name + '.svg')

    if file_name == "window.nc":
        number_of_total_days_in_burn_window = end_date + 1 - start_date
        plt.colorbar(ax=ax, label="Days that burn windows are met", boundaries=np.linspace(0, number_of_total_days_in_burn_window))
    elif file_name == "temperature_avg.nc":
        plt.colorbar(ax=ax, label="Average Temperature (°C)")
    elif file_name == "temperature_max.nc":
        plt.colorbar(ax=ax, label="Max Temperature (°C)")
    elif file_name == "humidity_min.nc":
        plt.colorbar(ax=ax, label="Min Humidity (%)")
    ax.remove()
    plt.close(fig)
    fig.savefig("./flaskr/" + legend_file_name + '.png', bbox_inches='tight', pad_inches=0, dpi=1200)


def allow_svg_to_stretch(file_name):
    opened_file = open(file_name, "r")
    data_to_change = opened_file.read()
    data_to_change = data_to_change.replace('<svg ', '<svg preserveAspectRatio="none" ')
    opened_file.close()

    opened_file = open(file_name, "w+")
    opened_file.write(data_to_change)
    opened_file.close()
