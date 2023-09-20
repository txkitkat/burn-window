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

# main threading issues with matplotlib
matplotlib.use('Agg')
cali_shape = geopandas.read_file("./california_shp/CA_State_TIGER2016.shp")


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
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
    with xarray.open_dataset(file_name) as environmental_dataset:
        environmental_data = environmental_dataset.__xarray_dataarray_variable__
        flattened_data = xarray.DataArray(coords=[environmental_data.coords['lat'][:], environmental_data.coords['lon'][:]],
                                            dims=['lat', 'lon'])

        # Check if you want burn window or temperature
        if file_name == "window.nc":
            # Sum data between a period of time (in days)
            flattened_data.data = np.sum(environmental_data.data[start_date:end_date + 1, :, :], axis=0)
        elif file_name == "temperature_avg.nc":
            # Average data between a period of time (in days)
            flattened_data.data = np.mean(environmental_data.data[start_date:end_date + 1, :, :], axis=0)
            flattened_data = flattened_data.where(flattened_data != 0, np.nan)
        elif file_name == "temperature_max.nc":
            flattened_data.data = np.max(environmental_data.data[start_date:end_date + 1, :, :], axis=0)
            flattened_data = flattened_data.where(flattened_data != 0, np.nan)
        elif file_name == "humidity_min.nc":
            print("humidity size", environmental_data.data[start_date:end_date + 1, :, :].size)
            flattened_data.data = np.min(environmental_data.data[start_date:end_date + 1, :, :], axis=0)
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
        fig.savefig(window_plot_file_name + '.svg', format='svg', dpi=1500)
        allow_svg_to_stretch(window_plot_file_name + '.svg')

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
        fig.savefig(legend_file_name + '.png', bbox_inches='tight', pad_inches=0, dpi=1200)


def allow_svg_to_stretch(file_name):
    opened_file = open(file_name, "r")
    data_to_change = opened_file.read()
    data_to_change = data_to_change.replace('<svg ', '<svg preserveAspectRatio="none" ')
    opened_file.close()

    opened_file = open(file_name, "w+")
    opened_file.write(data_to_change)
    opened_file.close()
