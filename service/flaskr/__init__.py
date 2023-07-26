import os
import glob
import time

import matplotlib
matplotlib.use('Agg') #main threading issues with matplotlib
import numpy as np
import xarray
from flask import Flask, request, send_from_directory
from flask_cors import cross_origin
import matplotlib.image
import matplotlib.pyplot as plt
from werkzeug.serving import run_simple
from .county import query_county

from PIL import Image

def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/query', methods=['GET'])
    @cross_origin()
    def make_query():
        cleanup()

        start_date, end_date = request.args.get('start_date'), request.args.get('end_date')
        if start_date is not None and end_date is not None:
            return query(int(start_date), int(end_date))

        return 'failed'

    @app.route('/image', methods=['GET'])
    @cross_origin()
    def get_image():
        return send_from_directory('.', 'window.png')

    @app.route('/county', methods=['GET'])
    @cross_origin()
    def county():
        start_date, end_date = request.args.get('start_date'), request.args.get('end_date')
        return query_county(int(start_date), int(end_date))

    @app.route('/legend', methods=['GET'])
    @cross_origin()
    def get_legend():
        return send_from_directory('.', 'legend.png')

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
    with xarray.open_dataset("window.nc") as burn_windows_dataset:
        burn_windows = burn_windows_dataset.__xarray_dataarray_variable__
        flattened_window = xarray.DataArray(coords=[burn_windows.coords['lat'][:], burn_windows.coords['lon'][:]],
                                            dims=['lat', 'lon'])

        flattened_window.data = np.sum(burn_windows.data[start_date:end_date, :, :], axis=0) # Sum data between a period of time (in days)
        flattened_window = flattened_window.astype('uint32')

        #Create Legend and Burn-Window Map
        fig, ax = plt.subplots()
        fig.patch.set_visible(False)
        ax.axis('off')
        plt.ioff()
        plt.imshow(flattened_window, cmap = 'hot')

        # Currently, the colormap includes the background color of the burn-window which is represented as the top color in the scale
        # making it hard to visualize the numbers for the burn-window in California
        # A solution in the meantime is to reduce the top range (for a large amount of days)
        number_of_total_days_in_burn_window = end_date - start_date
        if(number_of_total_days_in_burn_window >= 100):
            number_of_total_days_in_burn_window = number_of_total_days_in_burn_window * 0.7

        plt.colorbar(ax = ax, label="Day", boundaries=np.linspace(0, number_of_total_days_in_burn_window))
        ax.remove()
        #plt.show()
        plt.close(fig)
        fig.savefig('legend.png', bbox_inches = 'tight', pad_inches = 0)

        matplotlib.rcParams['savefig.dpi'] = 300
        matplotlib.image.imsave('window1.png', flattened_window, cmap='hot') 
        whiten()
        return 'success'


def whiten():
    print("Cleaning png.")
    img = Image.open('window1.png')
    img = img.convert("RGBA")
    datas = img.getdata()

    newData = []
    for item in datas:
        if item[0] == 255 and item[1] == 245 and item[2] == 240:
            newData.append((255,255,255,0))
        else:
            newData.append(item)
    
    img.putdata(newData)
    img.save("window.png", "PNG")