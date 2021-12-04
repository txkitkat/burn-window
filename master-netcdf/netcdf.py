from netCDF4 import Dataset
import time
import geopandas
import rioxarray
import xarray
from shapely.geometry import mapping
import numpy as np
import os
        
cali_shape = geopandas.read_file('data/california_shp/CA_State_TIGER2016.shp')

def clip_to_cali(path_to_nc):
    nc = xarray.open_dataarray(path_to_nc)
    nc.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    nc.rio.write_crs("EPSG:4326", inplace=True)
    return nc.rio.clip(cali_shape.geometry.apply(mapping), cali_shape.crs, drop=True)

def close(*closeables):
    for closeable in closeables:
        closeable.close()

def init(data_path):
    burn_windows = Dataset(f"temp-window.nc", "w", format="NETCDF4")
    burn_windows.createDimension("day", None)
    burn_windows.createDimension("lon", 249)
    burn_windows.createDimension("lat", 227)
    
    burn_windows.createVariable("day", np.float64, ("day",))

    windows_var = burn_windows.createVariable("window", "i1", ("day", "lat", "lon",))
    windows_var.units = "Boolean"

    burn_windows.createVariable("lon", np.float64, ("lon",))
    burn_windows.createVariable("lat", np.float64, ("lat",))

    temp = Dataset(f"{data_path}rmin_1979.nc", "r")
    print(temp.variables["lat"])

    clipped = clip_to_cali(f"{data_path}rmin_1979.nc")
    burn_windows.variables["lon"] = clipped.coords["lon"]
    burn_windows.variables["lat"] = clipped.coords["lat"]
    clipped.close()

    return burn_windows

def run(data_path):
    burn_windows = init(data_path)
    # return

    days = 0
    years = [i for i in range(1979,2021)]

    for year in years:
        print(f"Filtering {year} ---")

        temp = Dataset(f"temp.nc", "w", format="NETCDF4")
        temp.createDimension("day", None)
        temp.createDimension("lat", 227)
        temp.createDimension("lon", 249)
        temp.createDimension("lower_relative_humidity", None)
        temp.createDimension("upper_relative_humidity", None)
        temp.createDimension("lower_air_temperature", None)
        temp.createDimension("upper_air_temperature", None)
        temp.createDimension("wind_speed", None)

        temp.createVariable("lower_relative_humidity","f",("day","lat","lon",))
        temp.createVariable("upper_relative_humidity","f",("day","lat","lon",))
        temp.createVariable("lower_air_temperature","f",("day","lat","lon",))
        temp.createVariable("upper_air_temperature","f",("day","lat","lon",))
        temp.createVariable("wind_speed","f",("day","lat","lon",))

        rmin = clip_to_cali(f"{data_path}rmin_{year}.nc")
        temp.variables["lower_relative_humidity"][:] = rmin.data
        burn_windows.variables["day"][:] = np.append(burn_windows.variables["day"][:], rmin.coords["day"].astype(np.float64))
        close(rmin)
        print("Added rmin to temp")

        rmax = clip_to_cali(f"{data_path}rmax_{year}.nc")
        temp.variables["upper_relative_humidity"][:] = rmax.data
        close(rmax)
        print("Added rmax to temp")

        tmmn = clip_to_cali(f"{data_path}tmmn_{year}.nc")
        temp.variables["lower_air_temperature"][:] = tmmn.data
        close(tmmn)
        print("Added tmmn to temp")

        tmmx = clip_to_cali(f"{data_path}tmmx_{year}.nc")
        temp.variables["upper_air_temperature"][:] = tmmx.data
        close(tmmx)
        print("Added tmmx to temp")

        vs = clip_to_cali(f"{data_path}vs_{year}.nc")
        temp.variables["wind_speed"][:] = vs.data
        close(vs)
        print("Added vs to temp")

        temp.variables["lower_relative_humidity"][:] = np.where(temp.variables["lower_relative_humidity"][:] >= 30, 0, 1)
        temp.variables["upper_relative_humidity"][:] = np.where(temp.variables["upper_relative_humidity"][:] <= 55, 0, 1)
        temp.variables["lower_air_temperature"][:] = np.where(temp.variables["lower_air_temperature"][:] >= 237.15, 0, 1)
        temp.variables["upper_air_temperature"][:] = np.where(temp.variables["upper_air_temperature"][:] <= 305.15, 0, 1)
        temp.variables["wind_speed"][:] = np.where(((temp.variables["wind_speed"][:] <= 10) & (temp.variables["wind_speed"][:] >= 2)), 0, 1)

        temp_arr = temp.variables["lower_relative_humidity"][:] == temp.variables["upper_relative_humidity"][:]
        temp_arr = temp_arr == temp.variables["lower_air_temperature"][:]
        temp_arr = temp_arr == temp.variables["upper_air_temperature"][:]
        temp_arr = temp.variables["wind_speed"][:]
        temp_arr.astype(int)
        
        burn_windows.variables["window"][days:days+len(temp.dimensions["day"]),:,:] = temp_arr
        days += len(temp.dimensions["day"])

        close(temp)


    window_array = xarray.DataArray(coords=[burn_windows.variables['day'][:], burn_windows.variables['lat'][:], burn_windows.variables['lon'][:]], dims=['time', 'lat', 'lon'])
    window_array.data = burn_windows.variables['window'][:]
    window_array = window_array.astype('uint32')
    window_array.rio.write_crs("epsg:4326", inplace=True)
    window_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    window_array.to_netcdf('window.nc')

if __name__ == "__main__":
    run("data/unmasked/")
    if os.path.exists("temp.nc"):
        os.remove("temp.nc")
    if os.path.exists("temp-window.nc"):
        os.remove("temp-window.nc")