from netCDF4 import Dataset
import geopandas
import xarray
from shapely.geometry import mapping
import numpy as np
import os

cali_shape = geopandas.read_file('data/california_shp/CA_State_TIGER2016.shp')

def close(*closeables):
    for closeable in closeables:
        closeable.close()   

def clip_to_cali(path_to_nc):
    nc = xarray.open_dataarray(path_to_nc)
    nc.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    nc.rio.write_crs("EPSG:4326", inplace=True)

    # Without this, the nc files would cover the entire US
    # Use the shapefile to clip to California only for all nc files
    nc = nc.rio.clip(cali_shape.geometry.apply(mapping), cali_shape.crs, drop=True)
    return nc


def create_netcdf4_file(data_path):
    burn_windows = Dataset(f"temp-window.nc", "w", format="NETCDF4")

    # Create 3 dimensions for the netcdf4 file: lat, lon, and time
    burn_windows.createDimension("lat", 227)  # latitude axis 227
    burn_windows.createDimension("lon", 249)  # longitude axis 249
    burn_windows.createDimension("day", None)  # unlimited axis (can be appended to)

    lat = burn_windows.createVariable('lat', np.float64, ('lat',))
    lat.units = 'degrees_north'
    lat.long_name = 'latitude'

    lon = burn_windows.createVariable('lon', np.float64, ('lon',))
    lon.units = 'degrees_east'
    lon.long_name = 'longitude'

    day = burn_windows.createVariable('day', np.float64, ('day',))
    day.units = 'day'
    day.long_name = 'day'

    windows_var = burn_windows.createVariable("window", "i1", ("day", "lat", "lon",))
    windows_var.units = "Boolean"

    # Get lat and lon values
    temp = Dataset(f"{data_path}rmin_1979.nc", "r")
    print(temp.variables["lat"])

    clipped = clip_to_cali(f"{data_path}rmin_1979.nc")
    burn_windows.variables["lon"] = clipped.coords["lon"]
    burn_windows.variables["lat"] = clipped.coords["lat"]
    clipped.close()

    return burn_windows


def create_temp_file():
    temp = Dataset(f"temp.nc", "w", format="NETCDF4")
    temp.createDimension("day", None)
    temp.createDimension("lat", 227)
    temp.createDimension("lon", 249)
    temp.createDimension("lower_relative_humidity", None)
    temp.createDimension("upper_relative_humidity", None)
    temp.createDimension("lower_air_temperature", None)
    temp.createDimension("upper_air_temperature", None)
    temp.createDimension("wind_speed", None)

    temp.createVariable("lower_relative_humidity", "f", ("day", "lat", "lon",))
    temp.createVariable("upper_relative_humidity", "f", ("day", "lat", "lon",))
    temp.createVariable("lower_air_temperature", "f", ("day", "lat", "lon",))
    temp.createVariable("upper_air_temperature", "f", ("day", "lat", "lon",))
    temp.createVariable("wind_speed", "f", ("day", "lat", "lon",))

    return temp

# Ideal Burn-Window Conditions
#   Relative humidity: 30-55%: rmin >= 30, rmax <= 55
#   Wind speed: 2-10 m/s: vs >= 2, vs <= 10
#   Air Temperature 0-32C: tmmn >= 0C, tmmx <= 32C
def filter_burn_window(temp):
    temp.variables["lower_relative_humidity"][:] = np.where(temp.variables["lower_relative_humidity"][:] >= 30, 0, 1)
    temp.variables["upper_relative_humidity"][:] = np.where(temp.variables["upper_relative_humidity"][:] <= 55, 0, 1)
    temp.variables["lower_air_temperature"][:] = np.where(temp.variables["lower_air_temperature"][:] >= 237.15, 0, 1)
    temp.variables["upper_air_temperature"][:] = np.where(temp.variables["upper_air_temperature"][:] <= 305.15, 0, 1)
    temp.variables["wind_speed"][:] = np.where(
        ((temp.variables["wind_speed"][:] <= 10) & (temp.variables["wind_speed"][:] >= 2)), 0, 1)

    return temp


def create_burn_window(data_path, burn_windows):
    days = 0
    years = [i for i in range(1979, 2021)]

    for year in years:
        print(f"Filtering {year} ---")

        temp = create_temp_file()

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

        temp = filter_burn_window(temp)

        temp_arr = temp.variables["lower_relative_humidity"][:] == temp.variables["upper_relative_humidity"][:]
        temp_arr = temp_arr == temp.variables["lower_air_temperature"][:]
        temp_arr = temp_arr == temp.variables["upper_air_temperature"][:]
        temp_arr = temp_arr == temp.variables["wind_speed"][:]
        temp_arr.astype(int)

        # About 365 days will be added for each year
        burn_windows.variables["window"][days:days + len(temp.dimensions["day"]), :, :] = temp_arr
        days += len(temp.dimensions["day"])

        close(temp)

    # Create a netcdf4 file named window.nc
    window_array = xarray.DataArray(
        coords=[burn_windows.variables['day'][:], burn_windows.variables['lat'][:], burn_windows.variables['lon'][:]],
        dims=['time', 'lat', 'lon'])
    window_array.data = burn_windows.variables['window'][:]
    window_array = window_array.astype('uint32')
    window_array.rio.write_crs("epsg:4326", inplace=True)
    window_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    window_array.to_netcdf('window.nc')


def run(data_path):
    burn_window = create_netcdf4_file(data_path)
    create_burn_window(data_path, burn_window)
    close(burn_window)


if __name__ == "__main__":
    # create unmasked folder in /data directory and run download.sh script in /unmasked to create required .nc files
    # a windows.nc file should be created as the end result
    run("data/unmasked/")
    if os.path.exists("temp.nc"):
        os.remove("temp.nc")
    if os.path.exists("temp-window.nc"):
        os.remove("temp-window.nc")
