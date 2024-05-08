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


def create_burn_netcdf4_file(data_path):
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

def create_temperature_netcdf4_file(data_path, data_name):
    yearly_temperatures = Dataset(f"{data_name}-temp-temperature.nc", "w", format="NETCDF4")

    # Create 3 dimensions for the netcdf4 file: lat, lon, and time
    yearly_temperatures.createDimension("lat", 227)  # latitude axis 227
    yearly_temperatures.createDimension("lon", 249)  # longitude axis 249
    yearly_temperatures.createDimension("day", None)  # unlimited axis (can be appended to)

    lat = yearly_temperatures.createVariable('lat', np.float64, ('lat',))
    lat.units = 'degrees_north'
    lat.long_name = 'latitude'

    lon = yearly_temperatures.createVariable('lon', np.float64, ('lon',))
    lon.units = 'degrees_east'
    lon.long_name = 'longitude'

    day = yearly_temperatures.createVariable('day', np.float64, ('day',))
    day.units = 'day'
    day.long_name = 'day'

    windows_var = yearly_temperatures.createVariable("temperature", "f", ("day", "lat", "lon",))
    windows_var.units = "Celsius"

    # Get lat and lon values
    temp = Dataset(f"{data_path}rmin_1979.nc", "r")
    print(temp.variables["lat"])

    clipped = clip_to_cali(f"{data_path}rmin_1979.nc")
    yearly_temperatures.variables["lon"] = clipped.coords["lon"]
    yearly_temperatures.variables["lat"] = clipped.coords["lat"]
    clipped.close()

    return yearly_temperatures

def create_humidity_netcdf4_file(data_path, data_name):
    yearly_humidity = Dataset(f"{data_name}-temp-humidity.nc", "w", format="NETCDF4")

    # Create 3 dimensions for the netcdf4 file: lat, lon, and time
    yearly_humidity.createDimension("lat", 227)  # latitude axis 227
    yearly_humidity.createDimension("lon", 249)  # longitude axis 249
    yearly_humidity.createDimension("day", None)  # unlimited axis (can be appended to)

    lat = yearly_humidity.createVariable('lat', np.float64, ('lat',))
    lat.units = 'degrees_north'
    lat.long_name = 'latitude'

    lon = yearly_humidity.createVariable('lon', np.float64, ('lon',))
    lon.units = 'degrees_east'
    lon.long_name = 'longitude'

    day = yearly_humidity.createVariable('day', np.float64, ('day',))
    day.units = 'day'
    day.long_name = 'day'

    windows_var = yearly_humidity.createVariable("humidity", "f", ("day", "lat", "lon",))
    windows_var.units = "Percent"

    # Get lat and lon values
    temp = Dataset(f"{data_path}rmin_1979.nc", "r")
    print(temp.variables["lat"])

    clipped = clip_to_cali(f"{data_path}rmin_1979.nc")
    yearly_humidity.variables["lon"] = clipped.coords["lon"]
    yearly_humidity.variables["lat"] = clipped.coords["lat"]
    clipped.close()

    return yearly_humidity


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


def create_all_netcdf(data_path, burn_windows, yearly_avg_temps, yearly_max_temps, yearly_min_humidity):
    days = 0
    years_s = [i for i in range(1979, 2024, 5)]
    years = [i for i in range(1979, 2024)]

    for start in range(len(years_s)-1):
        begin = years_s[start]
        end = years_s[start + 1]
        years = [i for i in range(begin, end)]

        for year in years:
            print(f"Filtering {year} ---")

            temp = create_temp_file()

            rmin = clip_to_cali(f"{data_path}rmin_{year}.nc")
            temp.variables["lower_relative_humidity"][:] = rmin.data
            burn_windows.variables["day"][:] = np.append(burn_windows.variables["day"][:], rmin.coords["day"].astype(np.float64))
            close(rmin)
            print("Added rmin to temp")

            # Min humidity and add 365 days of data for each year
            yearly_min_humidity.variables["day"][:] = np.append(yearly_min_humidity.variables["day"][:], rmin.coords["day"].astype(np.float64))
            yearly_min_humidity.variables["humidity"][days:days + len(temp.dimensions["day"]), :, :] = rmin.data
            print("Added humidity data to yearly_max_temps")

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

            # Average temperature min and max, convert to celsius, and add 365 days of data for each year
            tmav = (tmmn + tmmx) / 2
            tmav_C = (tmav - 273.15)
            yearly_avg_temps.variables["day"][:] = np.append(yearly_avg_temps.variables["day"][:], tmav_C.coords["day"].astype(np.float64))
            yearly_avg_temps.variables["temperature"][days:days + len(temp.dimensions["day"]), :, :] = tmav_C.data
            print("Added temperature data to yearly_avg_temps")

            # Highest temperature, convert to celsius, and add 365 days of data for each year
            tmmx_C = tmmx - 273.15
            yearly_max_temps.variables["day"][:] = np.append(yearly_max_temps.variables["day"][:], tmmx_C.coords["day"].astype(np.float64))
            yearly_max_temps.variables["temperature"][days:days + len(temp.dimensions["day"]), :, :] = tmmx_C.data
            print("Added temperature data to yearly_max_temps")

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
            print(days)

            close(temp)

        print("Finished year iteration")

        # Create a netcdf4 file named window.nc
        window_array = xarray.DataArray(
            coords=[burn_windows.variables['day'][:], burn_windows.variables['lat'][:], burn_windows.variables['lon'][:]],
            dims=['time', 'lat', 'lon'])
        window_array.data = burn_windows.variables['window'][:]
        window_array = window_array.astype('uint32')
        window_array.rio.write_crs("epsg:4326", inplace=True)
        window_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        window_array.to_netcdf(f'window_{begin}_{end}.nc')

        #create a netcdf4 file named temperature_avg.nc
        temperature_avg_array = xarray.DataArray(
            coords=[yearly_avg_temps.variables['day'][:], yearly_avg_temps.variables['lat'][:], yearly_avg_temps.variables['lon'][:]],
            dims=['time', 'lat', 'lon'])
        temperature_avg_array.data = yearly_avg_temps.variables['temperature'][:]
        temperature_avg_array = temperature_avg_array.astype('float32')
        temperature_avg_array.rio.write_crs("epsg:4326", inplace=True)
        temperature_avg_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        temperature_avg_array.to_netcdf(f'temperature_avg_{begin}_{end}.nc')

        #create a netcdf4 file named temperature_max.nc
        temperature_max_array = xarray.DataArray(
            coords=[yearly_max_temps.variables['day'][:], yearly_max_temps.variables['lat'][:], yearly_max_temps.variables['lon'][:]],
            dims=['time', 'lat', 'lon'])
        temperature_max_array.data = yearly_max_temps.variables['temperature'][:]
        temperature_max_array = temperature_max_array.astype('float32')
        temperature_max_array.rio.write_crs("epsg:4326", inplace=True)
        temperature_max_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        temperature_max_array.to_netcdf(f'temperature_max_{begin}_{end}.nc')

        #create a netcdf4 file named humidity_min.nc
        humidity_min_array = xarray.DataArray(
            coords=[yearly_min_humidity.variables['day'][:], yearly_min_humidity.variables['lat'][:], yearly_min_humidity.variables['lon'][:]],
            dims=['time', 'lat', 'lon'])
        humidity_min_array.data = yearly_min_humidity.variables['humidity'][:]
        humidity_min_array = humidity_min_array.astype('uint32')
        humidity_min_array.rio.write_crs("epsg:4326", inplace=True)
        humidity_min_array.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        humidity_min_array.to_netcdf(f'humidity_min_{begin}_{end}.nc')


def run(data_path):
    burn_windows = create_burn_netcdf4_file(data_path)
    yearly_avg_temps = create_temperature_netcdf4_file(data_path, "avg")
    yearly_max_temps = create_temperature_netcdf4_file(data_path, "max")
    yearly_min_humidity = create_humidity_netcdf4_file(data_path, "min")
    create_all_netcdf(data_path, burn_windows, yearly_avg_temps, yearly_max_temps, yearly_min_humidity)
    close(burn_windows)
    close(yearly_avg_temps)
    close(yearly_max_temps)
    close(yearly_min_humidity)


if __name__ == "__main__":
    # create unmasked folder in /data directory and run download.sh script in /unmasked to create required .nc files
    # a windows.nc file should be created as the end result
    run("data/unmasked/")
    if os.path.exists("temp.nc"):
        os.remove("temp.nc")
    if os.path.exists("temp-window.nc"):
        os.remove("temp-window.nc")
    if os.path.exists("temp-temperature.nc"):
        os.remove("temp-temperature.nc")
    if os.path.exists("avg-temp-temperature.nc"):
        os.remove("avg-temp-temperature.nc")
    if os.path.exists("max-temp-temperature.nc"):
        os.remove("max-temp-temperature.nc")
    if os.path.exists("min-temp-humidity.nc"):
        os.remove("min-temp-humidity.nc")

    
