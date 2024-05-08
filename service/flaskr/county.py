import xarray
import warnings
import rioxarray
import geopandas
import numpy as np
import boto3
import io

import datetime
import time

deploying_production = False

warnings.simplefilter("ignore", category=RuntimeWarning)

counties = {
    '06001': 'Alameda',
    '06003': 'Alpine',
    '06005': 'Amador',
    '06007': 'Butte',
    '06009': 'Calaveras',
    '06011': 'Colusa',
    '06013': 'Contra Costa',
    '06015': 'Del Norte',
    '06017': 'El Dorado',
    '06019': 'Fresno',
    '06021': 'Glenn',
    '06023': 'Humboldt',
    '06025': 'Imperial',
    '06027': 'Inyo',
    '06029': 'Kern',
    '06031': 'Kings',
    '06033': 'Lake',
    '06035': 'Lassen',
    '06037': 'Los Angeles',
    '06039': 'Madera',
    '06041': 'Marin',
    '06043': 'Mariposa',
    '06045': 'Mendocino',
    '06047': 'Merced',
    '06049': 'Modoc',
    '06051': 'Mono',
    '06053': 'Monterey',
    '06055': 'Napa',
    '06057': 'Nevada',
    '06059': 'Orange',
    '06061': 'Placer',
    '06063': 'Plumas',
    '06065': 'Riverside',
    '06067': 'Sacramento',
    '06069': 'San Benito',
    '06071': 'San Bernardino',
    '06073': 'San Diego',
    '06075': 'San Francisco',
    '06077': 'San Joaquin',
    '06079': 'San Luis Obispo',
    '06081': 'San Mateo',
    '06083': 'Santa Barbara',
    '06085': 'Santa Clara',
    '06087': 'Santa Cruz',
    '06089': 'Shasta',
    '06091': 'Sierra',
    '06093': 'Siskiyou',
    '06095': 'Solano',
    '06097': 'Sonoma',
    '06099': 'Stanislaus',
    '06101': 'Sutter',
    '06103': 'Tehama',
    '06105': 'Trinity',
    '06107': 'Tulare',
    '06109': 'Tuolumne',
    '06111': 'Ventura',
    '06113': 'Yolo',
    '06115': 'Yuba'
}

s3 = boto3.client('s3')

def get_file_from_s3(bucket_name, file_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        body = response.get('Body')

        if body is not None:
            return io.BytesIO(response['Body'].read())
        else:
            print("Error: Response body is None.")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def query_county(start, end):
    shape = geopandas.read_file("./flaskr/CA_Counties/CA_Counties_TIGER2016.shp")

    county_result = process_window_data("window.nc", shape, start, end)
    return county_result


def process_window_data(file_name, shape, start, end):
        # Check if in deployment
    if deploying_production:
        # Fetch a file from S3
        bucket_name = 'fire-map-dashboard-geospatial-data'
        data_bytes = get_file_from_s3(bucket_name, file_name)
    else:
        data_bytes = "./" + file_name




    unx_offset = time.mktime(datetime.datetime(1979,1,1).timetuple()) #- (8*60*60)
    #print(start_date)
    start_time_unx = start*24*60*60 + unx_offset
    end_time_unx = end*24*60*60 + unx_offset

    #print(start_time_unx)
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


    result = []


    flattened_data = None
    total_days = 0


    for file in range(start_file, end_file, 5):
       print(start_file, ", ", end_file, ", ", file)
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

           file_data = np.sum(current_data[start_idx:end_idx + 1, :, :], axis=0)
           if first_file:
                    flattened_data.data = file_data
           else:
                    flattened_data.data += file_data


        #burn_windows = burn_windows_dataset.__xarray_dataarray_variable__
    flattened_window = xarray.DataArray(coords=[flattened_data.coords['lat'][:], flattened_data.coords['lon'][:]],
                                            dims=['lat', 'lon'])



    flattened_window.data = flattened_data.data#np.sum(burn_windows.data[start:end + 1, :, :], axis=0)
    flattened_window.rio.write_crs("EPSG:4326", inplace=True)

    county_area = xarray.DataArray(coords=[flattened_data.coords['lat'][:], flattened_data.coords['lon'][:]],
                                       dims=['lat', 'lon'])
    county_area.rio.write_crs("EPSG:4326", inplace=True)
    county_area.data = np.ones(county_area.shape).astype('uint32')

    for i in range(58):
            area_in_window = flattened_window.rio.clip([shape.geometry[i]], shape.crs, drop=True)

            area_total = county_area.rio.clip([shape.geometry[i]], shape.crs, drop=True)
            area_total = np.sum(area_total.data, axis=(0, 1))

            if np.sum(area_in_window.data, axis=(0, 1)) > 0:
                percent = np.sum(area_in_window.data) / area_total / (end - start + 1)
                percent = f'{percent.astype(float):.2%}'
                result.append(f"{counties[shape['GEOID'][i]]:<17}{percent:>6}")

    return result
