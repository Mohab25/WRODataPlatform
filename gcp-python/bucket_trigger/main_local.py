import sys

import pandas.api.types
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import zipfile
import logging
import io
import geopandas
import pandas as pd
import json
import os
import time
from datetime import datetime
import gcsfs
import numpy
from dateutil.parser import parse
import math

import requests
from requests import get, post
from requests.exceptions import Timeout


class Default:
    # For testing
    #BUCKET_TEMP = 'wrc_wro_temp2'
    #PROJECT_ID = 'static-webbing-359410'
    #BUCKET_TRIGGER = 'trigger_bucket2'
    #BIGQUERY_DATASET_BUCKET = 'bucket_data'
    #REGION = 'us-east1'

    # Projects
    PROJECT_ID = 'wrc-wro'

    # Buckets
    BUCKET_TRIGGER = 'wrc_wro_datasets'
    BUCKET_DONE = 'wro-done'
    BUCKET_FAILED = 'wro-failed'
    BUCKET_TEMP = 'wrc_wro_temporary'

    # Regions (e.g. us, us-east1, etc.)
    REGION = 'us'

    # BigQuery
    BIGQUERY_DATASET_DAILY = 'NASA_POWER_climate'
    BIGQUERY_DATASET_MONTHLY = 'NASA_POWER_weather_daily'
    BIGQUERY_DATASET_CLIMATOLOGY = 'NASA_POWER_weather_monthly'
    BIGQUERY_DATASET_BUCKET = 'bucket_tables'
    LIST_BQ_DATASETS = [
        BIGQUERY_DATASET_DAILY,
        BIGQUERY_DATASET_MONTHLY,
        BIGQUERY_DATASET_CLIMATOLOGY
    ]
    BIGQUERY_DATETIME_STRUCTURE = '%Y-%m-%d'

    # CSV parsing
    CHARS_TO_REMOVE = [' ', '\n', '\t']  # Characters to remove from field contents
    # Characters to remove from field names
    FIELD_NAME_INVALID_CHARS = [
        ' ', '-', '.', '!', '@', '#', '$',
        '%', '&', '*', '\n', '\t', '\'', '\"'
    ]
    ALLOWED_DATE_STRUCTURES = ['%Y-%m-%d', '%Y/%m/%d']  # Date structures allowed by parser
    # Rows will be read in as chunks. This is required due to limitations
    CSV_ROW_READ_LIMIT = 10000000  # 10 million rows seems to work fine

    # NASA POWER request parameters
    NASA_POWER_URL = 'https://power.larc.nasa.gov/api/temporal'
    NASA_POWER_FORMAT = 'CSV'
    # AG: Agroclimatology, RE: Renewable energy, or SB: Sustainable buildings
    NASA_POWER_COMMUNITY = ['RE', 'SB', 'AG']
    DEFAULT_TIMEOUT = 60  # Timeout in seconds for requests
    DEFAULT_SLEEP = 30  # Sleep time in seconds for when a request fails
    MAX_REQUESTS = 10  # Number of attempts a request will be done if it fails
    SKIP_LEADING_ROWS = 10  # Rows to skip at the start of the received request content (e.g. headers)
    SKIP_TRAILING_ROWS = 1  # Rows to skip at the e of the reveived request content (e.g. headers)
    LAT_FIELD = 'LAT'  # Latitude field name
    LON_FIELD = 'LON'  # Longitude field name

    # Temporal types and settings
    DAILY = 'daily'
    MONTHLY = 'monthly'
    CLIMATOLOGY = 'climatology'
    NASA_POWER_TEMPORAL_AVE = [DAILY, MONTHLY, CLIMATOLOGY]
    NUMBER_OF_PREVIOUS_DAY = 5  # This will be the number of days prior to the current/today date
    SKIP_CLIMATOLOGY = True  # These datasets will likely not change
    SLEEP_LENGTH = 20  # How long python will do a sleep (e.g. waiting for data). Seconds
    SLEEP_COUNT = 5  # Number of sleeps which will be performed, likely before timeout is performed

    # 'D' for daily downloads, 'M' for all days on a monthly basis
    # 'M' will be useful for bulk downloads, but set to 'D' for the Cloud trigger as it should only
    # download on a daily basis.
    DAILY_DATES_FREQUENCY = 'D'

    # Grid tiles used for each request
    SA_GRID_EXTENTS = [
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "16.057872772",  # left
            "lon_max": "19.057872772"  # right
        },
        {
            "lat_min": "-33.623123168999996",  # bottom
            "lat_max": "-30.623123169",  # top
            "lon_min": "16.057872772",  # left
            "lon_max": "19.057872772"  # right
        },
        {
            "lat_min": "-36.623123168999996",  # bottom
            "lat_max": "-33.623123168999996",  # top
            "lon_min": "16.057872772",  # left
            "lon_max": "19.057872772"  # right
        },
        {
            "lat_min": "-27.623123169",  # bottom
            "lat_max": "-24.623123169",  # top
            "lon_min": "19.057872772",  # left
            "lon_max": "22.057872772"  # right
        },
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "19.057872772",  # left
            "lon_max": "22.057872772"  # right
        },
        {
            "lat_min": "-33.623123168999996",  # bottom
            "lat_max": "-30.623123169",  # top
            "lon_min": "19.057872772",  # left
            "lon_max": "22.057872772"  # right
        },
        {
            "lat_min": "-36.623123168999996",  # bottom
            "lat_max": "-33.623123168999996",  # top
            "lon_min": "19.057872772",  # left
            "lon_max": "22.057872772"  # right
        },
        {
            "lat_min": "-27.623123169",  # bottom
            "lat_max": "-24.623123169",  # top
            "lon_min": "22.057872772",  # left
            "lon_max": "25.057872772"  # right
        },
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "22.057872772",  # left
            "lon_max": "25.057872772"  # right
        },
        {
            "lat_min": "-33.623123168999996",  # bottom
            "lat_max": "-30.623123169",  # top
            "lon_min": "22.057872772",  # left
            "lon_max": "25.057872772"  # right
        },
        {
            "lat_min": "-36.623123168999996",  # bottom
            "lat_max": "-33.623123168999996",  # top
            "lon_min": "22.057872772",  # left
            "lon_max": "25.057872772"  # right
        },
        {
            "lat_min": "-24.623123169",  # bottom
            "lat_max": "-21.623123169",  # top
            "lon_min": "25.057872772",  # left
            "lon_max": "28.057872772"  # right
        },
        {
            "lat_min": "-27.623123169",  # bottom
            "lat_max": "-24.623123169",  # top
            "lon_min": "25.057872772",  # left
            "lon_max": "28.057872772"  # right
        },
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "25.057872772",  # left
            "lon_max": "28.057872772"  # right
        },
        {
            "lat_min": "-33.623123168999996",  # bottom
            "lat_max": "-30.623123169",  # top
            "lon_min": "25.057872772",  # left
            "lon_max": "28.057872772"  # right
        },
        {
            "lat_min": "-36.623123168999996",  # bottom
            "lat_max": "-33.623123168999996",  # top
            "lon_min": "25.057872772",  # left
            "lon_max": "28.057872772"  # right
        },
        {
            "lat_min": "-24.623123169",  # bottom
            "lat_max": "-21.623123169",  # top
            "lon_min": "28.057872772",  # left
            "lon_max": "31.057872772"  # right
        },
        {
            "lat_min": "-27.623123169",  # bottom
            "lat_max": "-24.623123169",  # top
            "lon_min": "28.057872772",  # left
            "lon_max": "31.057872772"  # right
        },
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "28.057872772",  # left
            "lon_max": "31.057872772"  # right
        },
        {
            "lat_min": "-33.623123168999996",  # bottom
            "lat_max": "-30.623123169",  # top
            "lon_min": "28.057872772",  # left
            "lon_max": "31.057872772"  # right
        },
        {
            "lat_min": "-24.623123169",  # bottom
            "lat_max": "-21.623123169",  # top
            "lon_min": "31.057872772",  # left
            "lon_max": "34.057872771999996"  # right
        },
        {
            "lat_min": "-27.623123169",  # bottom
            "lat_max": "-24.623123169",  # top
            "lon_min": "31.057872772",  # left
            "lon_max": "34.057872771999996"  # right
        },
        {
            "lat_min": "-30.623123169",  # bottom
            "lat_max": "-27.623123169",  # top
            "lon_min": "31.057872772",  # left
            "lon_max": "34.057872771999996"  # right
        }
    ]

    # Prefixes used for temporal types
    MONTHLY_PREFIX = [
        'Jan',
        'Feb',
        'Mar',
        'Apr',
        'May',
        'Jun',
        'Jul',
        'Aug',
        'Sep',
        'Oct',
        'Nov',
        'Dec',
        'Monthly_ave'
    ]
    CLIMATOLOGY_PREFIX = [
        'Jan',
        'Feb',
        'Mar',
        'Apr',
        'May',
        'Jun',
        'Jul',
        'Aug',
        'Sep',
        'Oct',
        'Nov',
        'Dec',
        'Annual'
    ]


class Definitions:
    # Solar fluxes and related
    SKY_SURFACE_SW_IRRADIANCE = {
        'key': 'ALLSKY_SFC_SW_DWN',
        'name': 'Sky_surface_shortwave_irradiance',
        'description': 'All sky surface shortwave downward irradiance'
    }
    SKY_SURFACE_SW_IRRADIANCE_GMT = {
        'key': 'ALLSKY_SFC_SW_DWN_HR',
        'name': 'Sky_surface_shortwave_irradiance_GMT',
        'description': 'All sky surface shortwave downward irradiance at GMT times'
    }
    CLEAR_SKY_SURFACE_SW_IRRADIANCE = {
        'key': 'CLRSKY_SFC_SW_DWN',
        'name': 'Clear_sky_surface_shortwave_irradiance',
        'description': 'Clear sky surface shortwave'
    }
    SKY_INSOLATION_CLEARNESS_INDEX = {
        'key': 'ALLSKY_KT',
        'name': 'Sky_insolation_clearness_index',
        'description': 'All sky insolation clearness index'
    }
    CLEAR_SKY_INSOLATION_CLEARNESS_INDEX = {
        'key': 'CLRSKY_KT',
        'name': 'Clear_sky_insolation_clearness_index',
        'description': 'Clear sky insolation clearness index'
    }
    SKY_SURFACE_LW_IRRADIANCE = {
        'key': 'ALLSKY_SFC_LW_DWN',
        'name': 'Sky_surface_longwave',
        'description': 'All sky surface longwave downward irradiance (thermal infrared)'
    }
    SKY_SURFACE_PS_ACTIVE_RADIATION = {
        'key': 'ALLSKY_SFC_PAR_TOT',
        'name': 'Sky_surface_PAR_total',
        'description': 'All sky surface photosynthetically active radiation (PAR) total'
    }
    CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION = {
        'key': 'CLRSKY_SFC_PAR_TOT',
        'name': 'Clear_sky_surface_PAR_total',
        'description': 'Clear sky surface photosynthetically active radiation (PAR) total'
    }
    SKY_SURFACE_UVA_IRRADIANCE = {
        'key': 'ALLSKY_SFC_UVA',
        'name': 'Sky_surface_UVA',
        'description': 'All sky surface UVA irradiance'
    }
    SKY_SURFACE_UVB_IRRADIANCE = {
        'key': 'ALLSKY_SFC_UVB',
        'name': 'Sky_surface_UVB',
        'description': 'All sky surface UVB irradiance'
    }
    SKY_SURFACE_UV_INDEX = {
        'key': 'ALLSKY_SFC_UV_INDEX',
        'name': 'Sky_surface_UV_index',
        'description': 'All sky surface UV index'
    }
    SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE = {
        'key': 'ALLSKY_SFC_SW_DNI',
        'name': 'Sky_surface_shortwave_direct_normal_irradiance',
        'description': 'All sky surface shortwave downward direct normal irradiance'
    }
    SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MAX = {
        'key': 'ALLSKY_SFC_SW_DNI_MAX',
        'name': 'Sky_surface_shortwave_direct_normal_irradiance_max',
        'description': 'All sky surface shortwave direct normal irradiance maximum'
    }
    SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MIN = {
        'key': 'ALLSKY_SFC_SW_DNI_MIN',
        'name': 'Sky_surface_shortwave_direct_normal_irradiance_min',
        'description': 'All sky surface shortwave direct normal irradiance minimum'
    }
    SKY_SURFACE_SW_DIFFUSE_IRRADIANCE = {
        'key': 'ALLSKY_SFC_SW_DIFF',
        'name': 'Sky_surface_shortwave_diffuse_irradiance',
        'description': 'All sky surface shortwave diffuse irradiance'
    }
    SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MAX = {
        'key': 'ALLSKY_SFC_SW_DIFF_MAX',
        'name': 'Sky_surface_shortwave_diffuse_irradiance_max',
        'description': 'All sky surface shortwave diffuse irradiance maximum'
    }
    SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MIN = {
        'key': 'ALLSKY_SFC_SW_DIFF_MIN',
        'name': 'Sky_surface_shortwave_diffuse_irradiance_min',
        'description': 'All sky surface shortwave diffuse irradiance minimum'
    }
    SKY_SURFACE_ALBEDO = {
        'key': 'ALLSKY_SRF_ALB',
        'name': 'Sky_surface_albedo',
        'description': 'All sky surface albedo'
    }
    TOA_SW_IRRADIANCE = {
        'key': 'TOA_SW_DWN',
        'name': 'TOA_shortwave_irradiance',
        'description': 'Top-of-atmosphere shortwave downward irradiance'
    }
    CLOUD_AMOUNT = {
        'key': 'CLOUD_AMT',
        'name': 'Cloud_amount',
        'description': 'Cloud amount'
    }
    CLOUD_AMOUNT_GMT = {
        'key': 'CLOUD_AMT_HR',
        'name': '',
        'description': 'Cloud amount at GMT times'
    }

    # Parameters for solar cooking
    WINDSPEED_2M = {
        'key': 'WS2M',
        'name': 'Windspeed_02m',
        'description': 'Windspeed at 2 meters'
    }
    MIDDAY_INSOLATION_INCIDENT = {
        'key': 'MIDDAY_INSOL',
        'name': 'Midday_insolation',
        'description': 'Midday insolation incident'
    }

    # Temperature/Precipitation
    TEMP = {
        'key': 'T2M',
        'name': 'Temperature',
        'description': 'Temperature at 2 meters'
    }
    DEW_FROST = {
        'key': 'T2MDEW',
        'name': 'Frost',
        'description': 'Dew/frost point at 2 meters'
    }
    WET_TEMP = {
        'key': 'T2MWET',
        'name': 'Wet_temperature',
        'description': 'Wet bulb temperature at 2 meters'
    }
    EARTH_SKIN_TEMP = {
        'key': 'TS',
        'name': 'Earth_skin_temperature',
        'description': 'Earth skin temperature'
    }
    EARTH_SKIN_TEMP_MAX = {
        'key': 'TS_MAX',
        'name': 'Earth_skin_temperature_max',
        'description': 'Earth skin temperature maximum'
    }
    EARTH_SKIN_TEMP_MIN = {
        'key': 'TS_MIN',
        'name': 'Earth_skin_temperature_min',
        'description': 'Earth skin temperature minimum'
    }
    TEMP_RANGE = {
        'key': 'T2M_RANGE',
        'name': 'Temperature_range',
        'description': 'Temperature at 2 meters range'
    }
    TEMP_MAX = {
        'key': 'T2M_MAX',
        'name': 'Temperature_max',
        'description': 'Temperature at 2 meters maximum'
    }
    TEMP_MIN = {
        'key': 'T2M_MIN',
        'name': 'Temperature_min',
        'description': 'Temperature at 2 meters minimum'
    }
    SPECIFIC_HUMIDITY = {
        'key': 'QV2M',
        'name': 'Specific_humidity',
        'description': 'Specific humidity at 2 meters'
    }
    RELATIVE_HUMIDITY = {
        'key': 'RH2M',
        'name': 'Relative_humidity',
        'description': 'Relative humidity at meters'
    }
    PRECIPITATION = {
        'key': 'PRECTOTCORR',
        'name': 'Precipitation_ave',
        'description': 'Average precipitation'
    }
    PRECIPITATION_SUM = {
        'key': 'PRECTOTCORR_SUM',
        'name': 'Precipitation_ave_sum',
        'description': 'Precipitation sum average'
    }

    # Wind/Pressure
    SURFACE_PRESSURE = {
        'key': 'PS',
        'name': 'Surface_pressure',
        'description': 'Surface pressure'
    }
    WINDSPEED_10M = {
        'key': 'WS10M',
        'name': 'Windspeed_10m',
        'description': 'Wind speed at 10 meters'
    }
    WINDSPEED_10M_MAX = {
        'key': 'WS10M_MAX',
        'name': 'Windspeed_max_10m',
        'description': 'Wind speed at 10 meters maximum'
    }
    WINDSPEED_10M_MIN = {
        'key': 'WS10M_MIN',
        'name': 'Windspeed_min_10m',
        'description': 'Wind speed at 10 meters minimum'
    }
    WINDSPEED_10M_RANGE = {
        'key': 'WS10M_RANGE',
        'name': 'Windspeed_range_10m',
        'description': 'Wind speed at 10 meters range'
    }
    WIND_DIRECTION_10M = {
        'key': 'WD10M',
        'name': 'Wind_direction_10m',
        'description': 'Wind direction at 10 meters'
    }
    WINDSPEED_50M = {
        'key': 'WS50M',
        'name': 'Windspeed_50m',
        'description': 'Wind speed at 50 meters'
    }
    WINDSPEED_50M_MAX = {
        'key': 'WS50M_MAX',
        'name': 'Windspeed_max_50m',
        'description': 'Wind speed at 50 meters maximum'
    }
    WINDSPEED_50M_MIN = {
        'key': 'WS50M_MIN',
        'name': 'Windspeed_min_50m',
        'description': 'Wind speed at 50 meters minimum'
    }
    WINDSPEED_50M_RANGE = {
        'key': 'WS50M_RANGE',
        'name': 'Windspeed_range_50m',
        'description': 'Wind speed at 50 meters range'
    }
    WIND_DIRECTION_50M = {
        'key': 'WD50M',
        'name': 'Wind_direction_50m',
        'description': 'Wind direction at 50 meters'
    }

    # DOE/ASHRAE climate building
    COOLING_DEGREE_DAYS_ABOVE_ZERO = {
        'key': 'CDD0',
        'name': 'Cooling_degree_days_above_00',
        'description': 'Cooling degree days above 0 celcius'
    }
    COOLING_DEGREE_DAYS_ABOVE_10 = {
        'key': 'CDD10',
        'name': 'Cooling_degree_days_above_10',
        'description': 'Cooling degree days above 10 celcius'
    }
    COOLING_DEGREE_DAYS_ABOVE_18 = {
        'key': 'CDD18_3',
        'name': 'Cooling_degree_days_above_18_3',
        'description': 'Cooling degree days above 18.3 celcius'
    }
    HEATING_DEGREE_DAYS_BELOW_ZERO = {
        'key': 'HDD0',
        'name': 'Heating_degree_days_below_00',
        'description': 'Heating degree days below 0 celcius'
    }
    HEATING_DEGREE_DAYS_BELOW_10 = {
        'key': 'HDD10',
        'name': 'Heating_degree_days_below_10',
        'description': 'Heating degree days below 10 celcius'
    }
    HEATING_DEGREE_DAYS_BELOW_18 = {
        'key': 'HDD18_3',
        'name': 'Heating_degree_days_below_18_3',
        'description': 'Heating degree days below 18.3 celcius'
    }

    # Soil properties
    SURFACE_SOIL_WETNESS = {
        'key': 'GWETTOP',
        'name': 'Surface_soil_wetness',
        'description': 'Surface soil wetness (surface to 5 cm below)'
    }
    ROOT_SOIL_WETNESS = {
        'key': 'GWETROOT',
        'name': 'Root_soil_wetness',
        'description': 'Root zone soil wetness (surface to 100 cm below)'
    }
    PROFILE_SOIL_MOISTURE = {
        'key': 'GWETPROF',
        'name': 'Profile_soil_moisture',
        'description': 'Profile soil moisture (surface to bedrock)'
    }

    # Dataset lists
    LIST_NASA_POWER_DATASETS_RE_DAILY = [
        # SKY_SURFACE_SW_IRRADIANCE,  # include
        # CLEAR_SKY_SURFACE_SW_IRRADIANCE,
        # SKY_INSOLATION_CLEARNESS_INDEX,
        # SKY_SURFACE_LW_IRRADIANCE,
        # SKY_SURFACE_PS_ACTIVE_RADIATION,
        # CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION,
        # SKY_SURFACE_UVA_IRRADIANCE,
        # SKY_SURFACE_UVB_IRRADIANCE,
        # SKY_SURFACE_UV_INDEX,
        WINDSPEED_2M,  # include
        TEMP,  # include
        # DEW_FROST,  # include
        # WET_TEMP,  # include
        # EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        # TEMP_MAX,  # include
        # TEMP_MIN,  # include
        # SPECIFIC_HUMIDITY,
        # RELATIVE_HUMIDITY,  # include
        # PRECIPITATION,  # include
        # SURFACE_PRESSURE,
        # WINDSPEED_10M,  # include
        # WINDSPEED_10M_MAX,
        # WINDSPEED_10M_MIN,
        # WINDSPEED_10M_RANGE,
        # WIND_DIRECTION_10M,
        # WINDSPEED_50M,
        # WINDSPEED_50M_MAX,
        # WINDSPEED_50M_MIN,
        # WINDSPEED_50M_RANGE,
        # WIND_DIRECTION_50M,
    ]
    LIST_NASA_POWER_DATASETS_SB_DAILY = [
        # COOLING_DEGREE_DAYS_ABOVE_ZERO,
        # COOLING_DEGREE_DAYS_ABOVE_10,
        # COOLING_DEGREE_DAYS_ABOVE_18,
        # HEATING_DEGREE_DAYS_BELOW_ZERO,
        # HEATING_DEGREE_DAYS_BELOW_10,
        # HEATING_DEGREE_DAYS_BELOW_18
    ]
    LIST_NASA_POWER_DATASETS_AG_DAILY = [
        # SURFACE_SOIL_WETNESS,  # include
        # ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]

    LIST_NASA_POWER_DATASETS_RE_MONTHLY = [
        #SKY_SURFACE_SW_IRRADIANCE,
        # CLEAR_SKY_SURFACE_SW_IRRADIANCE,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE,
        # SKY_INSOLATION_CLEARNESS_INDEX,
        # CLEAR_SKY_INSOLATION_CLEARNESS_INDEX,
        # SKY_SURFACE_ALBEDO,
        # TOA_SW_IRRADIANCE,
        CLOUD_AMOUNT,  # include
        #SKY_SURFACE_PS_ACTIVE_RADIATION,
        # CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION,
        # SKY_SURFACE_UVA_IRRADIANCE,
        # SKY_SURFACE_UVB_IRRADIANCE,
        # SKY_SURFACE_UV_INDEX,
        #WINDSPEED_2M,  # include
        TEMP,  # include
        #DEW_FROST,  # include
        #WET_TEMP,  # include
        #EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        #TEMP_MAX,  # include
        #TEMP_MIN,  # include
        # SKY_SURFACE_LW_IRRADIANCE,
        #SPECIFIC_HUMIDITY,  # include
        #RELATIVE_HUMIDITY,  # include
        #PRECIPITATION,  # include
        #PRECIPITATION_SUM,  # include
        # SURFACE_PRESSURE,
        #WINDSPEED_10M,  # include
        # WINDSPEED_10M_MAX,
        # WINDSPEED_10M_MIN,
        # WINDSPEED_10M_RANGE,
        # WIND_DIRECTION_10M,
        # WINDSPEED_50M,
        # WINDSPEED_50M_MAX,
        # WINDSPEED_50M_MIN,
        # WINDSPEED_50M_RANGE,
        # WIND_DIRECTION_50M
    ]
    LIST_NASA_POWER_DATASETS_SB_MONTHLY = [
        # COOLING_DEGREE_DAYS_ABOVE_ZERO,
        # COOLING_DEGREE_DAYS_ABOVE_10,
        # COOLING_DEGREE_DAYS_ABOVE_18,
        # HEATING_DEGREE_DAYS_BELOW_ZERO,
        # HEATING_DEGREE_DAYS_BELOW_10,
        # HEATING_DEGREE_DAYS_BELOW_18
    ]
    LIST_NASA_POWER_DATASETS_AG_MONTHLY = [
        #SURFACE_SOIL_WETNESS,  # include
        #ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]

    LIST_NASA_POWER_DATASETS_RE_CLIMATOLOGY = [
        #SKY_SURFACE_SW_IRRADIANCE,  # include
        # SKY_SURFACE_SW_IRRADIANCE_GMT,
        # CLEAR_SKY_SURFACE_SW_IRRADIANCE,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE,
        # SKY_INSOLATION_CLEARNESS_INDEX,
        # SKY_SURFACE_ALBEDO,
        # TOA_SW_IRRADIANCE,
        CLOUD_AMOUNT,  # include
        #SKY_SURFACE_PS_ACTIVE_RADIATION,  # include
        #CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION,  # include
        # SKY_SURFACE_UVA_IRRADIANCE,
        # SKY_SURFACE_UVB_IRRADIANCE,
        # SKY_SURFACE_UV_INDEX,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MAX,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MIN,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MAX,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MIN,
        # MIDDAY_INSOLATION_INCIDENT,
        #WINDSPEED_2M,  # include
        TEMP,  # include
        #DEW_FROST,  # include
        #WET_TEMP,  # include
        #EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        #TEMP_MAX,  # include
        #TEMP_MIN,  # include
        # EARTH_SKIN_TEMP_MAX,
        # EARTH_SKIN_TEMP_MIN,
        # SKY_SURFACE_LW_IRRADIANCE,
        #SPECIFIC_HUMIDITY,  # include
        #RELATIVE_HUMIDITY,  # include
        #PRECIPITATION,  # include
        #PRECIPITATION_SUM,  # include
        # SURFACE_PRESSURE,
        #WINDSPEED_10M,  # include
        # WINDSPEED_10M_MAX,
        # WINDSPEED_10M_MIN,
        # WINDSPEED_10M_RANGE,
        # WINDSPEED_50M,
        # WINDSPEED_50M_MAX,
        # WINDSPEED_50M_MIN,
        # WINDSPEED_50M_RANGE,
        # CLOUD_AMOUNT_GMT
    ]
    LIST_NASA_POWER_DATASETS_SB_CLIMATOLOGY = [
        # COOLING_DEGREE_DAYS_ABOVE_ZERO,
        # COOLING_DEGREE_DAYS_ABOVE_10,
        # COOLING_DEGREE_DAYS_ABOVE_18,
        # HEATING_DEGREE_DAYS_BELOW_ZERO,
        # HEATING_DEGREE_DAYS_BELOW_10,
        # HEATING_DEGREE_DAYS_BELOW_18
    ]
    LIST_NASA_POWER_DATASETS_AG_CLIMATOLOGY = [
        #SURFACE_SOIL_WETNESS,  # include
        #ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]


class Utilities:
    @staticmethod
    def is_float(string):
        """Checks if a given string can be converted to float.

        :param string: String variable which will be tested if its a float value
        :type string: str

        :returns: True if its float, otherwise False
        :rtype: boolean
        """
        try:
            float(string)
        except ValueError:
            return False
        return True

    @staticmethod
    def is_date(string):
        """Checks if a given string can be converted to a date. Allowed date formats
        can be set by Default.ALLOWED_DATE_STRUCTURES (e.g. %Y-%m-%d).

        :param string: String which will be tested if it is a valid date
        :type string: str

        :returns: True if the value can be converted to a date object, otherwise False
        :rtype: boolean
        """
        for date_structure in Default.ALLOWED_DATE_STRUCTURES:
            try:
                # Attempts to read the string as a date
                datetime.strptime(string, date_structure)
                return True
            except ValueError:
                # Check the next date structure
                continue
        return False

    @staticmethod
    def remove_unwanted_chars(string):
        """Removes all unwanted characters from a string. The possible characters can be set
        by Default.CHARS_TO_REMOVE. This is done to check if a value can possibly be a float (e.g. 20 000)
        or invalid characters from field names.

        :param string: String which will be from which unwanted characters will be removed.
        :type string: str

        :returns: Updated string with no unwanted characters
        :rtype: str, float
        """
        if isinstance(string, str):  # Checks if the variable stores a string
            updated_string = string
            for char in Default.CHARS_TO_REMOVE:
                updated_string = updated_string.replace(char, '')

            if Utilities.is_float(updated_string):
                return float(updated_string)

        return string

    @staticmethod
    def parse_column_names(list_column_names):
        """Parses column names provided. Will remove all unwanted charcters from the column names. Returns the
        updated names

        :param list_column_names: The lines received from NASA POWER
        :type list_column_names: list

        :returns: A list containing the updated field names
        :rtype: list
        """
        parsed_names = []
        for name in list_column_names:
            new_name = name
            for char in Default.FIELD_NAME_INVALID_CHARS:
                # Removes all wanted characters
                new_name = new_name.replace(char, '_')

            if new_name[0].isdigit():
                # Field name cannot start with a digit
                new_name = '_' + new_name

            parsed_names.append(new_name)
        return parsed_names

    @staticmethod
    def parse_date(date):
        """Check if the date is valid and whether it can be converted to the valid BigQuery date format (%Y-%m-%d).

        :param date: Date to be checked
        :type date: str

        :returns: Date in BigQuery format
        :rtype: date
        """
        parsed_date = None
        for date_structure in Default.ALLOWED_DATE_STRUCTURES:
            try:
                # Checks if the date structure is accepted
                datetime.strptime(date, date_structure)
                parsed_date = datetime.strptime(date, date_structure).strftime(Default.BIGQUERY_DATETIME_STRUCTURE)
            except ValueError:
                # Check the next date structure
                continue

        return parsed_date

    @staticmethod
    def list_bucket_data(project, bucket):
        """Lists files contained in a bucket.

        :param project: Google cloud platform project ID
        :type project: String

        :param bucket: Google cloud platform bucket name
        :type bucket: String

        :returns: Lists containing files directories.
        :rtype: List
        """
        # Storage client
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket)

        list_csv = []
        list_excel = []
        list_archives = []
        list_raster = []
        list_vector = []

        for blob in bucket.list_blobs():
            content_name = blob.name
            if content_name.endswith('.csv'):
                list_csv.append(content_name)
            elif content_name.endswith('.zip') or content_name.endswith('.7z'):
                list_archives.append(content_name)
            elif content_name.endswith('.shp'):
                list_vector.append(content_name)

        return list_csv, list_excel, list_archives, list_raster, list_vector

    @staticmethod
    def move_data(source_bucket, destination_bucket, source_file, destination_file):
        """Moves a file from a bucket to other location or bucket.

        :param source_bucket: Bucket name where the file is stored
        :type source_bucket: String

        :param destination_bucket: Destination bucket name to which the file will be moved
        :type destination_bucket: String

        :param source_file: File which will be moved
        :type source_file: String

        :param destination_file: Location to which the file will be moved
        :type destination_file: String

        :returns: True if data move has been successful, false if data move has failed
        :rtype: boolean
        """
        try:
            client = storage.Client(project=Default.PROJECT_ID)
            bucket = client.bucket(source_bucket)
            destination_bucket = client.bucket(destination_bucket)

            blob_to_move = bucket.blob(source_file)
            bucket.copy_blob(
                blob_to_move,
                destination_bucket,
                destination_file
            )
        except Exception as e:
            # Loading from csv file into bigquery failed
            # The newly created bigquery table will be deleted
            print("Could not move " + source_file)
            print("EXCEPTION: " + str(e))
            return False

        # Deletes the source file if the file has been copied to the destination
        bucket.delete_blob(source_file)

        # Returns if the file has been moved
        return True

    @staticmethod
    def convert_json_to_newline_json(data_json):
        """Convert JSON/GEOJSON to newline JSON. This is required for BigQuery table creation.

        :param data_json: JSON/GEOJSON data
        :type data_json: JSON/GEOJSON

        :returns: Newline JSON formatted string
        :rtype: Newline JSON
        """
        list_newline_json = []

        now = datetime.now()
        print('[' + str(now) + '] ' + "Converting to newline JSON")

        list_features = data_json['features']
        for feature in list_features:
            feat_id = feature['id']
            feat_geom = feature['geometry']

            coordinates = feat_geom['coordinates']
            feat_geom['coordinates'] = str(coordinates)

            new_json_feat = {
                'id': int(feat_id),
                'geometry': feat_geom
            }

            # A list of the all other attribute fields
            feat_properties = feature['properties']
            for feat_property in feat_properties:
                if feat_property not in new_json_feat:
                    new_json_feat[feat_property] = feat_properties[feat_property]

            list_newline_json.append(new_json_feat)

        return list_newline_json

    @staticmethod
    def shp_to_geojson(shp_file):
        """Converts a shapefile stored in Google cloud storage GEOJSON.

        :param shp_file: Google cloud storage directory of the shapefile (e.g. gs://bucket/folder/file.shp)
        :type shp_file: Shapefile
        """
        gc_shp = 'gs://' + Default.BUCKET_TRIGGER + '/' + shp_file

        now = datetime.now()
        print('[' + str(now) + '] ' + "Reading the shapefile using geopandas")
        shp_geopandas = geopandas.read_file(gc_shp)
        list_data_types = shp_geopandas.dtypes

        shp_json = json.loads(shp_geopandas.to_json())
        newline_json = Utilities.convert_json_to_newline_json(shp_json)

        schema = [
            bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('geometry', 'GEOGRAPHY', mode='NULLABLE')
        ]

        first_feat = newline_json[0]
        for field_name in first_feat:
            if field_name == 'id' or field_name == 'geometry':
                # Skips these fields as its already added
                continue

            field_type = list_data_types[field_name]
            if pandas.api.types.is_integer_dtype(field_type):
                f_type = 'INTEGER'
            elif pandas.api.types.is_float_dtype(field_type):
                f_type = 'FLOAT'
            else:
                f_type = 'STRING'

            schema.append(bigquery.SchemaField(
                field_name,
                f_type,
                mode='NULLABLE'
            ))

        client_bq = bigquery.Client()

        table_name = os.path.basename(shp_file).replace('.shp', '')
        bq_table_uri = Default.PROJECT_ID + '.' + Default.BIGQUERY_DATASET_BUCKET + '.' + table_name

        table = bigquery.Table(bq_table_uri, schema=schema)
        client_bq.create_table(table)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        load_job = client_bq.load_table_from_json(newline_json, bq_table_uri, job_config=job_config)
        load_job.result()

    @staticmethod
    def write_to_file(file, lines):
        """Writes lines to a text/CSV file.

        :param file: Output/target file
        :type file: String

        :param lines: Variable which contains a list of the lines which needs to be written to the file
        :type lines: List
        """
        if not os.path.exists(file):
            # File does not exist, create it
            with open(file, 'w+') as f:
                for line in lines:
                    f.write(line)
                    f.write('\n')
        else:
            # File exists, append to it
            with open(file, 'a') as f:
                for line in lines:
                    f.write(line)
                    f.write('\n')

        f.close()

    @staticmethod
    def write_to_log(file, line):
        """Writes to a log text file.

        :param file: Output/target file
        :type file: String

        :param line: Variable which contains a list of the lines which needs to be written to the log file
        :type line: List
        """
        if True:
            now = datetime.now()
            line = '[' + str(now) + '] ' + line
            print(line)

            if not os.path.exists(file):
                # File does not exist, create it
                with open(file, 'w+') as f:
                    f.write(line)
                    f.write('\n')
            else:
                # File exists, append to it
                with open(file, 'a') as f:
                    f.write(line)
                    f.write('\n')

            f.close()

    @staticmethod
    def unzip(zip_file):
        """Unzips a zip file stored in a Google cloud storage bucket.

        :param zip_file: Google cloud storage directory (e.g. gs://bucket/folder/file.zip)
        :type zip_file: String

        :returns: True if the zip file extracted successfully, false if it failed
        :rtype: boolean
        """
        try:
            client = storage.Client(project=Default.PROJECT_ID)
            bucket = client.get_bucket(Default.BUCKET_TRIGGER)

            blob = bucket.blob(zip_file)
            zipbytes = io.BytesIO(blob.download_as_string())

            if zipfile.is_zipfile(zipbytes):
                with zipfile.ZipFile(zipbytes, 'r') as myzip:
                    for contentfilename in myzip.namelist():
                        contentfile = myzip.read(contentfilename)

                        file_gs_path = os.path.dirname(zip_file)

                        # The extracted files goes to the same folder as the zip file
                        if file_gs_path == '':
                            blob_upload = bucket.blob(contentfilename)
                        else:
                            blob_upload = bucket.blob(file_gs_path + '/' + contentfilename)
                        blob_upload.upload_from_string(contentfile)
        except Exception as e:
            # Loading from csv file into bigquery failed
            # The newly created bigquery table will be deleted
            print("Could unzip " + zip_file)
            print("EXCEPTION: " + str(e))
            return False

        # Returns True if the file unzipped correctly
        return True

    @staticmethod
    def get_dataset_list(community, temporal):
        """Unzips a zip file stored in a Google cloud storage bucket.

        :param community: Renewable energy ('RE'), sustainable buildings ('SB'), or agroclimatology ('AG')
        :type community: String

        :param temporal: 'daily', 'monthly', or 'climatology'
        :type temporal: String

        :returns: A list contains the datasets which will be downloaded from NASA POWER
        :rtype: list
        """
        if community == 'RE':
            # Renewable energy
            if temporal == Default.DAILY:
                return Definitions.LIST_NASA_POWER_DATASETS_RE_DAILY
            elif temporal == Default.MONTHLY:
                return Definitions.LIST_NASA_POWER_DATASETS_RE_MONTHLY
            elif temporal == Default.CLIMATOLOGY:
                return Definitions.LIST_NASA_POWER_DATASETS_RE_CLIMATOLOGY
        elif community == 'SB':
            # Sustainable buildings
            if temporal == Default.DAILY:
                return Definitions.LIST_NASA_POWER_DATASETS_SB_DAILY
            elif temporal == Default.MONTHLY:
                return Definitions.LIST_NASA_POWER_DATASETS_SB_MONTHLY
            elif temporal == Default.CLIMATOLOGY:
                return Definitions.LIST_NASA_POWER_DATASETS_SB_CLIMATOLOGY
        elif community == 'AG':
            # Agroclimatology
            if temporal == Default.DAILY:
                return Definitions.LIST_NASA_POWER_DATASETS_AG_DAILY
            elif temporal == Default.MONTHLY:
                return Definitions.LIST_NASA_POWER_DATASETS_AG_MONTHLY
            elif temporal == Default.CLIMATOLOGY:
                return Definitions.LIST_NASA_POWER_DATASETS_AG_CLIMATOLOGY

        # List could not be determined
        return []

    @staticmethod
    def append_field_names(list_field_names, period, name, start_date, end_date):
        """Appends field names to an existing list. Daily only requires a single field,
        monthly requires 13 (Jan to Dec and average monthly), and climatology also
        requires 13 (Jan to Dec and annual).

        :param list_field_names: List of fieldnames
        :type list_field_names: list

        :param period: 'daily', 'monthly', or 'climatology'
        :type period: String

        :param name: The prefix to add to the field name
        :type name: String

        :param start_date: Start of the date range
        :type start_date: String

        :param end_date: End of the date range
        :type end_date: String

        :returns: Contains the updated fields list
        :rtype: list
        """

        if period == Default.DAILY:
            if Default.DAILY_DATES_FREQUENCY == 'D':
                # If daily, then only one date
                # Used when the code is deployed as a cloud function
                field_name = '{}_{}'.format(
                    name,
                    start_date
                )
                list_field_names.append(field_name)
            else:
                # If monthly, then all days of the month
                # Used for bulk downloading of daily data
                day_count = Utilities.get_day_count(
                    start_date,
                    end_date
                )

                i = 1
                while i <= day_count:
                    if i <= 9:
                        # For the first 9 numbers a '0' is added (e.g. 19980109)
                        date = start_date[:len(start_date) - 2] + '0' + str(i)
                    else:
                        date = start_date[:len(start_date) - 2] + str(i)
                    field_name = '{}_{}'.format(
                        name,
                        date
                    )
                    list_field_names.append(field_name)
                    i = i + 1
        elif period == Default.MONTHLY:
            for field_prefix in Default.MONTHLY_PREFIX:
                field_name = '{}_{}_{}'.format(
                    name,
                    field_prefix,
                    start_date
                )
                list_field_names.append(field_name)
        else:
            # Climatology
            for field_prefix in Default.CLIMATOLOGY_PREFIX:
                field_name = '{}_{}'.format(
                    name,
                    field_prefix
                )
                list_field_names.append(field_name)

        return list_field_names

    @staticmethod
    def table_to_geojson(dataset_id, table_name):
        client = bigquery.Client()

        table_json = table_name + ".json"
        destination_uri = "gs://{}/{}".format(Default.BUCKET_TEMP, table_json)
        dataset_ref = bigquery.DatasetReference(Default.PROJECT_ID, dataset_id)
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            # Location must match that of the source table.
            location=Default.REGION,
        )  # API request
        extract_job.result()  # Waits for job to complete.

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(Default.BUCKET_TEMP)
        blob = bucket.blob(table_json)

        blob_string = str(blob.download_as_string(client=None))
        blob_string = blob_string.replace('\'b', '')
        blob_string = blob_string[2:]  # Removes '\b' at the start of the string
        split_json = blob_string.split(r'\n')  # Creates a list
        split_json = split_json[:len(split_json) - 1]  # Removes the last element of the list

        blob_json = [json.loads(line) for line in split_json]
        data_geojson = Utilities.json_to_geojson(blob_json, 0, 1)

        bucket.delete_blob(table_json)

        return data_geojson

    @staticmethod
    def get_request_content_indices(period):
        """Gets the latitude index, longitude index, and a list of indices for the
        CSV file contents received from NASA POWER when a request is done. These indices
        differs for daily, monthly, and climatology.

        :param period: 'daily', 'monthly', or 'climatology'
        :type period: String

        :returns: lat_index contains the latitude index
        :rtype: integer

        :returns: lon_index contains the longitude index
        :rtype: integer

        :returns: value_index contains a list of indices for the values
        :rtype: list
        """
        if period == Default.DAILY:
            # Only a lat, long, and a single value
            lat_index = 0
            lon_index = 1
            value_index = [5]
        elif period == Default.MONTHLY:
            # Lat, lon, all months and monthly average
            lat_index = 2
            lon_index = 3
            value_index = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        else:
            # Climatology. Lat, lon, all months, and annual
            lat_index = 1
            lon_index = 2
            value_index = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        return lat_index, lon_index, value_index

    @staticmethod
    def get_day_count(start_date, end_date):
        """Get the number of days in the given date range.

        :param start_date: Start date of the date range (YYYYMMDD)
        :type start_date: str

        :param end_date: End date of the date range (YYYYMMDD)
        :type end_date: str

        :returns: Number of days in the given date range
        :rtype: int
        """
        frequency = 'D'  # Daily
        date_format = "%Y%m%d"  # YYYYMMDD
        list_dates = pd.date_range(start_date, end_date, freq=frequency).strftime(date_format).tolist()
        number_of_days = len(list_dates)

        return number_of_days

    @staticmethod
    def get_date_list(temporal, start_year, end_year, start_month, end_month, start_day, end_day):
        """Based on the temporal type and dates provided, this function returns a list of dates.
        For daily, it will return all days, monthly all the months, and for climatology it will return no dates.
        Climatology does not require dates.

        :param temporal: 'daily', 'monthly', or 'climatology'
        :type temporal: String

        :param start_year: Start year
        :type start_year: int

        :param end_year: End year
        :type end_year: int

        :param start_month: Start month
        :type start_month: int

        :param end_month: End month
        :type end_month: int

        :param start_day: Start day
        :type start_day: int

        :param end_day: End day
        :type end_day: int

        :returns: True if a date will be required for the request, otherwise False
        :rtype: boolean

        :returns: A list which contains the dates which will be used to perform requests
        :rtype: list
        """
        if temporal == Default.DAILY:
            if Default.DAILY_DATES_FREQUENCY == 'D':
                if start_month < 10:
                    s_month = '0' + str(start_month)
                else:
                    s_month = str(start_month)
                if start_day < 10:
                    s_day = '0' + str(start_day)
                else:
                    s_day = str(start_day)
                # Downloads on a daily basis. Use for Cloud function
                daily_date = '{}{}{}'.format(start_year, s_month, s_day)
                list_dates = [
                    {
                        'start_date': daily_date,
                        'end_date': daily_date
                    }
                ]
                dates_required = True
            else:
                # Used for bulk downloading. When set to 'M'
                # Converts all values to string
                # This is required for the pandas date_range method
                start_year = str(start_year)
                end_year = str(end_year)
                start_month = str(start_month)
                end_month = str(end_month)
                start_day = str(start_day)
                end_day = str(end_day)

                list_dates = []

                # YYYYMMDD
                pd_start_date = "{}-{}-{}".format(start_year, start_month, start_day)
                pd_end_date = "{}-{}-{}".format(end_year, end_month, end_day)

                frequency = 'M'  # All days in a month
                date_format = "%Y%m%d"
                list_days_temp = pd.date_range(pd_start_date, pd_end_date, freq=frequency).strftime(
                    date_format).tolist()

                for day in list_days_temp:
                    list_dates.append(
                        {
                            'start_date': day[:len(day) - 2] + '01',
                            'end_date': day
                        }
                    )
                dates_required = True
        elif temporal == Default.MONTHLY:
            list_years_temp = range(start_year, end_year + 1)
            list_dates = []

            for year in list_years_temp:
                dates = {
                    'start_date': str(year),
                    'end_date': str(year)
                }
                list_dates.append(dates)
            dates_required = True
        else:
            # Climatology requires no date
            # A single item added so that it enters the loop
            # Value will not be used
            list_dates = [-1]
            dates_required = False

        return dates_required, list_dates

    @staticmethod
    def transform_daily_data(lines):
        """Transforms the contents received from NASA POWER by creating columns for each day from the
        received contents.

        :param lines: The lines received from NASA POWER
        :type lines: list

        :returns: List which contains each of the transformed rows. The rows will now consist of columns
        :rtype: list
        """
        columns = []
        column_contents = []
        cur_day = None
        for line in lines:
            split_line = line.split(',')  # All columns of the row
            day = int(split_line[4])

            if cur_day is None:
                # Only performed at the start of the transformation
                cur_day = day

            if day > cur_day:
                # Next column (next day)
                columns.append(column_contents)
                column_contents = [line]
                cur_day = day
            else:
                # Append to current column (continuation of the same day)
                column_contents.append(line)
        columns.append(column_contents)  # Appends the new columns

        column_count = len(columns)  # Number of days in the month
        transformed_lines = []
        # Creates the new rows from the daily columns
        if column_count > 0:
            contents_count = len(columns[0])
            i = 0
            # Creates each row
            while i < contents_count:
                transformed_line = ''
                for column in columns:
                    column_line = column[i]
                    if transformed_line == '':
                        # Includes lat and lon if it's the start of a row
                        column_line_split = column_line.split(',')
                        lat = column_line_split[0]
                        lon = column_line_split[1]
                        value = column_line_split[5]
                        transformed_line = "{},{},{}".format(
                            lat,
                            lon,
                            value
                        )
                    else:
                        # Lat and lon no longer required, as it exists in the row
                        column_line_split = column_line.split(',')
                        transformed_line = "{},{}".format(
                            transformed_line,
                            column_line_split[5]
                        )
                transformed_lines.append(transformed_line)  # Adds the transformed row
                i = i + 1

        return transformed_lines

    @staticmethod
    def get_bq_dataset(period):
        """Gets the list of datasets associated with the provided temporal type.

        :param period: Temporal period (e.g. daily, monthly, or climatology)
        :type period: str

        :returns: All the datasets (e.g. windspeed) with the given temporal type
        :rtype: str
        """
        if period == Default.DAILY:
            return Default.LIST_BQ_DATASETS[0]
        elif period == Default.MONTHLY:
            return Default.LIST_BQ_DATASETS[1]
        else:
            return Default.LIST_BQ_DATASETS[2]

    @staticmethod
    def json_to_geojson(data_json, lat_index, lon_index):
        """Takes newline json as input and converts it to geojson. The index in the attribute table
        of the latitude and longitude fields needs to be provided.
        Currently for POINT data, but can be changed to include other types.

        :param data_json: Newline json which contains the spatial data
        :type data_json: Newline json

        :param lat_index: Index field which contains the latitude
        :type lat_index: int

        :param lon_index: Index field which contains the longitude
        :type lon_index: int

        :returns: Geojson version of the newline json data
        :rtype: geojson
        """
        list_elements = []
        for element in data_json:
            lat = 0
            lon = 0
            list_properties = {}
            i = 0
            for field in element:
                if i == lat_index:
                    # Latitude field
                    lat = element[field]
                elif i == lon_index:
                    # Longitude field
                    lon = element[field]
                else:
                    # Other fields (e.g. temperature)
                    list_properties[field] = element[field]

                i = i + 1

            new_element = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lat, lon]
                },
                'properties': list_properties
            }
            list_elements.append(new_element)

        data_geojson = {
            'type': 'FeatureCollection',
            'features': list_elements
        }

        return data_geojson

    @staticmethod
    def list_bigquery_tables(bq_dataset):
        """Lists all BigQuery tables in the given dataset.

        :param bq_dataset: BigQuery dataset
        :type bq_dataset: str

        :returns: List of tables found in the provided dataset
        :rtype: list
        """

        client = bigquery.Client()
        tables = client.list_tables(bq_dataset)

        return tables

    @staticmethod
    def get_data(link):
        """Performs a get request on the provided link. Several checks and try/exceptoions is done
        to avoid unexpected errors and to keep the code running.

        :param link: Address for the request
        :type link: str

        :returns: True if the request succeeded, False if it failed
        :rtype: boolean

        :returns: Received request contents
        :rtype: str
        """
        try:
            # Performs the requests and gets the contents from NASA POWER
            result = requests.get(link, timeout=Default.DEFAULT_TIMEOUT)
            result_status = result.status_code
            if result_status == 200:
                # Contents received from NASA POWER
                content = result.content
            else:
                # Errorenous response from NASA POWER
                return False, ''
        except Timeout:
            return False, ''
        except Exception as e:
            print(str(e))
            return False, ''

        return True, content

    @staticmethod
    def load_csv_into_bucket(bucket_name, contents, csv_name):
        """Uploads string into a CSV file into a bucket.

        :param bucket_name: Google cloud storage bucket name
        :type bucket_name: str

        :param contents: Contents to be written to the CSV file
        :type contents: str

        :param csv_name: Name to give to the CSV file
        :type csv_name: str

        :returns: True if the CSV data has been stored in the bucket successful, false if it failed
        :rtype: boolean
        """
        try:
            # Writes the CSV file to a bucket
            client = storage.Client(project=Default.PROJECT_ID)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(csv_name)
            blob.upload_from_string(contents)
        except Exception as e:
            return False

        return True

    @staticmethod
    def load_csv_into_bigquery(upload_uri, bq_table_uri, schema, append, skip_leading_rows=1):
        """Loads a CSV file stored in a bucket into BigQuery.

        :param upload_uri: Google cloud storage directory (e.g. gs://bucket/folder/file)
        :type upload_uri: uri

        :param bq_table_uri: Google cloud storage directory (e.g. gs://bucket/folder/file)
        :type bq_table_uri: uri

        :param schema: Fields structure of the BigQuery table
        :type schema: list

        :param skip_leading_rows: Number of rows to skip at the start of the file
        :type skip_leading_rows: int

        :param append: True if the new rows should be appended to the table, False for when a new table will be created
        :type append: boolean

        :returns: True if the CSV data has been stored in BigQuery successful, false if it failed
        :rtype: boolean
        """
        client_bq = bigquery.Client()

        if not append:
            # Only if it's a new table to be created
            # This will be skipped for appending rows
            try:
                table = bigquery.Table(bq_table_uri, schema=schema)
                client_bq.create_table(table)
            except Exception as e:
                print(e)
                return False

        try:
            if append:
                # Append rows to an existing table
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    skip_leading_rows=skip_leading_rows,
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
            else:
                # Create a new table
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    skip_leading_rows=skip_leading_rows,
                    source_format=bigquery.SourceFormat.CSV
                )

            load_job = client_bq.load_table_from_uri(
                upload_uri, bq_table_uri, job_config=job_config
            )
            load_job.result()
        except Exception as e:
            # Loading from csv file into bigquery failed
            # The newly created bigquery table will be deleted
            client_bq.delete_table(bq_table_uri)
            print(e)
            return False

        # Returns True if loading the data into BigQuery succeeded
        return True

    @staticmethod
    def append_to_bigquery_table(target_table_id, temp_table_id, list_field_names):
        """Append new columns to an existing table in BigQuery.

        :param target_table_id: ID for the target table in BigQuery: e.g. your-project.your_dataset.your_table_name
        :type target_table_id: str

        :param temp_table_id: ID for the temporary table in BigQuery: e.g. your-project.your_dataset.your_table_name
        :type temp_table_id: str

        :param list_field_names: Names for new field(s)
        :type list_field_names: list

        :return: True if succeeded, False if not
        :type: boolean
        """
        # Construct a BigQuery client object.
        client = bigquery.Client()

        try:
            # Gets the table. table_id == "your-project.your_dataset.your_table_name"
            table = client.get_table(target_table_id)  # Make an API request.

            # Updates the table schema
            original_schema = table.schema
            new_schema = original_schema[:]  # Copy of the original schema
            for field in list_field_names:
                bq_field = bigquery.SchemaField(field, 'FLOAT', mode='NULLABLE')

                if bq_field not in new_schema:
                    # Only adds the field if it does not exist
                    # If the field does exist, the contents will be overwritten
                    new_schema.append(bq_field)

            # Adds the new fields
            table.schema = new_schema
            client.update_table(table, ["schema"])
        except Exception as e:
            # Could not update the schema of the table
            return False

        try:
            # Performs a query to add the data to the new field
            for field in list_field_names:
                query_job = client.query(
                    "UPDATE " + target_table_id +
                    " a SET a." + field + " = b." +
                    field + " FROM " + temp_table_id +
                    " b WHERE a." + Default.LAT_FIELD + " = b."
                    + Default.LAT_FIELD +
                    " AND a." + Default.LON_FIELD +
                    " = b." + Default.LON_FIELD
                )
                query_job.result()
        except Exception as e:
            # Could not perform the query on the table
            return False

        return True

    @staticmethod
    def parse_csv_table(upload_uri):
        """Parses a provided CSV file. The parser automatically determines the type of each field based on the
        contents of the field. Unwanted character will also be removed from contents (e.g. 20 000 to 20000).
        Field names are also updated to agree with BigQuery limits (e.g. 123 Field will become _123_Field).

        :param upload_uri: URI for the CSV stored in a GCP bucket
        :type upload_uri: str

        :returns: URI list of the created CSV files in the temporary bucket
        :rtype: list

        :returns: BigQuery Schema
        :rtype: list
        """
        schema = []  # BigQuery schema
        column_types = []  # Data type for each column (e.g FLOAT, DATE, etc.)
        all_temp_uri = []  # A list of the of all the temporary created CSVs in a GCP bucket
        updated_column_names = []
        chunk_num = 1  # Keeps count of the current row chunk being processed

        # URIs/Paths
        csv_name = os.path.basename(upload_uri)
        temp_name = 'temp_' + csv_name.replace('.csv', '_' + str(chunk_num) + '.csv')
        temp_uri = 'gs://' + Default.BUCKET_TEMP + '/' + temp_name

        now = datetime.now()
        print('[' + str(now) + '] ' + 'Opening file')

        # Opens the file stored in a GCP bucket. gcsfs is required for this
        bucket_csv = gcsfs.GCSFileSystem(project=Default.PROJECT_ID)
        rows_remain = True
        skip_rows = 0
        # This will loop will keep going until all rows has been processed
        # Done in chunks (e.g. 10 million at a time)
        while rows_remain:
            with bucket_csv.open(upload_uri, 'r', errors='ignore') as f:
                now = datetime.now()
                print('[' + str(now) + '] ' + 'Reading into pandas. Chunk number: ' + str(chunk_num))

                if chunk_num == 1:
                    csv_contents = pd.read_csv(f, skiprows=skip_rows, nrows=Default.CSV_ROW_READ_LIMIT, low_memory=False)
                else:
                    csv_contents = pd.read_csv(f, skiprows=skip_rows + 1, nrows=Default.CSV_ROW_READ_LIMIT, low_memory=False, names=updated_column_names)

                csv_row_count = len(csv_contents)

                print("Chunk row count: " + str(csv_row_count))

                if chunk_num == 1:
                    # Columns names are only initialized once
                    now = datetime.now()
                    print('[' + str(now) + '] ' + 'Starting parsing of the columns')

                    column_names = csv_contents.columns
                    updated_column_names = Utilities.parse_column_names(column_names)

                    i = 0
                    columns_parameter = {}
                    for old_name in column_names:
                        # Updates all column names with the updated column names
                        new_name = updated_column_names[i]
                        columns_parameter[old_name] = new_name
                        column_types.append(None)
                        i = i + 1
                    csv_contents = csv_contents.rename(columns_parameter, axis='columns')

                now = datetime.now()
                print('[' + str(now) + '] ' + 'Parsing rows')

                cur_row = 0
                while cur_row < csv_row_count:
                    cur_column_index = 0
                    for column_name in updated_column_names:
                        cur_value = csv_contents.at[cur_row, column_name]

                        if cur_value == '' or cur_value is None:
                            # If the table cell is empty/None it will remain as is
                            cur_column_index = cur_column_index + 1
                            continue
                        elif column_types[cur_column_index] == 'STRING':
                            # If the column type is already set to string, no changes are required
                            cur_column_index = cur_column_index + 1
                            continue
                        else:
                            # Check what type the value is
                            new_value = Utilities.remove_unwanted_chars(cur_value)

                            if Utilities.is_float(new_value):
                                # Value can be stored as float
                                if math.isnan(new_value):
                                    # This is an empty value, the value and field type will be left as is
                                    cur_column_index = cur_column_index + 1
                                    continue

                                column_types[cur_column_index] = 'FLOAT'
                                csv_contents.at[cur_row, column_name] = new_value
                            elif Utilities.is_date(new_value):
                                new_value_date = Utilities.parse_date(new_value)
                                if new_value_date is None:
                                    # The date structure could not be determined
                                    # Set to type string, and leave the value as is
                                    column_types[cur_column_index] = 'STRING'
                                else:
                                    # The date structure could be determined
                                    # Change the date to the parsed date value
                                    column_types[cur_column_index] = 'DATE'
                                    csv_contents.at[cur_row, column_name] = new_value_date
                            else:
                                # All other cases will be set to string
                                column_types[cur_column_index] = 'STRING'
                                csv_contents.at[cur_row, column_name] = new_value

                            cur_column_index = cur_column_index + 1

                    cur_row = cur_row + 1

                # Writes the results to a CSV file in a GCP bucket
                csv_contents.to_csv(temp_uri, index=False)
                all_temp_uri.append(temp_uri)

                f.close()

            # Checks if more rows remain
            if csv_row_count < Default.CSV_ROW_READ_LIMIT:
                # Processing done
                rows_remain = False
            else:
                # More rows to process
                skip_rows = skip_rows + Default.CSV_ROW_READ_LIMIT
                chunk_num = chunk_num + 1
                temp_name = 'temp_' + csv_name.replace('.csv', '_' + str(chunk_num) + '.csv')
                temp_uri = 'gs://' + Default.BUCKET_TRIGGER + '/' + temp_name

                now = datetime.now()
                print('[' + str(now) + '] ' + 'NEXT')

        # Creates the BigQuery table schema
        j = 0
        for column_name in updated_column_names:
            data_type = column_types[j]
            if data_type is None:
                schema.append(
                    bigquery.SchemaField(column_name, 'STRING', mode='NULLABLE')
                )
            else:
                schema.append(
                    bigquery.SchemaField(column_name, data_type, mode='NULLABLE')
                )
            j = j + 1

        now = datetime.now()
        print('[' + str(now) + '] ' + 'DONE')

        return all_temp_uri, schema


def create_bigquery_json_files():
    for bq_dataset in Default.LIST_BQ_DATASETS:
        list_bq_tables = Utilities.list_bigquery_tables(bq_dataset)
        for bq_table in list_bq_tables:
            table_name = bq_table.table_id
            table_geojson = Utilities.table_to_geojson(bq_dataset, table_name)

            Utilities.write_to_log('log.txt', 'BQ TABLE: ' + table_name)

            client = storage.Client(project=Default.PROJECT_ID)
            bucket = client.get_bucket(Default.BUCKET_TEMP)

            blob = bucket.blob(table_name + '.geojson')
            # upload the blob
            blob.upload_from_string(
                data=json.dumps(table_geojson),
                content_type='application/json'
            )


def data_added_to_bucket():
    """Trigger function to call when data has been uploaded to a bucket.
    Deploy this function to a Google cloud storage bucket
    """
    client = storage.Client(project=Default.PROJECT_ID)
    bucket = client.get_bucket(Default.BUCKET_TRIGGER)
    bucket_temp = client.get_bucket(Default.BUCKET_TEMP)
    client_bq = bigquery.Client()

    list_csv, list_excel, list_archives, list_raster, list_vector = Utilities.list_bucket_data(
        project=Default.PROJECT_ID,
        bucket=Default.BUCKET_TRIGGER
    )

    for csv_file in list_csv:
        now = datetime.now()
        print('[' + str(now) + '] ' + "CSV: " + str(csv_file))

        output_table_name = os.path.basename(csv_file).replace('.csv', '')
        upload_uri = 'gs://' + Default.BUCKET_TRIGGER + '/' + csv_file
        bq_table_uri = Default.PROJECT_ID + '.' + Default.BIGQUERY_DATASET_BUCKET + '.' + output_table_name

        # Quick test to see of the BigQuery table exists
        try:
            table_bq = client_bq.get_table(bq_table_uri)
            table_exist = True
        except NotFound:
            table_exist = False

        if table_exist:
            # Skip file if the table exists
            now = datetime.now()
            print('[' + str(now) + '] ' + "Table exists")
            continue

        now = datetime.now()
        print('[' + str(now) + '] ' + "PARSING")

        all_temp_uri, schema = Utilities.parse_csv_table(upload_uri)

        now = datetime.now()
        print('[' + str(now) + '] ' + "LOADING CSV INTO BIGQUERY")

        csv_num = 1
        for temp_uri in all_temp_uri:
            if csv_num == 1:
                # First CSV chunk, create new table

                now = datetime.now()
                print('[' + str(now) + '] ' + "NEW TABLE")

                success = Utilities.load_csv_into_bigquery(temp_uri, bq_table_uri, schema, False, 1)
            else:
                now = datetime.now()
                print('[' + str(now) + '] ' + "APPEND TO TABLE")

                success = Utilities.load_csv_into_bigquery(temp_uri, bq_table_uri, schema, True, 1)

            #bucket_temp.delete_blob(os.path.basename(temp_uri))
            csv_num = csv_num + 1

        now = datetime.now()
        print('[' + str(now) + '] ' + "DONE")

        # if success:
        #     Utilities.move_data(Default.BUCKET_TRIGGER, Default.BUCKET_DONE, csv_file, csv_file)
        # else:
        #     Utilities.move_data(Default.BUCKET_TRIGGER, Default.BUCKET_FAILED, csv_file, csv_file)

    for archive_file in list_archives:
        now = datetime.now()
        print('[' + str(now) + '] ' + "Unzipping: " + archive_file)

        success = Utilities.unzip(archive_file)
        # if success:
        #     bucket.delete_blob(archive_file)
        # else:
        #     Utilities.move_data(Default.BUCKET_TRIGGER, Default.BUCKET_FAILED, archive_file, archive_file)

    for shp_file in list_vector:
        now = datetime.now()
        print('[' + str(now) + '] ' + "Shapefile: " + shp_file)

        table_name = os.path.basename(shp_file).replace('.shp', '')
        bq_table_uri = Default.PROJECT_ID + '.' + Default.BIGQUERY_DATASET_BUCKET + '.' + table_name
        # Quick test to see of the BigQuery table exists
        try:
            table_bq = client_bq.get_table(bq_table_uri)
            table_exist = True
        except NotFound:
            table_exist = False

        if table_exist:
            # Skip file if the table exists
            now = datetime.now()
            print('[' + str(now) + '] ' + "Table exists")
            continue

        # Required files
        pos_index_file = shp_file.replace('.shp', '.shx')
        dbf_file = shp_file.replace('.shp', '.dbf')
        prj_file = shp_file.replace('.shp', '.prj')

        # Other files
        sbn_file = shp_file.replace('.shp', '.sbn')
        sbx_file = shp_file.replace('.shp', '.sbx')
        fbn_file = shp_file.replace('.shp', '.fbn')
        fbx_file = shp_file.replace('.shp', '.fbx')
        ain_file = shp_file.replace('.shp', '.ain')
        aih_file = shp_file.replace('.shp', '.aih')
        ixs_file = shp_file.replace('.shp', '.ixs')
        mxs_file = shp_file.replace('.shp', '.mxs')
        atx_file = shp_file.replace('.shp', '.atx')
        xml_file = shp_file + '.xml'
        cpg_file = shp_file.replace('.shp', '.cpg')
        qix_file = shp_file.replace('.shp', '.qix')

        sleep_cnt = 0
        dbf_uploaded = False
        while not dbf_uploaded:
            if storage.Blob(bucket=bucket, name=dbf_file).exists(client):
                dbf_uploaded = True
            else:
                sleep_cnt = sleep_cnt + 1
                if sleep_cnt >= Default.SLEEP_COUNT:
                    # Maximum sleeps reached, break out of loop
                    break

                time.sleep(Default.SLEEP_LENGTH)

        sleep_cnt = 0
        pos_index_uploaded = False
        while not pos_index_uploaded:
            if storage.Blob(bucket=bucket, name=pos_index_file).exists(client):
                pos_index_uploaded = True
            else:
                sleep_cnt = sleep_cnt + 1
                if sleep_cnt >= Default.SLEEP_COUNT:
                    # Maximum sleeps reached, break out of loop
                    break

                time.sleep(Default.SLEEP_LENGTH)

        sleep_cnt = 0
        prj_uploaded = False
        while not prj_uploaded:
            if storage.Blob(bucket=bucket, name=prj_file).exists(client):
                prj_uploaded = True
            else:
                sleep_cnt = sleep_cnt + 1
                if sleep_cnt >= Default.SLEEP_COUNT:
                    # Maximum sleeps reached, break out of loop
                    break

                time.sleep(Default.SLEEP_LENGTH)

        if dbf_uploaded and pos_index_uploaded and prj_uploaded:
            now = datetime.now()
            print('[' + str(now) + '] ' + 'All required files received, continue.')
            Utilities.shp_to_geojson(shp_file)
        else:
            now = datetime.now()
            print('[' + str(now) + '] ' + "\t\tShapefile file missing.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    data_added_to_bucket()
