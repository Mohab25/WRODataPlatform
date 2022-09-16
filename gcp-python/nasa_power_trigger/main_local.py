import sys

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import io
import pandas as pd
import time
import datetime
from datetime import date, timedelta
import logging

import requests
from requests import get, post
from requests.exceptions import Timeout


class Default:
    # For testing
    #BUCKET_TEMP = 'wrc_wro_temp2'
    #PROJECT_ID = 'thermal-glazing-350010'

    # Projects
    PROJECT_ID = 'wrc-wro'

    # Buckets
    BUCKET_TRIGGER = 'wro-trigger-test'
    BUCKET_DONE = 'wro-done'
    BUCKET_FAILED = 'wro-failed'
    BUCKET_TEMP = 'wrc_wro_temp'

    # Regions (e.g. us, us-east1, etc.)
    REGION = 'us-east1'

    # BigQuery
    BIGQUERY_DATASET_DAILY = 'NASA_POWER_climate'
    BIGQUERY_DATASET_MONTHLY = 'NASA_POWER_weather_daily'
    BIGQUERY_DATASET_CLIMATOLOGY = 'NASA_POWER_weather_monthly'
    LIST_BQ_DATASETS = [
        BIGQUERY_DATASET_DAILY,
        BIGQUERY_DATASET_MONTHLY,
        BIGQUERY_DATASET_CLIMATOLOGY
    ]

    # Request parameters
    NASA_POWER_URL = 'https://power.larc.nasa.gov/api/temporal'
    NASA_POWER_FORMAT = 'CSV'
    # AG: Agroclimatology, RE: Renewable energy, or SB: Sustainable buildings
    NASA_POWER_COMMUNITY = ['RE', 'SB', 'AG']
    DEFAULT_TIMEOUT = 60  # Timeout in seconds for requests
    DEFAULT_SLEEP = 30  # Sleep time in seconds for when a request fails
    MAX_REQUESTS = 10  # Number of attempts a request will be done if it fails
    SKIP_LEADING_ROWS = 10  # Rows to skip at the start of the received request content (e.g. headers)
    SKIP_TRAILING_ROWS = 1  # Rows to skip at the e of the reveived request content (e.g. headers)

    # Temporal types and settings
    DAILY = 'daily'
    MONTHLY = 'monthly'
    CLIMATOLOGY = 'climatology'
    NASA_POWER_TEMPORAL_AVE = [
        DAILY,
        MONTHLY,
        CLIMATOLOGY
    ]
    NUMBER_OF_PREVIOUS_DAY = 1  # This will be the number of days prior to the current/today date
    SKIP_CLIMATOLOGY = True  # These datasets will likely not change
    # 'D' for daily downloads, 'M' for all days on a monthly basis
    # 'M' will be useful for bulk downloads, but set to 'D' for the Cloud trigger as it should only
    # download on a daily basis.
    DAILY_DATES_FREQUENCY = 'D'

    LAT_FIELD = 'LAT'
    LON_FIELD = 'LON'

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
        SKY_SURFACE_SW_IRRADIANCE,  # include
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
        DEW_FROST,  # include
        WET_TEMP,  # include
        EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        TEMP_MAX,  # include
        TEMP_MIN,  # include
        # SPECIFIC_HUMIDITY,
        RELATIVE_HUMIDITY,  # include
        PRECIPITATION,  # include
        # SURFACE_PRESSURE,
        WINDSPEED_10M,  # include
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
        SURFACE_SOIL_WETNESS,  # include
        ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]

    LIST_NASA_POWER_DATASETS_RE_MONTHLY = [
        SKY_SURFACE_SW_IRRADIANCE,  # include
        # CLEAR_SKY_SURFACE_SW_IRRADIANCE,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE,
        # SKY_INSOLATION_CLEARNESS_INDEX,
        # CLEAR_SKY_INSOLATION_CLEARNESS_INDEX,
        # SKY_SURFACE_ALBEDO,
        # TOA_SW_IRRADIANCE,
        CLOUD_AMOUNT,  # include
        # SKY_SURFACE_PS_ACTIVE_RADIATION,
        # CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION,
        # SKY_SURFACE_UVA_IRRADIANCE,
        # SKY_SURFACE_UVB_IRRADIANCE,
        # SKY_SURFACE_UV_INDEX,
        WINDSPEED_2M,  # include
        TEMP,  # include
        DEW_FROST,  # include
        WET_TEMP,  # include
        EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        TEMP_MAX,  # include
        TEMP_MIN,  # include
        # SKY_SURFACE_LW_IRRADIANCE,
        SPECIFIC_HUMIDITY,  # include
        RELATIVE_HUMIDITY,  # include
        PRECIPITATION,  # include
        PRECIPITATION_SUM,  # include
        # SURFACE_PRESSURE,
        WINDSPEED_10M,  # include
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
        SURFACE_SOIL_WETNESS,  # include
        ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]

    LIST_NASA_POWER_DATASETS_RE_CLIMATOLOGY = [
        # SKY_SURFACE_SW_IRRADIANCE,  # include
        # SKY_SURFACE_SW_IRRADIANCE_GMT,
        # CLEAR_SKY_SURFACE_SW_IRRADIANCE,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE,
        # SKY_INSOLATION_CLEARNESS_INDEX,
        # SKY_SURFACE_ALBEDO,
        # TOA_SW_IRRADIANCE,
        CLOUD_AMOUNT,  # include
        SKY_SURFACE_PS_ACTIVE_RADIATION,  # include
        CLEAR_SKY_SURFACE_PS_ACTIVE_RADIATION,  # include
        # SKY_SURFACE_UVA_IRRADIANCE,
        # SKY_SURFACE_UVB_IRRADIANCE,
        # SKY_SURFACE_UV_INDEX,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MAX,
        # SKY_SURFACE_SW_DIRECT_NORMAL_IRRADIANCE_MIN,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MAX,
        # SKY_SURFACE_SW_DIFFUSE_IRRADIANCE_MIN,
        # MIDDAY_INSOLATION_INCIDENT,
        WINDSPEED_2M,  # include
        TEMP,  # include
        DEW_FROST,  # include
        WET_TEMP,  # include
        EARTH_SKIN_TEMP,  # include
        # TEMP_RANGE,
        TEMP_MAX,  # include
        TEMP_MIN,  # include
        # EARTH_SKIN_TEMP_MAX,
        # EARTH_SKIN_TEMP_MIN,
        # SKY_SURFACE_LW_IRRADIANCE,
        SPECIFIC_HUMIDITY,  # include
        RELATIVE_HUMIDITY,  # include
        PRECIPITATION,  # include
        PRECIPITATION_SUM,  # include
        # SURFACE_PRESSURE,
        WINDSPEED_10M,  # include
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
        SURFACE_SOIL_WETNESS,  # include
        ROOT_SOIL_WETNESS,  # include
        PROFILE_SOIL_MOISTURE  # include
    ]


class Utilities:
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
                #s_date = date(start_year, start_month, start_day) + timedelta(days=1)
                s_date = date(start_year, start_month, start_day)
                e_date = date(end_year, end_month, end_day)
                delta = e_date - s_date

                list_dates = []
                for value in range(delta.days + 1):
                    day = s_date + timedelta(days=value)
                    day_val = day.day
                    month_val = day.month
                    year_val = day.year

                    if month_val < 10:
                        s_month = '0' + str(month_val)
                    else:
                        s_month = str(month_val)
                    if day_val < 10:
                        s_day = '0' + str(day_val)
                    else:
                        s_day = str(day_val)
                    # Downloads on a daily basis. Use for Cloud function
                    daily_date = '{}{}{}'.format(year_val, s_month, s_day)

                    list_dates.append(
                        {
                            'start_date': daily_date,
                            'end_date': daily_date
                        }
                    )
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
                list_days_temp = pd.date_range(pd_start_date, pd_end_date, freq=frequency).strftime(date_format).tolist()

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
    def load_csv_into_bigquery(upload_uri, bq_table_uri, schema, skip_leading_rows=1):
        """Loads a CSV file stored in a bucket into BigQuery.

        :param upload_uri: Google cloud storage directory (e.g. gs://bucket/folder/file)
        :type upload_uri: uri

        :param bq_table_uri: Google cloud storage directory (e.g. gs://bucket/folder/file)
        :type bq_table_uri: uri

        :param schema: Fields structure of the BigQuery table
        :type schema: list

        :param skip_leading_rows: Number of rows to skip at the start of the file
        :type skip_leading_rows: int

        :returns: True if the CSV data has been stored in BigQuery successful, false if it failed
        :rtype: boolean
        """
        client_bq = bigquery.Client()
        try:
            table = bigquery.Table(bq_table_uri, schema=schema)
            client_bq.create_table(table)
        except Exception as e:
            return False

        try:
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


def download_weather_data_into_bigquery():
    """Downloads data from NASA POWER and stores it in BigQuery
    """
    print("DOWNLOADER STARTED")
    skip_leading_rows = Default.SKIP_LEADING_ROWS  # Number of rows which will be skipped at the start of the file
    skip_trailing_rows = Default.SKIP_TRAILING_ROWS  # Number of rows at the enc of the file which will be skipped

    # Google cloud platform
    client = storage.Client(project=Default.PROJECT_ID)
    bucket = client.bucket(Default.BUCKET_TEMP)
    client_bq = bigquery.Client()

    # Community: Renewable energy, sustainable buildings, or climatology
    for community in Default.NASA_POWER_COMMUNITY:
        print("COMMUNITY: " + community)
        # Period/temporal: Daily, monthly, or climatology
        for period in Default.NASA_POWER_TEMPORAL_AVE:
            print("PERIOD: " + period)
            if period == Default.MONTHLY or period == Default.CLIMATOLOGY:
                # Monthly/Annual and climatology will only be downloaded at the end of January
                # NASA POWER only updates monthly/annual data after a year's end
                today_date = datetime.datetime.today()
                cur_m = int(today_date.strftime("%m"))
                cur_d = int(today_date.strftime("%d"))

                if cur_m == 1 and cur_d == 31:
                    # Will perform climatology and monthly/annual downloads
                    # Only happens on the 31st of January
                    pass
                else:
                    # Climatology and monthly/annual will be skipped
                    continue

            # BigQery dataset (e.g. weather_daily, weather_monthly, or weather_climatology)
            bq_dataset = Utilities.get_bq_dataset(period)

            # Indices for columns in request contents from NASA POWER
            lat_index, lon_index, value_index = Utilities.get_request_content_indices(period)

            # Gets the list of datasets to loop through
            # Based on community and period
            list_datasets = Utilities.get_dataset_list(community, period)
            if community == 'AG':
                # The agriculture community data can also be obtained using the renewable community.
                # The advantage of this is data the RE community responds with lat, lon, year, month, day.
                # This agrees with other data field schemas. AG responds with lat, lon, year, day-in-year
                community = 'RE'

            if len(list_datasets) == 0:
                # List datasets could not be determined, skip
                continue

            # Performs requests on each dataset
            for dataset in list_datasets:
                # This variable is used if the dataset needs to be skipped, likely when the data is
                # not available on NASA POWER
                flag_break = False

                dataset_key = dataset['key']
                dataset_name = dataset['name']
                # dataset_description = dataset['description']

                print("DATASET: " + dataset_key)

                # Table name which will be used for the BigQuery table
                # and temporary CSV file
                if period == Default.DAILY:
                    # Daily has years added because of the 10k column limit of BigQuery tables
                    table_name = '{}_{}_{}_{}_{}'.format(
                        dataset_name,
                        community,
                        period,
                        '2006',
                        'present'
                        # start_y,
                        # end_y
                    )
                else:
                    # Climatology and monthly will have much less than 10k columns
                    table_name = '{}_{}_{}'.format(
                        dataset_name,
                        community,
                        period
                    )
                file_name = table_name + '.csv'

                # Quick test to see of the BigQuery table exists
                target_table_id = Default.PROJECT_ID + '.' + bq_dataset + '.' + table_name
                try:
                    table_bq = client_bq.get_table(target_table_id)
                    table_exist = True

                    if period == Default.DAILY:
                        # Gets the last date from the last field in the existing table
                        current_schema = table_bq.schema
                        last_field = current_schema[len(current_schema) - 1:]  # Last field in the table
                        last_field = last_field[0]

                        # Gets the date from the field name
                        last_field_name = last_field.name
                        last_date = last_field_name.split('_')
                        last_date = last_date[len(last_date) - 1:][0]

                        # Start and end dates
                        today_date = datetime.datetime.today()
                        # Weather data will always be downloaded for the previous day, to be sure the data is available
                        previous_date = today_date - datetime.timedelta(days=Default.NUMBER_OF_PREVIOUS_DAY)
                        start_y = int(last_date[:4])
                        end_y = int(previous_date.strftime("%Y"))
                        start_m = int(last_date[4:6])
                        end_m = int(previous_date.strftime("%m"))
                        start_d = int(last_date[6:])
                        end_d = int(previous_date.strftime("%d"))

                        # start_y = 2022
                        # end_y = 2022
                        # start_m = 7
                        # end_m = 7
                        # start_d = 1
                        # end_d = 31

                    else:
                        # Start and end dates for monthly/annual and climatology
                        today_date = datetime.datetime.today()

                        start_y = int(today_date.strftime("%Y")) - 1
                        end_y = int(today_date.strftime("%Y")) - 1
                        start_m = 1
                        end_m = 12
                        start_d = 1
                        end_d = 31
                except NotFound:
                    table_exist = False
                    # Start and end dates
                    today_date = datetime.datetime.today()
                    # Weather data will always be downloaded for the previous day, to be sure the data is available
                    previous_date = today_date - datetime.timedelta(days=Default.NUMBER_OF_PREVIOUS_DAY)
                    start_y = int(previous_date.strftime("%Y"))
                    end_y = int(previous_date.strftime("%Y"))
                    start_m = int(previous_date.strftime("%m"))
                    end_m = int(previous_date.strftime("%m"))
                    start_d = int(previous_date.strftime("%d"))
                    end_d = int(previous_date.strftime("%d"))

                # Gets a list of dates based on the start and end date
                date_required, list_dates = Utilities.get_date_list(
                    period,
                    start_y,
                    end_y,
                    start_m,
                    end_m,
                    start_d,
                    end_d
                )

                for date_ in list_dates:
                    print("DATE: " + str(date_))
                    list_field_names = []

                    with io.StringIO() as file_mem:
                        # Sets the dates
                        if date_required:
                            # Daily and yearly
                            start_date = date_['start_date']
                            end_date = date_['end_date']
                        else:
                            # Climatology period type does not require dates
                            start_date = ''
                            end_date = ''

                        # Lat long will be added only once to a table
                        if not table_exist:
                            list_field_names.append(Default.LAT_FIELD)
                            list_field_names.append(Default.LON_FIELD)

                        # Adds fields to the list for the next dataset/date
                        list_field_names = Utilities.append_field_names(
                            list_field_names,
                            period,
                            dataset_name,
                            start_date,
                            end_date
                        )

                        for extent in Default.SA_GRID_EXTENTS:
                            print("EXTENT: " + str(extent))

                            # Extent of the tile
                            lat_min = extent["lat_min"]
                            lat_max = extent["lat_max"]
                            lon_min = extent["lon_min"]
                            lon_max = extent["lon_max"]

                            # Request link
                            if date_required:
                                # Dates required for daily and monthly
                                link = '{}/{}/regional?parameters={}&start={}&end={}&community={}&format={}&latitude-min={}&latitude-max={}&longitude-min={}&longitude-max={}'.format(
                                    Default.NASA_POWER_URL,
                                    period,
                                    dataset_key,
                                    start_date,
                                    end_date,
                                    community,
                                    Default.NASA_POWER_FORMAT,
                                    lat_min,
                                    lat_max,
                                    lon_min,
                                    lon_max
                                )
                            else:
                                # No date for climatology
                                link = '{}/{}/regional?parameters={}&community={}&format={}&latitude-min={}&latitude-max={}&longitude-min={}&longitude-max={}'.format(
                                    Default.NASA_POWER_URL,
                                    period,
                                    dataset_key,
                                    community,
                                    Default.NASA_POWER_FORMAT,
                                    lat_min,
                                    lat_max,
                                    lon_min,
                                    lon_max
                                )

                            success = False
                            max_requests = False
                            request_count = 0
                            # Performs the get request
                            while not success and not max_requests:
                                # NASA POWER will on occasion respond with errorenous content,
                                # or data could not be retrieved on their side. This is not a common issue, and
                                # performing another request should solve the problem. This will allow downloading
                                # to continue with no interruption.
                                success, content = Utilities.get_data(link)
                                request_count = request_count + 1

                                if not success:
                                    # If the request did not succeed, the script will wait for a time period
                                    # as the cause is likely from the minor
                                    time.sleep(Default.DEFAULT_SLEEP)
                                if request_count >= Default.MAX_REQUESTS:
                                    # This is done to avoid a permanent loop (e.g. NASA POWER is down)
                                    max_requests = True

                            if max_requests:
                                # The data could not be retrieved
                                # Print errors to the text file for later use or reruns
                                # Closes the memory file
                                file_mem.close()
                                # Skip this date range and go to the next date range
                                break

                            # Newline not stored as '\n' character, so use r'\n'
                            split_content = str(content).split(r'\n')

                            # Removes unwanted lines at the start and end of the data
                            split_content = split_content[skip_leading_rows:(len(split_content) - skip_trailing_rows)]

                            if period == Default.DAILY:
                                split_content = Utilities.transform_daily_data(split_content)

                            for line in split_content:
                                # Latitude, longitude and value
                                line = line.replace('\n', '')
                                list_columns = line.split(',')

                                # A check required only for climatology
                                if period == Default.CLIMATOLOGY:
                                    # This test is only required for climatology.
                                    # Climatology has a last row which equals '\\r'
                                    # This row (or any other) will be ignored/skipped
                                    if len(list_columns) < 15:
                                        continue

                                if period == Default.DAILY:
                                    # Daily. First does a test to see if the data is available for the date
                                    # If not, the dataset is skipped as it does not have the data available for
                                    # those dates. The nodata value for NASA POWER is -999.0
                                    value_test = float(list_columns[2])
                                    if value_test == -999:
                                        print('Date not available for the dataset, go to the next dataset')
                                        flag_break = True
                                        break
                                    # Transformtion has been done for daily
                                    write_to_mem = line

                                elif period == Default.MONTHLY:
                                    # Lat, lon, all months, and average monthly
                                    write_to_mem = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(
                                        list_columns[lat_index],
                                        list_columns[lon_index],
                                        list_columns[value_index[0]],
                                        list_columns[value_index[1]],
                                        list_columns[value_index[2]],
                                        list_columns[value_index[3]],
                                        list_columns[value_index[4]],
                                        list_columns[value_index[5]],
                                        list_columns[value_index[6]],
                                        list_columns[value_index[7]],
                                        list_columns[value_index[8]],
                                        list_columns[value_index[9]],
                                        list_columns[value_index[10]],
                                        list_columns[value_index[11]],
                                        list_columns[value_index[12]]
                                    )
                                else:
                                    # Climatology
                                    write_to_mem = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(
                                        list_columns[lat_index],
                                        list_columns[lon_index],
                                        list_columns[value_index[0]],
                                        list_columns[value_index[1]],
                                        list_columns[value_index[2]],
                                        list_columns[value_index[3]],
                                        list_columns[value_index[4]],
                                        list_columns[value_index[5]],
                                        list_columns[value_index[6]],
                                        list_columns[value_index[7]],
                                        list_columns[value_index[8]],
                                        list_columns[value_index[9]],
                                        list_columns[value_index[10]],
                                        list_columns[value_index[11]],
                                        list_columns[value_index[12]]
                                    )

                                file_mem.write(write_to_mem)
                                file_mem.write('\n')

                            if flag_break:
                                # Dataset date has no available data
                                break

                        if flag_break:
                            # Dataset date has no available data
                            file_mem.close()
                            break

                        print("CREATE CSV")
                        data_in_mem = file_mem.getvalue()
                        data_in_mem = data_in_mem[:len(data_in_mem) - 1]
                        csv_upload_success = Utilities.load_csv_into_bucket(
                            Default.BUCKET_TEMP,
                            data_in_mem,
                            file_name)

                        if not csv_upload_success:
                            # Closes the memory file
                            file_mem.close()
                            # Skip this date range and go to the next date range
                            break

                        # Temporary CSV file URI and BigQuery table URI
                        upload_uri = 'gs://' + Default.BUCKET_TEMP + '/' + file_name
                        bq_table_uri = Default.PROJECT_ID + '.' + bq_dataset + '.' + table_name

                        bq_upload_success = False
                        if not table_exist:
                            # Create new table as it does not exist
                            print("CREATE TABLE: " + str(table_name))

                            schema = []
                            for field in list_field_names:
                                bq_field = bigquery.SchemaField(field, 'FLOAT', mode='NULLABLE')
                                schema.append(bq_field)

                            bq_upload_success = Utilities.load_csv_into_bigquery(
                                upload_uri,
                                bq_table_uri,
                                schema,
                                skip_leading_rows=0)
                        else:
                            # Table exists, content will be appended as new columns
                            print("APPEND TO TABLE: " + str(table_name))

                            schema = [
                                bigquery.SchemaField(Default.LAT_FIELD, 'FLOAT', mode='NULLABLE'),
                                bigquery.SchemaField(Default.LON_FIELD, 'FLOAT', mode='NULLABLE')
                            ]
                            for field in list_field_names:
                                bq_field = bigquery.SchemaField(field, 'FLOAT', mode='NULLABLE')
                                schema.append(bq_field)

                            # Creates a temporary table in BigQuery
                            # This table will be used to store the appending columns by making use of a query
                            bq_temp_table_uri = Default.PROJECT_ID + '.' + bq_dataset + '.temp_' + table_name
                            bq_temp_upload_success = Utilities.load_csv_into_bigquery(
                                upload_uri,
                                bq_temp_table_uri,
                                schema,
                                skip_leading_rows=0)

                            # Skip if previous step failed
                            if bq_temp_upload_success:
                                # Appends to a BigQuery table using a query
                                bq_upload_success = Utilities.append_to_bigquery_table(
                                    bq_table_uri,
                                    bq_temp_table_uri,
                                    list_field_names
                                )

                            # Deletes the temporary table created in BigQuery
                            client_bq.delete_table(bq_temp_table_uri, not_found_ok=True)

                        # Removes the temporary CSV file stored in the bucket
                        bucket.delete_blob(table_name + '.csv')

                        # If the table creation or field appending failed
                        if not bq_upload_success:
                            # Closes the memory file
                            file_mem.close()
                            # Skip this date range and go to the next date range
                            break

                        # Closes the memory file when done with the current date
                        file_mem.close()

    print("END")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    download_weather_data_into_bigquery()

