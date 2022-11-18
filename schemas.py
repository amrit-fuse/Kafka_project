# define schema manually ## schema extracted from weather_df.json file wont'work
Schema_Q1 = {
    "type": "struct",
    "fields": [
        {
            "field": "name",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "sunrise",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "sunset",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "dt",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "dt_txt",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "pop",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "visibility",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "lat",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "lon",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "cloudiness",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "feels_like",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "grnd_level",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "humidity",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "pressure",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "sea_level",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "temp_max",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "temp_min",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "pod",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "deg",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "gust",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "speed",
            "type": "double",
            "nullable": "false"
        },
        {
            "field": "description",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "main",
            "type": "string",
            "nullable": "false"
        }
    ]
}


# Schemas for Q2

#  name|max_wind_speed|max_temp|max_humidity|max_pressure|max_cloudiness
# Schema for Q2
Schema_Q2 = {
    "type": "struct",
    "fields": [
        {
            "field": "name",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "max_wind_speed",
            "type": "double",
            "nullable": "false"
        },

        {
            "field": "max_temp",
            "type": "double",
            "nullable": "false"

        },
        {
            "field": "max_humidity",
            "type": "int64",
            "nullable": "false"
        },
        {
            "field": "max_pressure",
            "type": "int64",
            "nullable": "false"
        },

        {
            "field": "max_cloudiness",
            "type": "int64",
            "nullable": "false"
        }
    ]
}


# Schemas for Q3
#  name|       day|max_temp_night|
Schema_Q3 = {
    "type": "struct",
    "fields": [
        {
            "field": "name",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "day",
            "type": "string",
            "nullable": "false"
        },
        {
            "field": "min_temp_night",
            "type": "double",
            "nullable": "false"
        }
    ]
}
