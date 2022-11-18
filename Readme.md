# Apache kafka project

This is a sole project done during my traineeship at Fusemachines Nepal. This project includes topics like confluent kafka, pyspark and postgresql 

## Create and activate a virtual environment:

`>> python -m venv env_name`

`>> . env_name/bin/activate`

Use `pip install -r requirements.txt` to install the required packages.

## Sources
 + API  Source : https://openweathermap.org/forecast5
 + Lat and lon source :  https://simplemaps.com/data/np-cities
  
##  Request format
 api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}


## Workflow
![workflow](/Assets/workflow.png)

| **Folders/Files**                  | **Description**                                          |
| ---------------------------------- | -------------------------------------------------------- |
| **Assets**                         | Contains the images used in the README.md file           |
| **.gitignore**                     | Contains the files and folders to be ignored by git      |
| **connec_topic_list_delete.ipynb** | Contains code to list and delete connectors and topics   |
| **consumer_dump.json**             | json file containg response of API                       |
| **coordinates.py**                 | contains cooordinates in lat:long dictionary format      |
| **main_file.ipynb**                | contains code for whole project                          |
| **README.md**                      | Contains the project description                         |
| **requirements.txt**               | Contains the required packages to run the project        |
| **schemas.py**                     | Contains all manually created schemas for each questions |
