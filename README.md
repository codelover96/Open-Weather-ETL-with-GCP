
# Open Weather ETL with Google Cloud Platform

This project utilizes Google Cloud Platform and Python 3 to generate real time reports about weather conditions from 5 selected locations.

## Architecture
![Project Architecture](/project_arch.png "Looker Studio Report")

## Overview
1. Exctract raw data (json) from Open Weather API endpoints using python
2. Transfrom raw data with Python and Google Cloud Functions
3. Store data in a Google Storage Bucket
4. Load transformed data in Big Query for streamlined querying and analysis
5. Schedule automated data fetching using Cloud Scheduler 
6. Create a real-time dashboard with Looker Studio.


![Example Looker Studio Report with current weather and forecasted weather reports](Looker%20Studio%20Reports/looker_studio_report.JPG "Looker Studio Report")

