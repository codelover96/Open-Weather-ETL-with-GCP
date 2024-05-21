
# Open Weather ETL with Google Cloud Platform

This project utilizes Google Cloud Platform and Python 3 to generate real time reports about weather conditions from 5 selected locations.

## Project Architecture
![Project Architecture](/project_architecture.png "project architecture")

## Overview
1. Exctract raw data (json) from Open Weather API endpoints using python requests
2. Transfrom raw data with Python Pandas 
3. Store data in a Google Storage Bucket
4. Deploy python code as a Google Cloud Function
5. Load transformed data in Big Query for streamlined querying and analysis
6. Schedule automated data fetching using Cloud Scheduler
7. Create a real-time dashboard with Looker Studio.


![A weather report made with Looker Studio about current and forecasted weather.](Looker%20Studio%20Reports/looker_studio_report.JPG "Looker Studio Report")

## Description
This is an end to end Extract-Transform-Load pipeline using GCP services and the OpenWeather API. 
### Steps
Weather data is fetched from two available API endpoints and is transformed with Pandas. It is stored in a Google Storage Bucket and fed in the desired format into BigQuery. Then the main.py script is depployed as Cloud Function to be run automatically based on a scheduled by the Google Cloud Scheduler. Lastly, Looker Studio is utilized to generate interactive reports and visualize the weather data.  

## Prerequisites
1. Having a Google Cloud Platform account
2. Having an Open Weather API key

## How to deploy
1. Install package requirements
2. Place your open weather key, your Google project id, your bucket name and dataset id in a nameless .env file
3. Store your Google storage and big query service account keys in a folder names 'keys'


## Contributing
Clone repo and create pull requests of your new/updated code. Or you can fork this project and work on your own. Feel free to add/update any code piece you find useful.

## License
This project is released under MIT License

Copyright (c) 2024 codelover96

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
