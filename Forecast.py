import boto3
import json
from datetime import datetime
import csv
from io import StringIO

s3 = boto3.client('s3')
BUCKET = 'weather-data-opem-weather-api'

# List of expected city names
EXPECTED_CITIES = ['perth', 'melbourne', 'sydney', 'brisbane', 'adelaide']

def process_city_forecast(city_name, city_data):
    """Processes weather forecast data for a city into structured JSON"""
    forecast_days = []
    
    for forecast_day in city_data['forecast']['forecastday']:
        day_data = forecast_day['day']
        
        # Process hourly temperatures
        hourly_temps = {}
        for hour in forecast_day['hour']:
            time = hour['time'].split()[1]  # Extract HH:00
            hourly_temps[time] = hour['temp_c']
        
        forecast_days.append({
            'date': forecast_day['date'],
            'daily_summary': {
                'max_temp_c': day_data['maxtemp_c'],
                'min_temp_c': day_data['mintemp_c'],
                'avg_temp_c': day_data['avgtemp_c'],
                'total_precip_mm': day_data['totalprecip_mm'],
                'chance_of_rain': day_data['daily_chance_of_rain'],
                'condition': day_data['condition']['text'],
                'max_wind_kph': day_data['maxwind_kph'],
                'avg_humidity': day_data['avghumidity'],
                'uv_index': day_data['uv']
            },
            'hourly_temperatures': hourly_temps
        })
    
    return {
        'location': {
            'name': city_data['location']['name'],
            'region': city_data['location']['region'],
            'country': city_data['location']['country'],
            'lat': city_data['location']['lat'],
            'lon': city_data['location']['lon'],
            'tz_id': city_data['location']['tz_id']
        },
        'forecast_days': forecast_days,
        'last_processed': datetime.now().isoformat()
    }

def convert_to_csv(data):
    """Convert processed forecast data to CSV format"""
    # Create CSV for location data
    location_csv = StringIO()
    location_writer = csv.writer(location_csv)
    location_writer.writerow(['Location', 'Region', 'Country', 'Latitude', 'Longitude', 'Time Zone'])
    location_writer.writerow([
        data['location']['name'],
        data['location']['region'],
        data['location']['country'],
        data['location']['lat'],
        data['location']['lon'],
        data['location']['tz_id']
    ])
    
    # Create CSV for forecast data
    forecast_csv = StringIO()
    forecast_writer = csv.writer(forecast_csv)
    
    # Write forecast header
    forecast_writer.writerow([
        'Date', 'Max Temp (C)', 'Min Temp (C)', 'Avg Temp (C)', 
        'Total Precip (mm)', 'Chance of Rain', 'Condition',
        'Max Wind (kph)', 'Avg Humidity', 'UV Index',
        'Hourly Temperatures (JSON)'
    ])
    
    # Write forecast rows
    for day in data['forecast_days']:
        forecast_writer.writerow([
            day['date'],
            day['daily_summary']['max_temp_c'],
            day['daily_summary']['min_temp_c'],
            day['daily_summary']['avg_temp_c'],
            day['daily_summary']['total_precip_mm'],
            day['daily_summary']['chance_of_rain'],
            day['daily_summary']['condition'],
            day['daily_summary']['max_wind_kph'],
            day['daily_summary']['avg_humidity'],
            day['daily_summary']['uv_index'],
            json.dumps(day['hourly_temperatures'])
        ])
    
    return {
        'location': location_csv.getvalue(),
        'forecast': forecast_csv.getvalue()
    }

def delete_old_versions(bucket, prefix, keep_latest=True):
    """Delete old versions of files, keeping only the latest if specified"""
    try:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
        if not objects:
            return 0
        
        if keep_latest:
            sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
            deleted_count = 0
            for obj in sorted_objects[1:]:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                deleted_count += 1
                print(f"Deleted old file: {obj['Key']}")
            return deleted_count
        return 0
        
    except Exception as e:
        print(f"Error in delete_old_versions: {str(e)}")
        return 0

def get_city_name(city_data):
    """Extract and normalize city name from the data"""
    name = city_data['location']['name'].lower()
    
    # Check if the name matches our expected cities
    for expected in EXPECTED_CITIES:
        if expected in name:
            return expected
    
    # If not found, use the name but sanitize it
    return name.replace(" ", "_")

def lambda_handler(event, context):
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Get the latest forecast file from S3
        forecast_files = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f'to_be_processed/forecast/{current_date}/'
        ).get('Contents', [])
        
        if not forecast_files:
            return {
                'statusCode': 404,
                'body': json.dumps(f"No forecast files found for date {current_date}")
            }
        
        latest_forecast = max(forecast_files, key=lambda x: x['LastModified'])
        forecast_obj = s3.get_object(Bucket=BUCKET, Key=latest_forecast['Key'])
        forecast_data = json.loads(forecast_obj['Body'].read().decode('utf-8'))
        
        # Handle different input formats
        if isinstance(forecast_data, list):
            # Convert list to dict with proper city names
            forecast_data = {get_city_name(data): data for data in forecast_data}
        elif isinstance(forecast_data, dict):
            # Ensure keys are proper city names
            forecast_data = {get_city_name(data): data for key, data in forecast_data.items()}
        else:
            raise ValueError("Unexpected forecast data format")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_cities = []
        files_created = 0
        
        # Process each city's data
        for city_name, city_data in forecast_data.items():
            processed_data = process_city_forecast(city_name, city_data)
            processed_cities.append(city_name)
            
            # Create folder name like 'perth_forecast'
            prefix = f"processed/{city_name}_forecast/{current_date}/"
            
            # Upload JSON
            json_key = f"{prefix}forecast_{timestamp}.json"
            s3.put_object(
                Bucket=BUCKET,
                Key=json_key,
                Body=json.dumps(processed_data, indent=2),
                ContentType='application/json'
            )
            files_created += 1
            
            # Upload CSVs
            csv_data = convert_to_csv(processed_data)
            
            location_csv_key = f"{prefix}location_{timestamp}.csv"
            s3.put_object(
                Bucket=BUCKET,
                Key=location_csv_key,
                Body=csv_data['location'],
                ContentType='text/csv'
            )
            files_created += 1
            
            forecast_csv_key = f"{prefix}forecast_{timestamp}.csv"
            s3.put_object(
                Bucket=BUCKET,
                Key=forecast_csv_key,
                Body=csv_data['forecast'],
                ContentType='text/csv'
            )
            files_created += 1
            
            # Clean up old versions
            deleted_count = delete_old_versions(BUCKET, prefix)
            print(f"Deleted {deleted_count} old files for {city_name}")
        
        # Clean up raw forecast files
        raw_prefix = f"to_be_processed/forecast/{current_date}/"
        deleted_raw_count = delete_old_versions(BUCKET, raw_prefix)
        print(f"Deleted {deleted_raw_count} old raw forecast files")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Forecast data processed successfully',
                'processed_cities': processed_cities,
                'files_created': files_created,
                'files_deleted': deleted_raw_count,
                'processing_date': current_date
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to process forecast data'
            })
        }