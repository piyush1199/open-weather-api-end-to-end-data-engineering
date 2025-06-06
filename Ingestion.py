import boto3
import json
from datetime import datetime
import os
import pandas as pd
from io import StringIO
import http.client

s3 = boto3.client('s3')
BUCKET = 'weather-data-opem-weather-api'

# Weather API Configuration
API_HOST = "weatherapi-com.p.rapidapi.com"
API_KEY = os.environ['API_KEY']
CITIES = ["Perth", "Melbourne", "Sydney", "Brisbane", "Adelaide"]

headers = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}

def get_weather_data(city, endpoint):
    """Fetch weather data from API for a specific city and endpoint"""
    conn = http.client.HTTPSConnection(API_HOST)
    try:
        conn.request("GET", f"/{endpoint}.json?q={city}", headers=headers)
        res = conn.getresponse()
        data = json.loads(res.read().decode('utf-8'))
        return data
    finally:
        conn.close()

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

def save_to_s3(bucket, prefix, data, file_type='json'):
    """Save data to S3 with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}{file_type}_data_{timestamp}.{file_type}"
    
    if file_type == 'json':
        body = json.dumps(data)
        content_type = 'application/json'
    elif file_type == 'csv':
        csv_buffer = StringIO()
        pd.DataFrame(data).to_csv(csv_buffer, index=False)
        body = csv_buffer.getvalue()
        content_type = 'text/csv'
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type
    )
    return key

def lambda_handler(event, context):
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Fetch and save forecast data
        forecast_data = []
        for city in CITIES:
            forecast = get_weather_data(city, 'forecast')
            forecast_data.append(forecast)
        
        forecast_prefix = f"to_be_processed/forecast/{current_date}/"
        forecast_key = save_to_s3(BUCKET, forecast_prefix, forecast_data, 'json')
        
        # Fetch and save alert data
        alert_data = []
        for city in CITIES:
            try:
                alert = get_weather_data(city, 'alerts')  # Assuming 'alerts' endpoint exists
                alert_data.append(alert)
            except Exception as e:
                print(f"Failed to get alerts for {city}: {str(e)}")
                alert_data.append({'city': city, 'error': str(e)})
        
        alert_prefix = f"to_be_processed/alert/{current_date}/"
        alert_key = save_to_s3(BUCKET, alert_prefix, alert_data, 'json')
        
        # Process the data into final format
        current_list = []
        for city_data in forecast_data:
            city_name = city_data['location']['name']
            state_name = city_data['location']['region']
            local_time = city_data['location']['localtime']
            current_temp = city_data['current']['temp_c']
            feels_like = city_data['current']['feelslike_c']
            current_weather = city_data['current']['condition']['text']
            latitude = city_data['location']['lat']
            longitude = city_data['location']['lon']
            wind_kph = city_data['current']["wind_kph"]
            visibility = city_data['current']['vis_km']
            UV_index = city_data['current']['uv']
            
            # Find matching alert data
            alerts_text = "No alerts"
            for alert in alert_data:
                if isinstance(alert, dict) and alert.get('location', {}).get('name') == city_name:
                    if 'alerts' in alert:
                        alerts = alert['alerts'].get('alert', [])
                        alerts_text = " | ".join([a.get('description', '') for a in alerts]) if alerts else "No alerts"
                    break
            
            current_element = {
                'City': city_name, 'State': state_name, 'Local_time': local_time,
                'Current_temp_(C)': current_temp, 'Feels_like_(C)': feels_like,
                'Current_Weather': current_weather, 'Latitude': latitude,
                'Longitude': longitude, 'Wind_kph': wind_kph,
                'Visibility_(km)': visibility, 'UV_index': UV_index,
                'Alerts': alerts_text
            }
            current_list.append(current_element)
        
        # Save processed data
        processed_prefix = f"processed/current_weather/{current_date}/"
        json_key = save_to_s3(BUCKET, processed_prefix, current_list, 'json')
        csv_key = save_to_s3(BUCKET, processed_prefix, current_list, 'csv')
        
        # Clean up old files
        deleted_forecast = delete_old_versions(BUCKET, f"to_be_processed/forecast/{current_date}/")
        deleted_alerts = delete_old_versions(BUCKET, f"to_be_processed/alert/{current_date}/")
        deleted_processed = delete_old_versions(BUCKET, processed_prefix)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data processed successfully',
                'processed_locations': {
                    'json': f"s3://{BUCKET}/{json_key}",
                    'csv': f"s3://{BUCKET}/{csv_key}"
                },
                'raw_data_locations': {
                    'forecast': f"s3://{BUCKET}/{forecast_key}",
                    'alerts': f"s3://{BUCKET}/{alert_key}"
                },
                'cleanup_stats': {
                    'deleted_forecast_files': deleted_forecast,
                    'deleted_alert_files': deleted_alerts,
                    'deleted_processed_files': deleted_processed
                }
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to process weather data'
            })
        }