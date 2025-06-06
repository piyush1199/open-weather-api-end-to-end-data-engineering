import boto3
import json
from datetime import datetime
import os
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')
BUCKET = 'weather-data-opem-weather-api'

def delete_old_versions(bucket, prefix, keep_latest=True):
    """Delete old versions of files, keeping only the latest if specified"""
    try:
        # List all objects with the given prefix
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
        
        if not objects:
            return 0
        
        # If we only want to keep the latest, find and delete all others
        if keep_latest:
            # Sort by LastModified (newest first)
            sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
            latest = sorted_objects[0]
            
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

def lambda_handler(event, context):
    try:
        # Get current date in the format used in the S3 paths
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Get the latest forecast and alert files from S3 with date prefix
        forecast_files = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f'to_be_processed/forecast/{current_date}/'
        ).get('Contents', [])
        
        alert_files = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f'to_be_processed/alert/{current_date}/'
        ).get('Contents', [])

        if not forecast_files:
            return {
                'statusCode': 404,
                'body': json.dumps(f"No forecast files found for date {current_date}")
            }
            
        if not alert_files:
            return {
                'statusCode': 404,
                'body': json.dumps(f"No alert files found for date {current_date}")
            }

        # Sort by last modified time and pick the most recent
        latest_forecast = max(forecast_files, key=lambda x: x['LastModified'])
        latest_alert = max(alert_files, key=lambda x: x['LastModified'])

        # Download and load JSON data
        forecast_obj = s3.get_object(Bucket=BUCKET, Key=latest_forecast['Key'])
        alert_obj = s3.get_object(Bucket=BUCKET, Key=latest_alert['Key'])
        
        forecast_dict = json.loads(forecast_obj['Body'].read().decode('utf-8'))
        alert_dict = json.loads(alert_obj['Body'].read().decode('utf-8'))

        current_list = []
        
        # Process each city's forecast data
        for city_name, city_data in forecast_dict.items():
            # Extract current weather parameters
            current_temp = city_data['current']['temp_c']
            feels_like = city_data['current']['feelslike_c']
            current_weather = city_data['current']['condition']['text']
            wind_kph = city_data['current']['wind_kph']
            visibility = city_data['current']['vis_km']
            uv_index = city_data['current']['uv']
            
            # Extract location data
            location = city_data['location']
            state_name = location['region']
            local_time = location['localtime']
            latitude = location['lat']
            longitude = location['lon']
            
            # Extract alerts (default to "No alerts" if none exist)
            city_alerts = alert_dict.get(city_name, {}).get('alerts', {}).get('alert', [])
            alerts_text = "No alerts" if not city_alerts else str(city_alerts)
            
            current_element = {
                'City': city_name,
                'State': state_name,
                'Local_time': local_time,
                'Current_temp_(C)': current_temp,
                'Feels_like_(C)': feels_like,
                'Current_Weather': current_weather,
                'Latitude': latitude,
                'Longitude': longitude,
                'Wind_kph': wind_kph,
                'Visibility_(km)': visibility,
                'UV_index': uv_index,
                'Alerts': alerts_text
            }
            current_list.append(current_element)

        # Generate timestamp for the new files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_prefix = f"processed/current_weather/{current_date}/"
        
        # Create DataFrame for CSV conversion
        df = pd.DataFrame(current_list)
        
        # Save JSON file
        json_key = f"{processed_prefix}current_weather_{timestamp}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=json_key,
            Body=json.dumps(current_list),
            ContentType='application/json'
        )
        
        # Save CSV file (better for Athena)
        csv_key = f"{processed_prefix}current_weather_{timestamp}.csv"
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=BUCKET,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Clean up old processed files (keep only latest)
        deleted_processed_count = delete_old_versions(BUCKET, processed_prefix)
        
        # Clean up old raw forecast files (keep only latest)
        deleted_forecast_count = delete_old_versions(BUCKET, f"to_be_processed/forecast/{current_date}/")
        
        # Clean up old raw alert files (keep only latest)
        deleted_alert_count = delete_old_versions(BUCKET, f"to_be_processed/alert/{current_date}/")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Current weather data processed successfully',
                'processed_locations': {
                    'json': f"s3://{BUCKET}/{json_key}",
                    'csv': f"s3://{BUCKET}/{csv_key}"
                },
                'cleanup_stats': {
                    'deleted_processed_files': deleted_processed_count,
                    'deleted_forecast_files': deleted_forecast_count,
                    'deleted_alert_files': deleted_alert_count
                },
                'total_deleted': deleted_processed_count + deleted_forecast_count + deleted_alert_count
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to process current weather data'
            })
        }