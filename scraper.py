import requests
from datetime import datetime
import pandas as pd
from io import StringIO
import time
import csv

class DailyTrainCollector:
    def __init__(self):
        self.base_url = "https://api.irail.be/"
        self.gtfs_url = "https://gtfs.irail.be/nmbs/gtfs/latest/"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        self.output_file = f'daily_trains_{timestamp}.csv'
        self.setup_csv()

    def setup_csv(self):
        """Setup CSV file with headers"""
        headers = [
            'train_id',
            'train_type',
            'station_name',
            'station_position',
            'scheduled_arrival',
            'actual_arrival',
            'arrival_delay',
            'scheduled_departure',
            'actual_departure',
            'departure_delay',
            'platform',
            'is_cancelled'
        ]
        self.csv_file = open(self.output_file, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file, delimiter=';')
        self.csv_writer.writerow(headers)
        print(f"Created output file: {self.output_file}")

    def get_gtfs_data(self):
        """Get scheduled trains from GTFS data"""
        print("Downloading GTFS data...")
        try:
            # Get trips file
            print("Downloading trips data...")
            trips_response = requests.get(f"{self.gtfs_url}trips.txt")
            trips_df = pd.read_csv(StringIO(trips_response.text))
            
            # Get stop_times file
            print("Downloading stop times data...")
            stop_times_response = requests.get(f"{self.gtfs_url}stop_times.txt")
            stop_times_df = pd.read_csv(StringIO(stop_times_response.text))
            
            # Get unique train IDs from trips
            train_ids = set()
            for _, trip in trips_df.iterrows():
                train_id = trip.get('trip_short_name')
                if train_id is not None:
                    # Convert to string if it's an integer
                    train_id = str(train_id).strip()
                    if train_id:  # Only add if not empty after stripping
                        train_ids.add(train_id)
            
            print(f"Found {len(train_ids)} unique train IDs in GTFS data")
            return list(train_ids)
            
        except Exception as e:
            print(f"Error getting GTFS data: {e}")
            return []

    def format_time(self, timestamp):
        """Convert timestamp to HH:MM format"""
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime('%H:%M')
        except:
            return ""

    def process_vehicle(self, vehicle_id):
        """Process a single vehicle's data"""
        params = {
            'id': vehicle_id,
            'format': 'json',
            'lang': 'en'
        }
        
        try:
            response = requests.get(f"{self.base_url}vehicle/", params=params)
            response.raise_for_status()
            data = response.json()
            
            vehicle_info = data.get('vehicleinfo', {})
            stops = data.get('stops', {}).get('stop', [])
            total_stops = len(stops)
            
            if total_stops == 0:
                print(f"No stops found for train {vehicle_id}")
                return False
            
            train_id = vehicle_info.get('name', '').replace('BE.NMBS.', '')
            train_type = vehicle_info.get('type', '')
            
            print(f"Processing {total_stops} stops for train {train_id}")
            has_cancellations = False
            
            for i, stop in enumerate(stops):
                # Determine position
                if i == 0:
                    position = "DEPARTURE"
                elif i == total_stops - 1:
                    position = "ARRIVAL"
                else:
                    position = "INTERMEDIATE"
                
                # Calculate times and delays
                scheduled_arrival = int(stop.get('scheduledArrivalTime', "0"))
                arrival_delay = int(stop.get('arrivalDelay', "0"))
                actual_arrival = scheduled_arrival + arrival_delay
                
                scheduled_departure = int(stop.get('scheduledDepartureTime', "0"))
                departure_delay = int(stop.get('departureDelay', "0"))
                actual_departure = scheduled_departure + departure_delay
                
                # Check cancellation
                is_cancelled = (stop.get('canceled', 0) == 1 or 
                              stop.get('arrivalCanceled', 0) == 1 or 
                              stop.get('departureCanceled', 0) == 1)
                
                if is_cancelled:
                    has_cancellations = True
                    print(f"  Found cancellation at {stop.get('station', '')}")
                
                # Create row
                row = [
                    train_id,
                    train_type,
                    stop.get('station', ''),
                    position,
                    self.format_time(scheduled_arrival),
                    self.format_time(actual_arrival),
                    arrival_delay // 60,  # Convert to minutes
                    self.format_time(scheduled_departure),
                    self.format_time(actual_departure),
                    departure_delay // 60,  # Convert to minutes
                    stop.get('platform', ''),
                    1 if is_cancelled else 0
                ]
                
                self.csv_writer.writerow(row)
            
            self.csv_file.flush()
            
            if has_cancellations:
                print(f"Found cancellations for train {train_id}")
            
            return True
            
        except Exception as e:
            print(f"Error processing vehicle {vehicle_id}: {e}")
            return False

    def collect_data(self):
        """Main data collection method"""
        print("\nStarting data collection for all daily trains...")
        start_time = datetime.now()
        
        # Get all scheduled trains from GTFS
        train_ids = self.get_gtfs_data()
        
        if not train_ids:
            print("No trains found to process")
            return
        
        print(f"\nFound {len(train_ids)} trains to process")
        processed = 0
        errors = 0
        cancelled_count = 0
        
        # Process each train
        for i, train_id in enumerate(train_ids, 1):
            print(f"\nProcessing train {i}/{len(train_ids)}: {train_id}")
            
            try:
                if self.process_vehicle(train_id):
                    processed += 1
                else:
                    errors += 1
                
                # Progress update every 10 trains
                if i % 10 == 0:
                    print(f"\nProgress update:")
                    print(f"Processed: {i}/{len(train_ids)} trains")
                    print(f"Success: {processed}")
                    print(f"Errors: {errors}")
                    print(f"Time elapsed: {datetime.now() - start_time}")
                
            except Exception as e:
                print(f"Error processing train {train_id}: {e}")
                errors += 1
            
            time.sleep(1)  # Respect API rate limits
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\nCollection complete!")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration}")
        print(f"Total trains processed: {processed}")
        print(f"Errors encountered: {errors}")
        print(f"Results saved to: {self.output_file}")

    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'csv_file'):
            self.csv_file.close()

if __name__ == "__main__":
    collector = DailyTrainCollector()
    try:
        collector.collect_data()
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        del collector
