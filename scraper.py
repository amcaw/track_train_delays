import requests
import csv
from datetime import datetime, timedelta
import time
import pandas as pd
from pathlib import Path
import unicodedata
import hashlib

class TrainDataCollector:
    def __init__(self, output_file=None):
        """
        Initialize the TrainDataCollector with optional output file.
        
        Args:
            output_file (str, optional): Path to the output CSV file. 
                                         Defaults to train_delays_YYYYMMDD.csv
        """
        today = datetime.now()
        if output_file is None:
            output_file = f'train_delays_{today.strftime("%Y%m%d")}.csv'
        self.output_file = output_file
        self.base_url = "https://api.irail.be/"
        self.gtfs_url = "https://gtfs.irail.be/nmbs/gtfs/latest/"
        self.processed_journeys = set()  # Track unique journeys
        self.routes_df = None
        self.trips_df = None
        self.stops_df = None
        self.load_gtfs_data()
        self.setup_csv()
        
    def generate_journey_identifier(self, vehicle_data):
        """
        Create a unique identifier for a journey to prevent duplicates.
        
        Args:
            vehicle_data (dict): Vehicle data from iRail API
        
        Returns:
            str: Unique journey identifier or None if generation fails
        """
        try:
            vehicle_info = vehicle_data.get('vehicleinfo', {})
            stops_data = vehicle_data.get('stops', {})
            
            if not isinstance(stops_data, dict):
                return None
            
            stop_list = stops_data.get('stop', [])
            if not stop_list:
                return None
            
            # Create a unique string using first and last stop + train ID
            first_stop = stop_list[0].get('station', '')
            last_stop = stop_list[-1].get('station', '')
            train_id = vehicle_info.get('name', '').replace('BE.NMBS.', '')
            train_type = vehicle_info.get('type', '')
            scheduled_first_departure = int(stop_list[0].get('scheduledDepartureTime', 0))
            
            # Create a deterministic journey identifier
            journey_string = f"{train_id}_{train_type}_{first_stop}_{last_stop}_{scheduled_first_departure}"
            
            # Use hashlib to create a consistent identifier
            return hashlib.md5(journey_string.encode()).hexdigest()
        
        except Exception as e:
            print(f"Error generating journey identifier: {e}")
            return None

    def load_gtfs_data(self):
        """Load necessary GTFS data files from iRail"""
        print("Loading GTFS data...")
        try:
            # Load routes
            response = requests.get(f"{self.gtfs_url}routes.txt")
            with open('routes_temp.txt', 'wb') as f:
                f.write(response.content)
            self.routes_df = pd.read_csv('routes_temp.txt')
            Path('routes_temp.txt').unlink()
            
            # Load trips
            response = requests.get(f"{self.gtfs_url}trips.txt")
            with open('trips_temp.txt', 'wb') as f:
                f.write(response.content)
            self.trips_df = pd.read_csv('trips_temp.txt')
            Path('trips_temp.txt').unlink()
            
            # Load stop_times
            response = requests.get(f"{self.gtfs_url}stop_times.txt")
            with open('stop_times_temp.txt', 'wb') as f:
                f.write(response.content)
            self.stops_df = pd.read_csv('stop_times_temp.txt')
            Path('stop_times_temp.txt').unlink()
            
            print(f"Loaded {len(self.routes_df)} routes, {len(self.trips_df)} trips")
            
        except Exception as e:
            print(f"Error loading GTFS data: {e}")
            raise
    
    def setup_csv(self):
        """Create CSV with headers"""
        headers = [
            'date',
            'route_id',
            'trip_id',
            'train_id',
            'train_type',
            'station_name',
            'station_position',  # First, Intermediate, Last
            'scheduled_arrival',
            'actual_arrival',
            'arrival_delay',
            'scheduled_departure',
            'actual_departure',
            'departure_delay',
            'platform',
            'is_cancelled',
            'captured_at'
        ]
        
        self.csv_file = open(self.output_file, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(headers)
        print(f"Created new CSV file: {self.output_file}")

    def __del__(self):
        """Cleanup when the object is destroyed"""
        if hasattr(self, 'csv_file') and self.csv_file:
            self.csv_file.close()

    def normalize_station_name(self, station_name):
        """
        Remove accents and normalize station name for URL
        
        Args:
            station_name (str): Original station name
        
        Returns:
            str: Normalized station name
        """
        normalized = unicodedata.normalize('NFD', station_name)
        without_accents = ''.join(c for c in normalized if not unicodedata.combining(c))
        replacements = {
            'é': 'e', 'è': 'e', 'ë': 'e',
            'á': 'a', 'à': 'a',
            'ü': 'u', 'ï': 'i',
            'ö': 'o', 'ô': 'o',
            '/': '-', '\'': ''
        }
        for old, new in replacements.items():
            without_accents = without_accents.replace(old, new)
        return without_accents.lower()  # Added lowercase for consistency

    def fetch_vehicle_data(self, vehicle_id):
        """
        Fetch details for a specific vehicle/train
        
        Args:
            vehicle_id (str): Vehicle identifier
        
        Returns:
            dict: Vehicle data or None if fetch fails
        """
        params = {
            'id': vehicle_id,
            'format': 'json',
            'lang': 'en'
        }
        
        try:
            print(f"\nFetching vehicle data for {vehicle_id}")
            response = requests.get(f"{self.base_url}vehicle/", params=params)
            response.raise_for_status()
            data = response.json()
            print("Response received. Keys:", data.keys() if isinstance(data, dict) else "Not a dictionary")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching vehicle data: {e}")
            return None

    def fetch_connections(self, from_station, to_station):
        """
        Fetch all connections for today between two stations
        
        Args:
            from_station (str): Departure station
            to_station (str): Arrival station
        
        Returns:
            dict: Connection data or None if fetch fails
        """
        today = datetime.now()
        current_time = today.strftime('%H%M')
        
        params = {
            'from': from_station,
            'to': to_station,
            'date': today.strftime('%d%m%y'),
            'time': '0000',  # Start from midnight
            'timeSel': 'departure',
            'format': 'json',
            'lang': 'en',
            'alerts': 'false'
        }
        
        try:
            response = requests.get(f"{self.base_url}connections/", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching connections: {e}")
            return None

    def process_vehicle_stops(self, vehicle_data, route_id, trip_id):
        """
        Process all stops of a vehicle and save to CSV
        
        Args:
            vehicle_data (dict): Vehicle data from iRail API
            route_id (str): Route identifier
            trip_id (str): Trip identifier
        
        Returns:
            bool: True if processing successful, False otherwise
        """
        try:
            if not isinstance(vehicle_data, dict):
                print("Warning: vehicle_data is not a dictionary")
                return False
                
            vehicle_info = vehicle_data.get('vehicleinfo', {})
            stops_data = vehicle_data.get('stops', {})
            
            if not isinstance(stops_data, dict):
                print(f"Warning: stops_data is not a dictionary, got {type(stops_data)}")
                return False
                
            stop_list = stops_data.get('stop', [])
            if not stop_list:
                print("Warning: no stops found in vehicle data")
                return False
                
            train_id = vehicle_info.get('name', '').replace('BE.NMBS.', '')
            train_type = vehicle_info.get('type', '')
            
            print(f"\nProcessing {len(stop_list)} stops for train {train_id}")
            
            # Filter for past stops only
            current_time = datetime.now().timestamp()
            past_stops = [stop for stop in stop_list 
                         if int(stop.get('scheduledDepartureTime', current_time + 1)) < current_time]
            
            for i, stop in enumerate(past_stops):
                # Determine stop position
                if i == 0:
                    position = "First"
                elif i == len(past_stops) - 1:
                    position = "Last"
                else:
                    position = "Intermediate"
                
                # Get timestamps
                scheduled_arrival = int(stop.get('scheduledArrivalTime', 0))
                scheduled_departure = int(stop.get('scheduledDepartureTime', 0))
                arrival_delay = int(stop.get('arrivalDelay', 0))
                departure_delay = int(stop.get('departureDelay', 0))
                
                # Convert timestamps
                sched_arr = datetime.fromtimestamp(scheduled_arrival) if scheduled_arrival else None
                sched_dep = datetime.fromtimestamp(scheduled_departure) if scheduled_departure else None
                actual_arr = datetime.fromtimestamp(scheduled_arrival + arrival_delay) if scheduled_arrival else None
                actual_dep = datetime.fromtimestamp(scheduled_departure + departure_delay) if scheduled_departure else None
                
                row = [
                    datetime.now().strftime('%Y-%m-%d'),  # date
                    route_id,
                    trip_id,
                    train_id,
                    train_type,
                    stop.get('station', ''),
                    position,
                    sched_arr.strftime('%H:%M:%S') if sched_arr else '',
                    actual_arr.strftime('%H:%M:%S') if actual_arr else '',
                    arrival_delay // 60,  # Convert to minutes
                    sched_dep.strftime('%H:%M:%S') if sched_dep else '',
                    actual_dep.strftime('%H:%M:%S') if actual_dep else '',
                    departure_delay // 60,  # Convert to minutes
                    stop.get('platform', 'unknown'),
                    1 if stop.get('canceled', '0') == '1' else 0,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                
                self.csv_writer.writerow(row)
                self.csv_file.flush()
                print(f"Wrote row for stop {stop.get('station', '')}")
                
            return True
            
        except Exception as e:
            print(f"Error processing vehicle stops: {e}")
            print("Vehicle data structure:", vehicle_data)
            return False

    def collect_data(self):
        """Collect train data for all routes"""
        # Filter for train routes only
        train_routes = self.routes_df[self.routes_df['route_type'] == 100]
        total_processed = 0
        total_duplicates_skipped = 0
        total_routes_processed = 0
        current_time = datetime.now().timestamp()
        
        for _, route in train_routes.iterrows():
            print(f"\nProcessing route_id: {route['route_id']}")
            
            # Get trips for this route
            route_trips = self.trips_df[self.trips_df['route_id'] == route['route_id']]
            
            for _, trip in route_trips.iterrows():
                # Get headsign stations
                if '--' in route['route_long_name']:
                    from_station, to_station = [
                        self.normalize_station_name(s.strip()) 
                        for s in route['route_long_name'].split('--')
                    ]
                    
                    # Get connections for this route
                    data = self.fetch_connections(from_station, to_station)
                    if not data or 'connection' not in data:
                        continue
                    
                    for conn in data.get('connection', []):
                        # Check if this connection is in the past
                        departure_time = int(conn['departure'].get('time', 0))
                        if departure_time >= current_time:
                            continue  # Skip future connections
                            
                        vehicle_id = conn['departure'].get('vehicle')
                        if vehicle_id:
                            vehicle_data = self.fetch_vehicle_data(vehicle_id)
                            if vehicle_data:
                                # Generate unique journey identifier
                                journey_id = self.generate_journey_identifier(vehicle_data)
                                
                                # Skip if journey already processed
                                if journey_id in self.processed_journeys:
                                    total_duplicates_skipped += 1
                                    print(f"Skipping duplicate journey: {journey_id}")
                                    continue
                                
                                if self.process_vehicle_stops(vehicle_data, route['route_id'], trip['trip_id']):
                                    # Add to processed journeys
                                    self.processed_journeys.add(journey_id)
                                    total_processed += 1
                                    print(f"Processed vehicle {vehicle_id} ({total_processed} total)")
                            
                            time.sleep(1)
            
            total_routes_processed += 1
            print(f"Completed route {total_routes_processed}/{len(train_routes)}")
        
        print(f"\nCollection complete.")
        print(f"Total routes processed: {total_routes_processed}")
        print(f"Total vehicles processed: {total_processed}")
        print(f"Total duplicate journeys skipped: {total_duplicates_skipped}")

if __name__ == "__main__":
    collector = TrainDataCollector()
    try:
        collector.collect_data()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        del collector  # Ensure file is closed properly
