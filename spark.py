# # # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime, timedelta
# # import pandas as pd
# # from math import radians, cos, sin, sqrt, atan2
# # import os 

# # default_args = {
# #     'owner': 'airflow',
# #     'depends_on_past': False,
# #     'start_date': datetime(2024, 7, 20),
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=5),
# # }

# # dag = DAG(
# #     'airports_analysis',
# #     default_args=default_args,
# #     description='A DAG to perform various analytics on airport data',
# #     schedule_interval='0 0 * * *', 
# # )
 
# # def haversine(lat1, lon1, lat2, lon2):
# #     R = 6371  # Radius of the Earth in km
# #     dlat = radians(lat2 - lat1)
# #     dlon = radians(lon2 - lon1)
# #     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
# #     c = 2 * atan2(sqrt(a), sqrt(1 - a))
# #     distance = R * c
# #     return distance

# # def perform_analytics():
# #     # Define output directory
# #     output_dir = '/home/boss/airflow/dags/analytics/'
    
# #     # Create the directory if it does not exist
# #     if not os.path.exists(output_dir):
# #         os.makedirs(output_dir)
    
# #     # Load data
# #     file_path = '/home/boss/airflow/dags/airports.dat'
# #     df = pd.read_csv(file_path, header=None, names=[
# #         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
# #         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
# #     ])
    
# #     # 1. Distance Calculation (example airports)
# #     airport1 = df[df['iata'] == 'GKA'].iloc[0]
# #     airport2 = df[df['iata'] == 'MAG'].iloc[0]
# #     distance = haversine(
# #         airport1['latitude'], airport1['longitude'],
# #         airport2['latitude'], airport2['longitude']
# #     )
# #     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
    
# #     # 2. Airports by Country
# #     airports_by_country = df.groupby('country').size().reset_index(name='count')
# #     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
    
# #     # 3. Airports within a Region (example region)
# #     min_lat, max_lat, min_lon, max_lon = -10, 10, 120, 150
# #     airports_in_region = df[
# #         (df['latitude'] >= min_lat) & (df['latitude'] <= max_lat) &
# #         (df['longitude'] >= min_lon) & (df['longitude'] <= max_lon)
# #     ]
# #     airports_in_region.to_csv(os.path.join(output_dir, 'airports_in_region.csv'), index=False)
    
# #     # 4. Airport Statistics
# #     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
# #     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
# #     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
    
# #     # 5. Airport Density by Region (example regions)
# #     def determine_region(lat, lon):
# #         if -10 <= lat <= 0 and 120 <= lon <= 130:
# #             return 'Region 1'
# #         elif 0 <= lat <= 10 and 130 <= lon <= 140:
# #             return 'Region 2'
# #         else:
# #             return 'Other'

# #     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
# #     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
# #     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)
    
# #     # 6. Airports by Country with Distance Calculation
# #     def calculate_distances(df, country):
# #         df_country = df[df['country'] == country]
# #         df_country['nearest_airport'] = None
# #         df_country['distance'] = None
        
# #         for index, row in df_country.iterrows():
# #             distances = df_country.apply(
# #                 lambda x: haversine(row['latitude'], row['longitude'], x['latitude'], x['longitude']),
# #                 axis=1
# #             )
# #             nearest_idx = distances.idxmin()
# #             df_country.at[index, 'nearest_airport'] = df_country.loc[nearest_idx, 'iata']
# #             df_country.at[index, 'distance'] = distances.min()
        
# #         return df_country

# #     countries = df['country'].unique()
# #     for country in countries:
# #         country_airports_with_distances = calculate_distances(df, country)
# #         country_airports_with_distances.to_csv(os.path.join(output_dir, f'airports_{country}_with_distances.csv'), index=False)

# #     print("Analytics have been completed and saved.")

# # perform_analytics_task = PythonOperator(
# #     task_id='perform_analytics',
# #     python_callable=perform_analytics,
# #     dag=dag,
# # )
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime, timedelta
# # import pandas as pd
# # from math import radians, cos, sin, sqrt, atan2
# # import os

# # default_args = {
# #     'owner': 'airflow',
# #     'depends_on_past': False,
# #     'start_date': datetime(2024, 7, 20),
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=5),
# # }

# # dag = DAG(
# #     'airports_analysis',
# #     default_args=default_args,
# #     description='A DAG to perform various analytics on airport data',
# #     schedule_interval='0 0 * * *',
# # )

# # def haversine(lat1, lon1, lat2, lon2):
# #     R = 6371  # Radius of the Earth in km
# #     dlat = radians(lat2 - lat1)
# #     dlon = radians(lon2 - lon1)
# #     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
# #     c = 2 * atan2(sqrt(a), sqrt(1 - a))
# #     distance = R * c
# #     return distance

# # def perform_analytics():
# #     # Define output directory
# #     output_dir = '/home/boss/airflow/dags/analytics/'
    
# #     # Create the directory if it does not exist
# #     if not os.path.exists(output_dir):
# #         os.makedirs(output_dir)
    
# #     # Load data
# #     file_path = '/home/boss/airflow/dags/airports.dat'
# #     df = pd.read_csv(file_path, header=None, names=[
# #         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
# #         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
# #     ])
    
# #     # 1. Distance Calculation (example airports)
# #     try:
# #         airport1 = df[df['iata'] == 'GKA'].iloc[0]
# #         airport2 = df[df['iata'] == 'MAG'].iloc[0]
# #         distance = haversine(
# #             airport1['latitude'], airport1['longitude'],
# #             airport2['latitude'], airport2['longitude']
# #         )
# #         print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
# #     except IndexError:
# #         print("Example airports not found in the dataset.")
    
# #     # 2. Airports by Country
# #     airports_by_country = df.groupby('country').size().reset_index(name='count')
# #     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
    
# #     # 3. Airports within a Region (covering more regions)
# #     regions = {
# #         'Asia/Manila': {'min_lat': 4.6, 'max_lat': 21.0, 'min_lon': 116.9, 'max_lon': 126.6},
# #         'Asia/Jakarta': {'min_lat': -10.0, 'max_lat': 5.9, 'min_lon': 95.0, 'max_lon': 141.0},
# #         'Pacific': {'min_lat': -20.0, 'max_lat': 0.0, 'min_lon': 140.0, 'max_lon': 180.0},
# #         # Add other regions as needed
# #     }

# #     for region, bounds in regions.items():
# #         airports_in_region = df[
# #             (df['latitude'] >= bounds['min_lat']) & (df['latitude'] <= bounds['max_lat']) &
# #             (df['longitude'] >= bounds['min_lon']) & (df['longitude'] <= bounds['max_lon'])
# #         ]
# #         airports_in_region.to_csv(os.path.join(output_dir, f'airports_in_{region}.csv'), index=False)
    
# #     # 4. Airport Statistics
# #     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
# #     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
# #     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
    
# #     # 5. Airport Density by Region (covering more regions)
# #     def determine_region(lat, lon):
# #         if -10 <= lat <= 0 and 120 <= lon <= 130:
# #             return 'Region 1'
# #         elif 0 <= lat <= 10 and 130 <= lon <= 140:
# #             return 'Region 2'
# #         else:
# #             return 'Other'

# #     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
# #     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
# #     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)
    
# #     # 6. Airports by Country with Distance Calculation
# #     def calculate_distances(df, country):
# #         df_country = df[df['country'] == country]
# #         if df_country.empty:
# #             return df_country
        
# #         df_country['nearest_airport'] = None
# #         df_country['distance'] = None
        
# #         for index, row in df_country.iterrows():
# #             distances = df_country.apply(
# #                 lambda x: haversine(row['latitude'], row['longitude'], x['latitude'], x['longitude']),
# #                 axis=1
# #             )
# #             nearest_idx = distances.nsmallest(2).idxmax()  # To avoid 0 distance (itself)
# #             df_country.at[index, 'nearest_airport'] = df_country.loc[nearest_idx, 'iata']
# #             df_country.at[index, 'distance'] = distances.loc[nearest_idx]
        
# #         return df_country

# #     countries = df['country'].unique()
# #     for country in countries:
# #         country_airports_with_distances = calculate_distances(df, country)
# #         if not country_airports_with_distances.empty:
# #             country_airports_with_distances.to_csv(os.path.join(output_dir, f'airports_{country}_with_distances.csv'), index=False)

# #     print("Analytics have been completed and saved.")

# # perform_analytics_task = PythonOperator(
# #     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )
# # 
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime, timedelta
# # import pandas as pd
# # from math import radians, cos, sin, sqrt, atan2
# # import os
# # 
# # default_args = {
#     # 'owner': 'airflow',
#     # 'depends_on_past': False,
#     # 'start_date': datetime(2024, 7, 20),
#     # 'email_on_failure': False,
#     # 'email_on_retry': False,
#     # 'retries': 1,
#     # 'retry_delay': timedelta(minutes=5),
# # }
# # 
# # dag = DAG(
#     # 'airports_analysis',
#     # default_args=default_args,
#     # description='A DAG to perform various analytics on airport data',
#     # schedule_interval='0 0 * * *',
# # )
# # 
# # def haversine(lat1, lon1, lat2, lon2):
#     # R = 6371  # Radius of the Earth in km
#     # dlat = radians(lat2 - lat1)
#     # dlon = radians(lat2 - lon2)
#     # a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     # c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     # distance = R * c
#     # return distance
# # 
# # def perform_analytics():
#     Define output directory
#     # output_dir = '/home/boss/airflow/dags/analytics/'
#     # 
#     Create the directory if it does not exist
#     # if not os.path.exists(output_dir):
#         # os.makedirs(output_dir)
#     # 
#     Load data
#     # file_path = '/home/boss/airflow/dags/airports.dat'
#     # df = pd.read_csv(file_path, header=None, names=[
#         # 'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         # 'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     # ])
#     # 
#     1. Distance Calculation (example airports)
#     # try:
#         # airport1 = df[df['iata'] == 'GKA'].iloc[0]
#         # airport2 = df[df['iata'] == 'MAG'].iloc[0]
#         # distance = haversine(
#             # airport1['latitude'], airport1['longitude'],
#             # airport2['latitude'], airport2['longitude']
#         # )
#         # print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
#     # except IndexError:
#         # print("Example airports not found in the dataset.")
#     # 
#     2. Airports by Country
#     # airports_by_country = df.groupby('country').size().reset_index(name='count')
#     # airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
#     # 
#     3. Airports within a Region (Dynamic regions based on timezone or lat/lon)
#     # regions = df['tz'].unique()
#     # if regions is not None and len(regions) > 0:
#         # for region in regions:
#             # airports_in_region = df[df['tz'] == region]
#             # if not airports_in_region.empty:
#                 # airports_in_region.to_csv(os.path.join(output_dir, f'airports_in_{region.replace("/", "_")}.csv'), index=False)
#     # else:
#         Fallback: categorize by lat/lon bounds if no timezone data is available
#         # regions = {
#             # 'Region1': {'min_lat': -10.0, 'max_lat': 10.0, 'min_lon': 120.0, 'max_lon': 150.0},
#             # 'Region2': {'min_lat': -20.0, 'max_lat': 0.0, 'min_lon': 100.0, 'max_lon': 140.0},
#             Add more regions as needed
#         # }
#         # for region, bounds in regions.items():
#             # airports_in_region = df[
#                 # (df['latitude'] >= bounds['min_lat']) & (df['latitude'] <= bounds['max_lat']) &
#                 # (df['longitude'] >= bounds['min_lon']) & (df['longitude'] <= bounds['max_lon'])
#             # ]
#             # if not airports_in_region.empty:
#                 # airports_in_region.to_csv(os.path.join(output_dir, f'airports_in_{region}.csv'), index=False)
#     # 
#     4. Airport Statistics
#     # airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     # airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     # airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
#     # 
#     5. Airport Density by Region (Dynamic regions)
#     # def determine_region(lat, lon):
#         # if -10 <= lat <= 0 and 120 <= lon <= 130:
#             # return 'Region 1'
#         # elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             # return 'Region 2'
#         # else:
#             # return 'Other'
# # 
#     # df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     # density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     # density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)
#     # 
#     6. Airports by Country with Distance Calculation
#     # def calculate_distances(df, country):
#         # df_country = df[df['country'] == country]
#         # if df_country.empty:
#             # return df_country
#         # 
#         # df_country['nearest_airport'] = None
#         # df_country['distance'] = None
#         # 
#         # for index, row in df_country.iterrows():
#             # distances = df_country.apply(
#                 # lambda x: haversine(row['latitude'], row['longitude'], x['latitude'], x['longitude']),
#                 # axis=1
#             # )
#             # nearest_idx = distances.nsmallest(2).idxmax()  # To avoid 0 distance (itself)
#             # df_country.at[index, 'nearest_airport'] = df_country.loc[nearest_idx, 'iata']
#             # df_country.at[index, 'distance'] = distances.loc[nearest_idx]
#         # 
#         # return df_country
# # 
#     # countries = df['country'].unique()
#     # for country in countries:
#         # country_airports_with_distances = calculate_distances(df, country)
#         # if not country_airports_with_distances.empty:
#             # country_airports_with_distances.to_csv(os.path.join(output_dir, f'airports_{country}_with_distances.csv'), index=False)
# # 
#     # print("Analytics have been completed and saved.")
# # 
# # perform_analytics_task = PythonOperator(
#     # task_id='perform_analytics',
#     # python_callable=perform_analytics,
#     # dag=dag,
# # )
# # 
# # 
# # 
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# from math import radians, cos, sin, sqrt, atan2
# import os

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 20),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'airports_analysis',
#     default_args=default_args,
#     description='A DAG to perform various analytics on airport data',
#     schedule_interval='0 0 * * *', 
# )

# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # Radius of the Earth in km
#     dlat = radians(lat2 - lat1)
#     dlon = radians(lon2 - lon1)  # Fixed typo: lon2 - lon1 instead of lat2 - lon1
#     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     distance = R * c
#     return distance

# def perform_analytics():
#     # Define output directory
#     output_dir = '/home/boss/airflow/dags/analytics/'
    
#     # Create the directory if it does not exist
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
    
#     # Load data
#     file_path = '/home/boss/airflow/dags/airports.dat'
#     df = pd.read_csv(file_path, header=None, names=[
#         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     ])
    
#     # Task 1: Distance Calculation (example airports)
#     airport1 = df[df['iata'] == 'GKA'].iloc[0]
#     airport2 = df[df['iata'] == 'MAG'].iloc[0]
#     distance = haversine(
#         airport1['latitude'], airport1['longitude'],
#         airport2['latitude'], airport2['longitude']
#     )
#     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
    
#     # Task 2: Airports by Country
#     airports_by_country = df.groupby('country').size().reset_index(name='count')
#     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
#     print("Saved airports_by_country.csv")
    
#     # Task 3: Airports within a Region (example region)
#     min_lat, max_lat, min_lon, max_lon = -10, 10, 120, 150
#     airports_in_region = df[
#         (df['latitude'] >= min_lat) & (df['latitude'] <= max_lat) &
#         (df['longitude'] >= min_lon) & (df['longitude'] <= max_lon)
#     ]
#     airports_in_region.to_csv(os.path.join(output_dir, 'airports_in_region.csv'), index=False)
#     print("Saved airports_in_region.csv")
    
#     # Task 4: Airport Statistics
#     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
#     print("Saved airport_stats_by_country.csv")
    
#     # Task 5: Airport Density by Region (example regions)
#     def determine_region(lat, lon):
#         if -10 <= lat <= 0 and 120 <= lon <= 130:
#             return 'Region 1'
#         elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             return 'Region 2'
#         else:
#             return 'Other'

#     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)
#     print("Saved airport_density_by_region.csv")
    
#     # Task 6: Airports by Country with Distance Calculation
#     def calculate_distances(df, country):
#         df_country = df[df['country'] == country].copy()  # Make a copy to avoid SettingWithCopyWarning
    
#         df_country['nearest_airport'] = None
#         df_country['distance'] = None
    
#         for index, row in df_country.iterrows():
#             distances = df_country.apply(
#                 lambda x: haversine(row['latitude'], row['longitude'], x['latitude'], x['longitude']),
#                 axis=1
#             )
#             nearest_idx = distances.idxmin()
            
#             # Use .loc for assignment to avoid warnings
#             df_country.loc[index, 'nearest_airport'] = df_country.loc[nearest_idx, 'iata']
#             df_country.loc[index, 'distance'] = distances.min()
        
#         return df_country
    
#     countries = df['country'].unique()
#     for country in countries:
#         country_dir = os.path.join(output_dir, f'airports_in_{country}')
#         if not os.path.exists(country_dir):
#             os.makedirs(country_dir)
        
#         country_airports_with_distances = calculate_distances(df, country)
#         country_airports_with_distances.to_csv(os.path.join(country_dir, f'airports_{country}_with_distances.csv'), index=False)
#         print(f"Saved airports_{country}_with_distances.csv in {country_dir}")

#     print("Analytics have been completed and saved.")

# perform_analytics_task = PythonOperator(
#     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )
#--------------------------------------------------BELOW-------
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# from math import radians, cos, sin, sqrt, atan2
# import os

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 20),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'airports_analysis',
#     default_args=default_args,
#     description='A DAG to perform various analytics on airport data',
#     schedule_interval='0 0 * * *', 
# )

# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # Radius of the Earth in km
#     dlat = radians(lat2 - lat1)
#     dlon = radians(lon2 - lon1)
#     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     distance = R * c
#     return distance

# def perform_analytics():
#     # Define output directory
#     output_dir = '/home/boss/airflow/dags/analytics/'
    
#     # Create the directory if it does not exist
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
    
#     # Load data
#     file_path = '/home/boss/airflow/dags/airports.dat'
#     df = pd.read_csv(file_path, header=None, names=[
#         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     ])
    
#     # Task 1: Distance Calculation (example airports)
#     airport1 = df[df['iata'] == 'GKA'].iloc[0]
#     airport2 = df[df['iata'] == 'MAG'].iloc[0]
#     distance = haversine(
#         airport1['latitude'], airport1['longitude'],
#         airport2['latitude'], airport2['longitude']
#     )
#     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
    
#     # Task 2: Airports by Country
#     airports_by_country = df.groupby('country').size().reset_index(name='count')
#     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
#     print("Saved airports_by_country.csv")
    
#     # Task 3: Airports within a Region for all countries
#     regions = {
#         'Region1': {'min_lat': -10.0, 'max_lat': 10.0, 'min_lon': 120.0, 'max_lon': 150.0},
#         'Region2': {'min_lat': -20.0, 'max_lat': 0.0, 'min_lon': 100.0, 'max_lon': 140.0},
#         # Add more regions as needed
#     }
    
#     for country in df['country'].unique():
#         country_dir = os.path.join(output_dir, f'airports_in_{country}')
#         if not os.path.exists(country_dir):
#             os.makedirs(country_dir)
        
#         country_df = df[df['country'] == country]
#         for region_name, bounds in regions.items():
#             airports_in_region = country_df[
#                 (country_df['latitude'] >= bounds['min_lat']) & (country_df['latitude'] <= bounds['max_lat']) &
#                 (country_df['longitude'] >= bounds['min_lon']) & (country_df['longitude'] <= bounds['max_lon'])
#             ]
#             if not airports_in_region.empty:
#                 airports_in_region.to_csv(os.path.join(country_dir, f'airports_in_{region_name}.csv'), index=False)
#                 print(f"Saved airports_in_{region_name}.csv for {country}")
    
#     # Task 4: Airport Statistics
#     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
#     print("Saved airport_stats_by_country.csv")
    
#     # Task 5: Airport Density by Region (example regions)
#     def determine_region(lat, lon):
#         if -10 <= lat <= 0 and 120 <= lon <= 130:
#             return 'Region 1'
#         elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             return 'Region 2'
#         else:
#             return 'Other'

#     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)
#     print("Saved airport_density_by_region.csv")
    
#     # Task 6: Airports by Country with Distance Calculation
#     def calculate_distances(df, country):
#         df_country = df[df['country'] == country].copy()  # Make a copy to avoid SettingWithCopyWarning
    
#         df_country['nearest_airport'] = None
#         df_country['distance'] = None
    
#         for index, row in df_country.iterrows():
#             distances = df_country.apply(
#                 lambda x: haversine(row['latitude'], row['longitude'], x['latitude'], x['longitude']),
#                 axis=1
#             )
#             nearest_idx = distances.idxmin()
            
#             # Use .loc for assignment to avoid warnings
#             df_country.loc[index, 'nearest_airport'] = df_country.loc[nearest_idx, 'iata']
#             df_country.loc[index, 'distance'] = distances.min()
        
#         return df_country
    
#     for country in df['country'].unique():
#         country_dir = os.path.join(output_dir, f'airports_in_{country}')
#         if not os.path.exists(country_dir):
#             os.makedirs(country_dir)
        
#         country_airports_with_distances = calculate_distances(df, country)
#         country_airports_with_distances.to_csv(os.path.join(country_dir, f'airports_{country}_with_distances.csv'), index=False)
#         print(f"Saved airports_{country}_with_distances.csv in {country_dir}")

#     print("Analytics have been completed and saved.")

# perform_analytics_task = PythonOperator(
#     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )
#--------------------UPPAR--------------------

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# from math import radians, cos, sin, sqrt, atan2
# import os

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 20),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'airports_analysis',
#     default_args=default_args,
#     description='A DAG to perform various analytics on airport data',
#     schedule_interval='0 0 * * *', 
# )

# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # Radius of the Earth in km
#     dlat = radians(lat2 - lat1)
#     dlon = radians(lon2 - lon1)
#     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     distance = R * c
#     return distance

# def perform_analytics():
#     # Define output directory
#     output_dir = '/home/boss/airflow/dags/analytics/'
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
    
#     # Load data
#     file_path = '/home/boss/airflow/dags/airports.dat'
#     df = pd.read_csv(file_path, header=None, names=[
#         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     ])
    
#     # Task 1: Distance Calculation (example airports)
#     airport1 = df[df['iata'] == 'GKA'].iloc[0]
#     airport2 = df[df['iata'] == 'MAG'].iloc[0]
#     distance = haversine(
#         airport1['latitude'], airport1['longitude'],
#         airport2['latitude'], airport2['longitude']
#     )
#     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
    
#     # Task 2: Airports by Country
#     airports_by_country = df.groupby('country').size().reset_index(name='count')
#     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
    
#     # Task 3: Airports Statistics
#     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
    
#     # Task 4: Airport Density by Region
#     def determine_region(lat, lon):
#         if -10 <= lat <= 0 and 120 <= lon <= 130:
#             return 'Region 1'
#         elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             return 'Region 2'
#         else:
#             return 'Other'

#     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)

#     print("Analytics have been completed and saved.")

# perform_analytics_task = PythonOperator(
#     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )

#----------- scheduler-----------------
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# from math import radians, cos, sin, sqrt, atan2
# import os
# import json
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 20),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Initialize the DAG
# dag = DAG(
#     'airports_analysis',
#     default_args=default_args,
#     description='A DAG to perform various analytics on airport data',
#     schedule_interval='0 */2 * * *',  # Run every 2 hours
# )

# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # Radius of the Earth in km
#     dlat = radians(lat2 - lat1)
#     dlon = radians(lon2 - lon1)
#     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     distance = R * c
#     return distance

# def upload_to_google_drive(file_name, folder_id):
#     SCOPES = ['https://www.googleapis.com/auth/drive.file']
#     creds = None

#     # Load credentials from file or initiate OAuth flow
#     if os.path.exists('/home/boss/airflow/dags/token.json'):
#         creds = Credentials.from_authorized_user_file('/home/boss/airflow/dags/token.json', SCOPES)
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_config(json.load(open('/home/boss/airflow/dags/credentials.json')), SCOPES)
#             creds = flow.run_local_server(port=0)
#         # Save the credentials for the next run
#         with open('/home/boss/airflow/dags/token.json', 'w') as token:
#             token.write(creds.to_json())

#     # Build the Drive API service
#     service = build('drive', 'v3', credentials=creds)

#     # Delete the old file if it exists
#     file_id = find_file_id(service, folder_id, file_name)
#     if file_id:
#         service.files().delete(fileId=file_id).execute()
#         print(f"Deleted old file: {file_name}")

#     # Upload the new file
#     file_metadata = {
#         'name': file_name,
#         'parents': [folder_id]
#     }
#     media = MediaFileUpload(file_name, mimetype='text/csv')
#     service.files().create(
#         body=file_metadata,
#         media_body=media,
#         fields='id'
#     ).execute()
#     print(f"Uploaded new file: {file_name}")

# def find_file_id(service, folder_id, file_name):
#     # List files in the folder and find the file ID
#     results = service.files().list(q=f"'{folder_id}' in parents and name='{file_name}'",
#                                    fields="files(id, name)").execute()
#     items = results.get('files', [])
#     if items:
#         return items[0]['id']
#     return None

# def perform_analytics():
#     # Define output directory
#     output_dir = '/home/boss/airflow/dags/analytics/'
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
    
#     # Load data
#     file_path = '/home/boss/airflow/dags/airports.dat'
#     df = pd.read_csv(file_path, header=None, names=[
#         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     ])
    
#     # Task 1: Distance Calculation (example airports)
#     airport1 = df[df['iata'] == 'GKA'].iloc[0]
#     airport2 = df[df['iata'] == 'MAG'].iloc[0]
#     distance = haversine(
#         airport1['latitude'], airport1['longitude'],
#         airport2['latitude'], airport2['longitude']
#     )
#     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")
    
#     # Task 2: Airports by Country
#     airports_by_country = df.groupby('country').size().reset_index(name='count')
#     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)
    
#     # Task 3: Airports Statistics
#     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)
    
#     # Task 4: Airport Density by Region
#     def determine_region(lat, lon):
#         if -10 <= lat <= 0 and 120 <= lon <= 130:
#             return 'Region 1'
#         elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             return 'Region 2'
#         else:
#             return 'Other'

#     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)

#     print("Analytics have been completed and saved.")

#     # Build the Drive API service
#     SCOPES = ['https://www.googleapis.com/auth/drive.file']
#     creds = Credentials.from_authorized_user_file('/home/boss/airflow/dags/token.json', SCOPES)
#     service = build('drive', 'v3', credentials=creds)

#     # Delete old files and upload new files
#     folder_id = '1Np-mAqeVRfClV5Qtm4udkSV9Lz_0PLrx'  # Your specific folder ID
#     for file_name in os.listdir(output_dir):
#         upload_to_google_drive(os.path.join(output_dir, file_name), folder_id)

# perform_analytics_task = PythonOperator(
#     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )

#-------------schedule----------
#down finale
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import os
# import pandas as pd
# import json
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from google.oauth2.credentials import Credentials
# from math import radians, cos, sin, sqrt, atan2

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 20),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Initialize the DAG
# dag = DAG(
#     'airports_analysis',
#     default_args=default_args,
#     description='A DAG to perform various analytics on airport data',
#     schedule_interval='0 */2 * * *',  # Run every 2 hours
#     catchup=False,  # Ensure that only the latest schedule is run
# )

# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # Radius of the Earth in km
#     dlat = radians(lat2 - lat1)
#     dlon = radians(lon2 - lon1)
#     a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     distance = R * c
#     return distance

# def perform_analytics():
#     output_dir = '/home/boss/airflow/dags/analytics/'
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)

#     file_path = '/home/boss/airflow/dags/airports.dat'
#     df = pd.read_csv(file_path, header=None, names=[
#         'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
#         'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
#     ])

#     airport1 = df[df['iata'] == 'GKA'].iloc[0]
#     airport2 = df[df['iata'] == 'MAG'].iloc[0]
#     distance = haversine(
#         airport1['latitude'], airport1['longitude'],
#         airport2['latitude'], airport2['longitude']
#     )
#     print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")

#     airports_by_country = df.groupby('country').size().reset_index(name='count')
#     airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)

#     airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
#     airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
#     airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)

#     def determine_region(lat, lon):
#         if -10 <= lat <= 0 and 120 <= lon <= 130:
#             return 'Region 1'
#         elif 0 <= lat <= 10 and 130 <= lon <= 140:
#             return 'Region 2'
#         else:
#             return 'Other'

#     df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
#     density_by_region = df.groupby('region').size().reset_index(name='airport_count')
#     density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)

#     print("Analytics have been completed and saved.")

#     SCOPES = ['https://www.googleapis.com/auth/drive.file']
#     creds = Credentials.from_authorized_user_file('/home/boss/airflow/dags/token.json', SCOPES)
#     service = build('drive', 'v3', credentials=creds)

#     folder_id = '1Np-mAqeVRfClV5Qtm4udkSV9Lz_0PLrx'

#     def delete_old_files():
#         results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)").execute()
#         items = results.get('files', [])
#         for item in items:
#             service.files().delete(fileId=item['id']).execute()
#         print("Old files deleted.")

#     delete_old_files()

#     for file_name in os.listdir(output_dir):
#         file_path = os.path.join(output_dir, file_name)
#         file_metadata = {
#             'name': file_name,
#             'parents': [folder_id]
#         }
#         media = MediaFileUpload(file_path, mimetype='text/csv')
#         service.files().create(
#             body=file_metadata,
#             media_body=media,
#             fields='id'
#         ).execute()
#         print(f"Uploaded {file_name} to Google Drive.")

# perform_analytics_task = PythonOperator(
#     task_id='perform_analytics',
#     python_callable=perform_analytics,
#     dag=dag,
# )
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from math import radians, cos, sin, sqrt, atan2

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'airports_analysis',
    default_args=default_args,
    description='A DAG to perform various analytics on airport data',
    schedule_interval='0 */2 * * *',  # Run every 2 hours
    catchup=False,  # Ensure that only the latest schedule is run
)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def perform_analytics():
    output_dir = '/home/boss/airflow/dags/analytics/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = '/home/boss/airflow/dags/airports.dat'
    df = pd.read_csv(file_path, header=None, names=[
        'id', 'name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude',
        'altitude', 'timezone_offset', 'dst', 'tz', 'type', 'source'
    ])

    # 1. Calculate the distance between two specific airports
    airport1 = df[df['iata'] == 'MAA'].iloc[0]
    airport2 = df[df['iata'] == 'MAG'].iloc[0]
    distance = haversine(
        airport1['latitude'], airport1['longitude'],
        airport2['latitude'], airport2['longitude']
    )
    print(f"Distance between {airport1['iata']} and {airport2['iata']}: {distance:.2f} km")

    # 2. Airports by Country
    airports_by_country = df.groupby('country').size().reset_index(name='airport_count')
    airports_by_country.to_csv(os.path.join(output_dir, 'airports_by_country.csv'), index=False)

    # 3. Average Altitude by Country
    airport_stats = df.groupby('country').agg({'altitude': 'mean'}).reset_index()
    airport_stats.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
    airport_stats.to_csv(os.path.join(output_dir, 'airport_stats_by_country.csv'), index=False)

    # 4. Top 10 Countries with the Most Airports
    top10_countries = airports_by_country.nlargest(10, 'airport_count')
    top10_countries.to_csv(os.path.join(output_dir, 'top10_airport_countries.csv'), index=False)

    # 5. Regional Airport Density
    def determine_region(lat, lon):
        if -10 <= lat <= 0 and 120 <= lon <= 130:
            return 'Region 1'
        elif 0 <= lat <= 10 and 130 <= lon <= 140:
            return 'Region 2'
        else:
            return 'Other'

    df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude']), axis=1)
    density_by_region = df.groupby('region').size().reset_index(name='airport_count')
    density_by_region.to_csv(os.path.join(output_dir, 'airport_density_by_region.csv'), index=False)

    # 6. Average Altitude by Region
    avg_altitude_by_region = df.groupby('region').agg({'altitude': 'mean'}).reset_index()
    avg_altitude_by_region.rename(columns={'altitude': 'avg_altitude'}, inplace=True)
    avg_altitude_by_region.to_csv(os.path.join(output_dir, 'avg_altitude_by_region.csv'), index=False)

    print("Analytics have been completed and saved.")

    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = Credentials.from_authorized_user_file('/home/boss/airflow/dags/token.json', SCOPES)
    service = build('drive', 'v3', credentials=creds)

    folder_id = '1Np-mAqeVRfClV5Qtm4udkSV9Lz_0PLrx'

    def delete_old_files():
        results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)").execute()
        items = results.get('files', [])
        for item in items:
            service.files().delete(fileId=item['id']).execute()
        print("Old files deleted.")

    delete_old_files()

    for file_name in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file_name)
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='text/csv')
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"Uploaded {file_name} to Google Drive.")

perform_analytics_task = PythonOperator(
    task_id='perform_analytics',
    python_callable=perform_analytics,
    dag=dag,
)
