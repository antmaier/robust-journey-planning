# # Inference Notebook

# +
# !pip install pyarrow

from datetime import datetime, time
from hdfs3 import HDFileSystem
from os.path import join
import pandas as pd
from queue import PriorityQueue
import random
from scipy.stats import norm
import numpy as np
# -

# ## Loading functions

# The functions in the cells below use the HDFS to load the partitioned graph in chunks. We assume that the time of the trip lies between 6 a.m. and 11 p.m. For a trip at time HH:MM, we only consider the hourly partitions of HH-2, HH-1, and HH.

# +
# Creating the functions to load the data
network_location = '/user/anmaier/schedule_network.orc'
delay_stats_location = '/user/amenon/df_minutes_partitioned.orc'
stops_information_path = '/user/anmaier/df_stops_near.orc'

hdfs = HDFileSystem(user='ebouille')
def read_hdfs(path, fmt='parquet'):
    files = hdfs.glob(path)
    df = pd.DataFrame()
    for file in files:
        with hdfs.open(file) as f:
            if fmt == 'orc':
                if not file.endswith('.orc'):
                    continue
                df = pd.concat([df, pd.read_orc(f)])
            else:
                df = pd.concat([df, pd.read_parquet(f)])
    return df

def read_partitions(path, hours, fmt='parquet'):
    df = pd.DataFrame()
    for hour in hours:
        df = pd.concat([df, read_hdfs(join(path, f"hour={hour}/*.{fmt}"), fmt)])
    return df

def load_partitioned_data(path, arrival_time, verbose=False, fmt='orc'):
    max_hour = arrival_time.hour
    assert max_hour >= 6 and max_hour <= 23
    if verbose:
        print('Considering hours:', [str(max_hour-2), str(max_hour-1), str(max_hour)])
    df = read_partitions(f"{path}", hours=[str(max_hour-2), str(max_hour-1), str(max_hour)], fmt=fmt)
    return df

def load_network(arrival_time, verbose=False, fmt='orc'):
    df = load_partitioned_data(network_location, arrival_time, verbose, fmt)
    df['dst_timestamp'] = pd.to_datetime(df["dst_timestamp"].dt.strftime('%H:%M'))
    df['src_timestamp'] = pd.to_datetime(df["src_timestamp"].dt.strftime('%H:%M'))
    return df

def load_delay_stats(arrival_time, verbose=False, fmt='orc', network=None):
    df = load_partitioned_data(delay_stats_location, arrival_time, verbose, fmt)
    # if network is provided, filter to only keep rows with trip id which is in the network
    if network is not None:
        network_trip_ids = list(network['trip_id'].unique())
        df = df[df['trip_id'].isin(network_trip_ids)]
    return df

def load_stops_data():
    stops_df = read_hdfs(stops_information_path, fmt='orc')
    return stops_df


# +
# load the stops data when the script is loaded as a module, and make a function to retrieve this df since it doesn't change
stops_data = load_stops_data()

def get_stops_data():
    return stops_data


# -

# ## Utility functions

# +
def timestamp_to_mins(time):
    return time.hour * 60 + time.minute

def get_arrival_delay_stats(delay_stats, stop_name, trip_id, arrival_mins):
    stats = delay_stats[(delay_stats['stop_name'] == stop_name) & (delay_stats['trip_id'] == trip_id) & (delay_stats['scheduled_arrival_minutes'] == arrival_mins)]
    # if no entry found, return None
    if stats.empty:
        #print(f"arrival delay stats missing for {stop_name}, {trip_id}, {arrival_mins}")
        return None, None
    return stats['mean_arrival_time'].iloc[0], stats['std_arrival_time'].iloc[0]

def get_departure_delay_stats(delay_stats, stop_name, trip_id, departure_mins):
    stats = delay_stats[(delay_stats['stop_name'] == stop_name) & (delay_stats['trip_id'] == trip_id) & (delay_stats['scheduled_departure_minutes'] == departure_mins)]
    # if no entry found, return None
    if stats.empty:
        #print(f"departure delay stats missing for {stop_name}, {trip_id}, {departure_mins}")
        return None, None
    return stats['mean_departure_time'].iloc[0], stats['std_departure_time'].iloc[0]

def final_arrival_probability(arrival_mins, delay_stats, stop_name, trip_id, sched_arrival_mins, missing_prob_handle_method='remove'):
    mean_arrival_time, std_arrival_time = get_arrival_delay_stats(delay_stats, stop_name, trip_id, sched_arrival_mins)
    # if no delay stats were found, set probability to 1 or 0 depending on missing_prob_handle_method (either keep or remove)
    if mean_arrival_time is None:
        if missing_prob_handle_method == 'keep':
            return 1
        return 0
    return norm.cdf(arrival_mins, mean_arrival_time, std_arrival_time + 1e-8)

# calculate probability of making a successful transfer with a specific transit duration
def calc_probability(transit_duration, mean_arrival_time, std_arrival_time, mean_departure_time, std_departure_time, missing_prob_handle_method='remove'):
    # if either delay stats were not found, set probability to 1 or 0 depending on if we want to keep/remove missing probs
    if mean_arrival_time is None or mean_departure_time is None:
        if missing_prob_handle_method == 'keep':
            return 1
        return 0
    diff_std = np.sqrt(std_arrival_time**2 + std_departure_time**2)
    diff_mean = mean_departure_time - mean_arrival_time
    p = norm.cdf(transit_duration, loc=diff_mean, scale=diff_std+1e-8)
    return 1 - float(p)

def transfer_success_probability(delay_stats, transit_duration, arr_trip_id, arr_stop_name, arr_sched_time, dep_trip_id, dep_stop_name, dep_sched_time, missing_prob_handle_method='remove'):
    mean_arrival_time, std_arrival_time = get_arrival_delay_stats(delay_stats, arr_stop_name, arr_trip_id, timestamp_to_mins(arr_sched_time))
    mean_departure_time, std_departure_time = get_departure_delay_stats(delay_stats, dep_stop_name, dep_trip_id, timestamp_to_mins(dep_sched_time))
    return calc_probability(transit_duration, mean_arrival_time, std_arrival_time, mean_departure_time, std_departure_time, missing_prob_handle_method)


# -

# ## Search implementation

# +
class Path:
    """
    A class that stores the possible paths along with some overall statistics (also used for ordering on duration in the priorityqueue)
    """
    def __init__(self, path_list, duration, probability, walk_dist, num_transfers):
        self.path_list = pd.concat(path_list, axis=1, ignore_index=True).transpose()
        #print(self.path_list)
        self.duration = duration
        self.probability = probability
        self.walk_dist = walk_dist
        self.num_transfers = num_transfers
    
    def __lt__(self, other):
         # prioritises shorter time (i.e. latest departure), then less walking then less transfers. better route will be >
        return not (self.duration, self.walk_dist, self.num_transfers) < (other.duration, other.walk_dist, other.num_transfers)
    
def get_supernode_edges(df_edges, delay_stats, arrival_station, arrival_time, missing_prob_handle_method='remove'):
    """
    Adds a super node to all entries of the arrival station
    """
    # find connections which end up in the arrival station. disallow final walking edge (must arrive at the station via a trip)
    arr_stops = df_edges[(df_edges['dst_stop_name'] == arrival_station) & (df_edges['dst_timestamp'] < arrival_time) & (df_edges['route_desc'] != 'walking')]\
                .drop_duplicates(subset=['dst_stop_name', 'dst_timestamp'])\
                .sort_values(by='dst_timestamp', ascending=False).copy()
    
    # Probabilities of making the last stop in time
    arrival_mins = timestamp_to_mins(arrival_time)
    probs = []
    for _, row in arr_stops.iterrows():
        prob = final_arrival_probability(arrival_mins, delay_stats, row['dst_stop_name'], row['trip_id'], timestamp_to_mins(row['dst_timestamp']), missing_prob_handle_method)
        probs.append(prob)
    arr_stops['probability'] = probs
    
    # print(arr_stops['probability'].hist())
    arr_stops[['src_timestamp', 'src_stop_name']] = arr_stops[['dst_timestamp', 'dst_stop_name']]
    arr_stops['dst_timestamp'] = arrival_time
    arr_stops['dst_stop_name'] = 'Arrival'
    arr_stops['route_desc'] = 'early_arrival'
    #arr_stops['trip_id'] = ''
    arr_stops['distance'] = 0
    # minutes early before desired arrival time
    arr_stops['duration'] = (arrival_time - arr_stops['src_timestamp']).dt.total_seconds() / 60.0
    # We only assume that you want to arrive 30 min before you actually wanna be there (worst-case)
    arr_stops = arr_stops[arr_stops['duration'] < 30]
    
    return arr_stops

def recursive_search(delay_stats, depth, df_edges, stop_name, timestamp, target_stop_name, min_probability, max_transfers, max_duration, cur_duration, cur_probability, 
                     cur_walk_dist, cur_transfers, cur_trip_id, prev_route_desc, prev_trip_id, cur_path, seen_stops, valid_paths, top_n, verbose=False, debugging=False,
                     missing_prob_handle_method='remove'):    
    """
    This is the search algorithm. It does not do DFS completely, but, at each level explores the trips with the lowest "duration" first
    """
    if valid_paths.qsize() == 1:
        return
    #if verbose:
        #print(stop_name, timestamp, cur_transfers, prev_route_desc)
    #    print()
        #print([d['route_desc'] for d in cur_path], cur_transfers)
        
    # if the max number of transfers is exceeded, stop exploring this branch
    if cur_transfers > max_transfers:
        return
    if cur_probability < min_probability:
        return
        
    # if the desired depature station is reach, add the path
    if stop_name == target_stop_name:
        # if last edge is a walking edge, then this was incorrectly counted as a transfer to remedy this
        # also in this case you don't have to time the walk for a particular connection, so we can adjut the times and durations such that you walk directly there
        if prev_route_desc == 'walking':
            cur_transfers -= 1
            cur_path[0]['src_timestamp'] = cur_path[0]['src_timestamp'] - pd.Timedelta(minutes=cur_path[0]['walking_duration'])
            cur_duration -= cur_path[0]['duration'] - cur_path[0]['walking_duration']
            cur_path[0]['duration'] = cur_path[0]['walking_duration']
            
        # if the current pqueue is full, remove the worst route to make space for the new one
        # otherwise trying to add when the queue is full raises an exception (which just causes the search to hang instead of throwing an error)
        if valid_paths.full():
            valid_paths.get()
        valid_paths.put(Path(cur_path, cur_duration, cur_probability, cur_walk_dist, cur_transfers))
        if verbose:
            print(f'Found a valid path of duration {cur_duration} with probability {cur_probability} and {cur_transfers} transfers')
        if debugging:
            print('Debugging:', [i.duration for i in valid_paths.queue])
        return
    
    # elif valid_paths.full():
    #     # found the req number of paths already
    #     return
    
    # print(stop_name, timestamp, cur_duration, cur_probability, cur_walk_dist, cur_path, seen_stops)
    
    # Get the predecessors of the stop_name. 
    valid_predecessor_edges = df_edges[(df_edges['dst_stop_name'] == stop_name) & (df_edges['dst_timestamp'] == timestamp)]  
    
    #print(valid_predecessor_edges)
        
    # Filter 1: Only valid edge orders. must be (walk)?[[(trip)(wait)]*(trip)?](walk)?]*[(trip)(wait)]*(trip)?]?early_arrival
    # cannot walk to final destination, and cannot walk after a walk. also cannot wait then walk/arrive since waiting is only btwn edges of the same trip
    if prev_route_desc == 'early_arrival' or prev_route_desc == 'walking':
        valid_predecessor_edges = valid_predecessor_edges[~(valid_predecessor_edges['route_desc'].isin(['walking', 'waiting']))]
    # can only have a waiting edge after a trip edge, and trip id must be the same
    elif prev_route_desc == 'waiting':
        valid_predecessor_edges = valid_predecessor_edges[valid_predecessor_edges['route_desc'].isin(['Bus', 'Zug', 'Tram'])]
        valid_predecessor_edges = valid_predecessor_edges[valid_predecessor_edges['trip_id'] == cur_trip_id]
    # a trip edge cannot come immediately after another trip edge. if a waiting edge comes after, it must have the same trip_id
    else:
        valid_predecessor_edges = valid_predecessor_edges[~(valid_predecessor_edges['route_desc'].isin(['Bus', 'Zug', 'Tram']))]
        valid_predecessor_edges = valid_predecessor_edges[~((valid_predecessor_edges['route_desc'] == 'waiting') & (valid_predecessor_edges['trip_id'] != cur_trip_id))]
    
    #print('filter 1:' , len(valid_predecessor_edges))
    # Filter 2: Consider only edges with a high enough probability
    # if we had a walking edge last the next one must be a trip edge, so if there is a previous trip id, we can get the delay stats for the prev and next trips
    if prev_route_desc == 'walking':
        if prev_trip_id is not None:
            # access the stop and time for the previous trip in the path (most recent edge is the walking edge so skip this, although we could use either)
            prev_stop_name, prev_sched_departure_time = cur_path[1]['src_stop_name'], cur_path[1]['src_timestamp']
            # for each possible transfer, calculate the prob of making it by walking
            probs = []
            for idx, predecessor_stop in valid_predecessor_edges.iterrows():
                next_trip_id, next_stop_name, next_sched_arrival_time = predecessor_stop['trip_id'], predecessor_stop['dst_stop_name'], predecessor_stop['dst_timestamp']
            
                prob = transfer_success_probability(delay_stats, predecessor_stop['walking_duration'], next_trip_id, next_stop_name, next_sched_arrival_time,
                                                    prev_trip_id, prev_stop_name, prev_sched_departure_time, missing_prob_handle_method)
                probs.append(prob)
                
            valid_predecessor_edges['probability'] = probs
            # filter if the prob will fall below robustness threhsold
            valid_predecessor_edges = valid_predecessor_edges[(valid_predecessor_edges['probability']*cur_probability >= min_probability)]
    #print('filter 2:' , len(valid_predecessor_edges)) 
    
    # Filter 3: Only consider walking edges such that < 500m is walked in total
    valid_predecessor_edges = valid_predecessor_edges[~((valid_predecessor_edges['route_desc'] == 'walking') & (valid_predecessor_edges['distance'] + cur_walk_dist > 500))]
    #print('filter 3:' , len(valid_predecessor_edges))
    
    # Filter 4: Get rid of paths that are already worse if we've filled our results buffer in terms of trip duration
    if valid_paths.qsize() == top_n:
        lowest_distance = valid_paths.queue[0].duration
        valid_predecessor_edges = valid_predecessor_edges[valid_predecessor_edges['duration'] + cur_duration < lowest_distance]    
    #print('filter 4:' , len(valid_predecessor_edges))
    
    # Filter 5: Cycles in terms of the previously visited stations unless its a waiting edge in which case this is allowed
    valid_predecessor_edges = valid_predecessor_edges[~((valid_predecessor_edges['route_desc'] != 'waiting') & (valid_predecessor_edges['src_stop_name'].isin(seen_stops)))]
    #print('filter 5:' , len(valid_predecessor_edges))
    
    # Filter 6: Consider only edges with low enough max duration
    # We also sort as a guiding heuristic in the search
    valid_predecessor_edges = valid_predecessor_edges[(valid_predecessor_edges['duration'] + cur_duration) <= max_duration].sort_values(by='duration', ascending=False).copy()
    #print('filter 6:' , len(valid_predecessor_edges))
    
    for idx, predecessor_stop in valid_predecessor_edges.iterrows():

        new_seen_stops = seen_stops | {predecessor_stop['src_stop_name']} #if predecessor_stop['route_desc'] != 'waiting' else seen_stops
        #if verbose:
        #    print("\t"*depth, f"Type:{predecessor_stop['route_desc']}, {predecessor_stop['src_stop_name']}", end='')
            
        recursive_search(delay_stats=delay_stats, depth=depth+1, df_edges=df_edges, stop_name=predecessor_stop['src_stop_name'], timestamp=predecessor_stop['src_timestamp'], 
                         target_stop_name=target_stop_name, min_probability=min_probability, max_transfers=max_transfers, max_duration=max_duration,
                         cur_duration=cur_duration + predecessor_stop['duration'], # New trip duration
                         cur_probability=predecessor_stop['probability']*cur_probability, # New probability
                         cur_walk_dist=cur_walk_dist+int(predecessor_stop['route_desc'] == 'walking')*predecessor_stop['distance'], # Added walking distance in case of a walking edge
                         cur_transfers=cur_transfers + (1 if predecessor_stop['route_desc'] == 'walking' else 0), # if theres a walking edge, add a transfer (TODO: wait->trip->wait->trip will count a transfer every time)
                         cur_trip_id=predecessor_stop['trip_id'],
                         prev_trip_id=(predecessor_stop['trip_id'] if predecessor_stop['trip_id']!='' else prev_trip_id),
                         prev_route_desc=predecessor_stop['route_desc'], # tracks the previous type of route
                         cur_path=[predecessor_stop] + cur_path, # Add the vertex to the path
                         seen_stops=new_seen_stops, valid_paths=valid_paths, top_n=top_n, verbose=verbose, debugging=debugging, 
                         missing_prob_handle_method=missing_prob_handle_method)

def run_search(departure_station, arrival_station, arrival_hour, arrival_minute, min_robustness, max_transfers=5, max_duration=120, top_n=5, 
               verbose=False, debugging=False, precomputed_edges=None, missing_prob_handle_method='remove'):
    """
    Loads the partitioned graph (if not already given), adds the supernode, performs the search, and returns the result
    """
    
    assert departure_station != arrival_station, "Departure station cannot be the same as arrival station"
    
    arrival_time = datetime.combine(datetime.today().date(), time(arrival_hour, arrival_minute)) # construct datetime object from hours and mins
    if precomputed_edges is not None:
        df_edges = precomputed_edges.copy()
    else:
        df_edges = load_network(arrival_time, verbose, fmt='orc')
        df_edges = df_edges[df_edges['duration'] >= 0]
        
    delay_stats = load_delay_stats(arrival_time, verbose=verbose, network=df_edges)

    supernode_edges = get_supernode_edges(df_edges, delay_stats, arrival_station, arrival_time, missing_prob_handle_method)
    search_edges = pd.concat([supernode_edges, df_edges], ignore_index=True)
    if verbose:
        print('Loaded the graph')     
        
    # Below we convert the columns in minutes to the datatime format we'll be using
    delay_stats['scheduled_arrival'] = pd.to_datetime(delay_stats['scheduled_arrival_minutes'], unit='m', origin=datetime.combine(datetime.today().date(), time(0, 0)))
    delay_stats['scheduled_departure'] = pd.to_datetime(delay_stats['scheduled_departure_minutes'], unit='m', origin=datetime.combine(datetime.today().date(), time(0, 0)))
           
    # Create a priorityqueue with maximum size = top_n
    valid_paths = PriorityQueue(maxsize=top_n)
            
    if verbose:
        print('Starting the search')

    recursive_search(delay_stats=delay_stats, depth=0, df_edges=search_edges, stop_name='Arrival', timestamp=arrival_time, target_stop_name=departure_station, 
                     min_probability=min_robustness, max_transfers=max_transfers, max_duration=max_duration, cur_duration=0.0, cur_probability=1.0, 
                     cur_walk_dist=0.0, cur_transfers=0, cur_trip_id='', prev_route_desc='early_arrival', prev_trip_id=None, cur_path=[], seen_stops=set(), 
                     valid_paths=valid_paths, top_n=top_n, verbose=verbose, debugging=debugging, missing_prob_handle_method=missing_prob_handle_method)
    
    valid_paths_list = []
    while not valid_paths.empty():
        path = valid_paths.get()
        valid_paths_list.append(path)
    
    return valid_paths_list



# -

# ## Runing the search

def run_example():

    # This is to get some statistics about possible paths:
    #df_edges = load_partitioned_data(arrival_time)

    #df_edges.groupby('src_stop_name').agg({'route_desc': 'count'}).sort_values(by='route_desc', ascending=False).head()
    
    # departure_station = 'Rüti ZH'
    # arrival_station = 'Bubikon'
    arrival_hour, arrival_minute = 11,50
    # ------------------------------------
    departure_station = 'Zürich, Seilbahn Rigiblick'
    arrival_station = 'Zürich, Kunsthaus'

    minimum_robustness = 0.8
    max_transfers = 5
    max_duration = 30
    top_n = 10
    

    # Some things:
    # - Exploring the whole tree is hard (without delay probs.)
    # - Optimization with top_n pruning can still be done
    valid_paths_list = run_search(departure_station, arrival_station, arrival_hour, arrival_minute, minimum_robustness, max_transfers, max_duration, top_n, verbose=True, missing_prob_handle_method='keep')
    
    for path in sorted(valid_paths_list, reverse=True):
        for idx, edge in path.path_list.iterrows():
            name, time, tp = edge['src_stop_name'], str(edge['src_timestamp']).split(" ")[-1], edge['route_desc']
            if idx == 0:
                print(f'({time})', end=' ')
            print(name, end=f' -- {tp} --> ')
            if idx+1 == len(path.path_list):
                print(f'{name} ({time})')

        print(f'Duration: {path.duration:<4}, Probability: {path.probability:<.2f}, Walking distance: {path.walk_dist:<4.4f}\n')
        
    return valid_paths_list

# +
# valid_paths = PriorityQueue(maxsize=5)
#a = run_example()

# arrival_time = datetime.combine(datetime.today().date(), time(11, 30)) # construct datetime object from hours and mins

# df_edges = load_network(arrival_time, True, fmt='orc')

# delay_stats = load_delay_stats(arrival_time, verbose=True, network=df_edges)
# -



