import pandas as pd
import datetime

def calculate_distance_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    cumulative_distances = {}

    for index, row in df.iterrows():
        start_id, end_id, distance = row['start_id'], row['end_id'], row['distance']

        cumulative_distances.setdefault(start_id, {}).setdefault(end_id, 0)
        cumulative_distances[start_id][end_id] += distance

        cumulative_distances.setdefault(end_id, {}).setdefault(start_id, 0)
        cumulative_distances[end_id][start_id] += distance

    distance_matrix = pd.DataFrame(cumulative_distances).fillna(0)
    distance_matrix = distance_matrix.combine(distance_matrix.transpose(), max)

    for col in distance_matrix.columns:
        distance_matrix.at[col, col] = 0

    return distance_matrix



def unroll_distance_matrix(df: pd.DataFrame) -> pd.DataFrame:

    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    unrolled_distances = []

    for start_id, row in df.iterrows():
        for end_id, distance in row.items():
            if start_id != end_id:
                unrolled_distances.append({'id_start': start_id, 'id_end': end_id, 'distance': distance})

    return pd.DataFrame(unrolled_distances)


def find_ids_within_ten_percentage_threshold(df: pd.DataFrame, reference_id: int) -> pd.DataFrame:
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    reference_avg_distance = df[df['id_start'] == reference_id]['distance'].mean()

    lower_threshold = 0.9 * reference_avg_distance
    upper_threshold = 1.1 * reference_avg_distance

    filtered_ids = df.groupby('id_start')['distance'].mean().between(lower_threshold, upper_threshold).index

    return pd.DataFrame({'id_start': sorted(filtered_ids)})


def calculate_toll_rate(df: pd.DataFrame) -> pd.DataFrame:

    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    for vehicle_type, rate_coefficient in rate_coefficients.items():
        df[vehicle_type] = df['distance'] * rate_coefficient

    return df



def calculate_time_based_toll_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here

    weekday_discounts = {
        (datetime.time(0, 0, 0), datetime.time(10, 0, 0)): 0.8,
        (datetime.time(10, 0, 0), datetime.time(18, 0, 0)): 1.2,
        (datetime.time(18, 0, 0), datetime.time(23, 59, 59)): 0.8
    }
    weekend_discount = 0.7

    for index, row in df.iterrows():
        start_time, end_time = row['start_time'], row['end_time']

        if start_time.weekday() < 5:  # Weekday
            for time_range, discount_factor in weekday_discounts.items():
                if time_range[0] <= start_time <= time_range[1] and time_range[0] <= end_time <= time_range[1]:
                    df.at[index, 'moto'] *= discount_factor
                    df.at[index, 'car'] *= discount_factor
                    df.at[index, 'rv'] *= discount_factor
                    df.at[index, 'bus'] *= discount_factor
                    df.at[index, 'truck'] *= discount_factor
        else:  # Weekend
            df.at[index, 'moto'] *= weekend_discount
            df.at[index, 'car'] *= weekend_discount
            df.at[index, 'rv'] *= weekend_discount
            df.at[index, 'bus'] *= weekend_discount
            df.at[index, 'truck'] *= weekend_discount

    return df
