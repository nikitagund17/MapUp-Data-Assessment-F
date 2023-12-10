import pandas as pd


def generate_car_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    # Write your logic here
    matrix_df = df.pivot(index='id_1', columns='id_2', values='car')
    matrix_df = matrix_df.fillna(0)

    matrix_df.values[[range(len(matrix_df))]*2] = 0

    return matrix_df


def get_type_count(df: pd.DataFrame) -> dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Write your logic here
    df['car_type'] = pd.cut(df['car'],
                            bins=[float('-inf'), 15, 25, float('inf')],
                            labels=['low', 'medium', 'high'],
                            include_lowest=True)

   
    type_counts = df['car_type'].value_counts().to_dict()

    sorted_type_counts = dict(sorted(type_counts.items()))

    return sorted_type_counts


def get_bus_indexes(df: pd.DataFrame) -> list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # Write your logic here
    bus_mean = df['bus'].mean()
    bus_indexes = df[df['bus'] > 2 * bus_mean].index.tolist()
    sorted_bus_indexes = sorted(bus_indexes)
    return sorted_bus_indexes


def filter_routes(df: pd.DataFrame) -> list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Write your logic here
    route_avg_truck = df.groupby('route')['truck'].mean()

    filtered_routes = route_avg_truck[route_avg_truck > 7].index.tolist()

    
    sorted_filtered_routes = sorted(filtered_routes)

    return sorted_filtered_routes


def multiply_matrix(matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Write your logic here
    modified_matrix = matrix.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25)

    modified_matrix = modified_matrix.round(1)

    return modified_matrix



def time_check(df: pd.DataFrame) -> pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    # Write your logic here
    df['startTimestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'])

    df['endTimestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'])

    df['duration'] = df['endTimestamp'] - df['startTimestamp']

    completeness_check = df.groupby(['id', 'id_2']).apply(lambda group: (
        (group['startTimestamp'].min().time() == pd.Timestamp('00:00:00').time()) and
        (group['endTimestamp'].max().time() == pd.Timestamp('23:59:59').time()) and
        (group['startTimestamp'].min().day_name() == 'Monday') and
        (group['endTimestamp'].max().day_name() == 'Sunday') and
        (group['duration'].sum() >= pd.Timedelta(days=7))
    ))

    return completeness_check
