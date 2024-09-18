from typing import List

import hopsworks
import pandas as pd

from src.config import hopsworks_config as config

def push_value_to_feature_group(
    value: dict,
    feature_group_name: str,
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
):
    """
    Pushes the given `value` to the given `feature_group_name` in the Feature Store.

    Args:
        value (dict): The value to push to the Feature Store
        feature_group_name (str): The name of the Feature Group
        feature_group_version (int): The version of the Feature Group
        feature_group_primary_keys (List[str]): The primary key of the Feature Group
        feature_group_event_time (str): The event time of the Feature Group

    Returns:
        None
    """
    breakpoint()
    
    project = hopsworks.login(
        project=config.hopsworks_project_name,
        api_key=config.hopsworks_api_key,
    )

    feature_store = project.get_feature_store()

    feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        primary_key=feature_group_primary_keys,
        event_time=feature_group_event_time,
        online_enabled=True,

        # TODO: either as homework or I will show one example.
        # expectation_suite=expectation_suite_transactions,
    )

    # transform the value dict into a pandas DataFrame
    value_df = pd.DataFrame([value])

    # push the value to the Feature Store
    feature_group.insert(value_df)