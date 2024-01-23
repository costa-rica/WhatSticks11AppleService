import os
from ws_models import sess, OuraSleepDescriptions
import pandas as pd
import json
from common.config_and_logger import config, logger_apple

def create_data_source_object_json_file(user_id):
    logger_apple.info(f"- WSAS creating data source object file for user: {user_id} -")

    list_data_source_objects = []

    # #get user's oura record count
    # # keys to data_source_object_oura must match WSiOS DataSourceObject
    # data_source_object_oura={}
    # data_source_object_oura['name']="Oura Ring"
    # record_count_oura = sess.query(OuraSleepDescriptions).filter_by(user_id=int(user_id)).all()
    # data_source_object_oura['recordCount']="{:,}".format(len(record_count_oura))
    # list_data_source_objects.append(data_source_object_oura)

    #get user's apple health record count
    # keys to data_source_object_apple_health must match WSiOS DataSourceObject
    data_source_object_apple_health={}
    data_source_object_apple_health['name']="Apple Health Data"
    # record_count_apple_health = sess.query(AppleHealthQuantityCategory).filter_by(user_id=current_user.id).all()
    
    # user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    # pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    # df_apple_health = pd.read_pickle(pickle_data_path_and_name)
    apple_health_record_count, earliest_date_str = get_apple_health_count_date(user_id)
    data_source_object_apple_health['recordCount'] = apple_health_record_count
    data_source_object_apple_health['earliestRecordDate'] = earliest_date_str
    # data_source_object_apple_health['earliestRecordDate']="{:,}".format(len(df_apple_health))
    list_data_source_objects.append(data_source_object_apple_health)

    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    user_data_source_json_file_name = f"data_source_list_for_user_{int(user_id):04}.json"

    json_data_path_and_name = os.path.join(config.DATA_SOURCE_FILES_DIR, user_data_source_json_file_name)
    logger_apple.info(f"Writing file name: {json_data_path_and_name}")
    with open(json_data_path_and_name, 'w') as file:
        json.dump(list_data_source_objects, file)

def get_apple_health_count_date(user_id):
    user_apple_qty_cat_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_workouts_dataframe.pkl"
    pickle_data_path_and_name_qty_cat = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_qty_cat_dataframe_pickle_file_name)
    pickle_data_path_and_name_workouts = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)
    df_apple_qty_cat = pd.read_pickle(pickle_data_path_and_name_qty_cat)
    if os.path.exists(pickle_data_path_and_name_workouts):
        df_apple_workouts = pd.read_pickle(pickle_data_path_and_name_workouts)
    else:
        df_apple_workouts = pd.DataFrame()

    # get count of qty_cat and workouts
    apple_health_record_count = "{:,}".format(len(df_apple_qty_cat) + len(df_apple_workouts))

    # Convert startDate to datetime
    df_apple_qty_cat['startDate'] = pd.to_datetime(df_apple_qty_cat['startDate'])
    earliest_date_qty_cat = df_apple_qty_cat['startDate'].min()

    if not os.path.exists(pickle_data_path_and_name_workouts):
        earliest_date_str = earliest_date_qty_cat.strftime('%b %d, %Y')
        return apple_health_record_count, earliest_date_str

    df_apple_workouts['startDate'] = pd.to_datetime(df_apple_workouts['startDate'])
    earliest_date_workouts = df_apple_workouts['startDate'].min()
    earliest_date_str = ""
    if earliest_date_workouts < earliest_date_qty_cat:
        earliest_date_str = earliest_date_workouts.strftime('%b %d, %Y')
    else:
        earliest_date_str = earliest_date_qty_cat.strftime('%b %d, %Y')

    return apple_health_record_count, earliest_date_str

