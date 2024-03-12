# This is a sample Python script
#from dataconnection import DynamoDB, Iot
from agrilution_aws.dynamodb_api import DynamoDbApi
from agrilution_aws.iot_api import IotApi
from preprocessing import Preprocessing
from getusers import get_users
import pandas as pd

if __name__ == '__main__':
    dynamo = DynamoDbApi(table_name='purchased_subscriptions_prod',logger=None)
    # subscription data
    # check for scan table in data-connection
    subscription_df = dynamo.scan_table(TableName ='purchased_subscriptions_prod')
    subscription_df = pd.DataFrame(subscription_df)
    print(subscription_df)

    # preprocess the data
    # transform dict values to individual values and merge into one table
    preprocess = Preprocessing(subscription_df)
    col_list = ['address', 'base_subscription']
    converted_subscription_df = preprocess.convert_dict_to_columns(col_list)

    # merge = Preprocessing(subscription_df)
    # table_list = ['archive', 'recipe']
    # merged_tables = preprocess.merge_tables('archive','recipe')

    # get relevant and currently active plantcube objects from AWS IOT core
    #iot = IotApi(logger=None)
    #plantcube_things = iot.get_plantcubes()
    #print('plant cube things',plantcube_things)
    # current_recipe = preprocess.retrieve_current_recipe('d6472f5d-94f9-4a31-9a8e-ddc6744023d6')
    user_prod_df = dynamo.scan_table(TableName='user_profile_prod')
    user_prod_df = pd.DataFrame(user_prod_df)
    #print(user_prod_df.columns)
    plantcubes_with_users_df = preprocess.filter_user_profile_for_cubes_and_users(user_prod_df)
    plantcubes_with_users_df = pd.DataFrame(plantcubes_with_users_df)
    print(plantcubes_with_users_df.columns)

    users = get_users()
    print(users)
    
    cubes_users = preprocess.filter_plantcube_cognito_users(user_prod_df)
    print(cubes_users)
   



