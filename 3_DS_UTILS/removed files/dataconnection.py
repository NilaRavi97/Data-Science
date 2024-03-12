import logging
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import time
import settings

class DynamoDB():
    """Establishes the connection to AWS DynamoDB backend
    """

    def __init__(self):
        """initializes the database connection
        """
        self.dynamo = boto3.session.Session(
            aws_access_key_id=settings.AWS_CUBE_DASHBOARD_ID_KEY,
            aws_secret_access_key=settings.AWS_CUBE_DASHBOARD_ACCESS_KEY,
            region_name=settings.AWS_REGION).resource('dynamodb')

    def query_table(self, table: str, limit: int = None, **kwargs):
        """this function allows to query AWS DynamoDB tables

        Args:
            table (str): the aws dynamo db table name
            limit (int, optional): sets limit of returned results. Defaults to None.

        Returns:
            (list): returns all matched elements as a list
        """
        # initialize connection to dynamo table
        table = self.dynamo.Table(table)
        # iterate through dynamo db
        result = []
        # initialize first query
        # catch provision throughput error
        retry_exceptions = ['ProvisionedThroughputExceededException', 'ThrottlingException']
        while True:
            try:
                response = table.query(**kwargs)
                break
            except ClientError as e:
                if e.response.get('Error').get('Code') not in retry_exceptions:
                    raise e
                time.sleep(1)

        # test whether expected key "Items" is in query
        if 'Items' not in response:
            print('No Items in dynamo response')
            return result

        # store relevant events
        for entry in response['Items']:
            result.append(entry)

        # test if stop early is matched
        if limit:
            if len(result) >= limit:
                return result

        while 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

            while True:
                try:
                    response = table.query(**kwargs)
                    break
                except ClientError as e:
                    if e.response.get('Error').get('Code') not in retry_exceptions:
                        raise e
                    time.sleep(1)

            # test whether expected key "Items" is in query
            if 'Items' not in response:
                print('No Items in dynamo response')
                break

            # store relevant events
            for entry in response['Items']:
                result.append(entry)

            if limit:
                if len(result) >= limit:
                    break

        return result

    def scan_table(self, table: str, **kwargs) -> pd.DataFrame:
        """scans table and returns content as pandas DataFrame

        Args:
            table (str): table name

        Returns:
            pd.DataFrame: complete table as DataFrame
        """

        response = self.dynamo.Table(table).scan(**kwargs)
        if 'Items' in response:
            df = pd.DataFrame(response['Items'])
        else:
            print(
                f'Scan of dynamo Table: {table} resulted in an empty DataFrame')
            df = pd.DataFrame()

        return df


class Iot():
    """Wrapper class for accessing aws IOT core
    """

    def __init__(self):
        self.iot_client = boto3.session.Session(
            aws_access_key_id=settings.AWS_CUBE_DASHBOARD_ID_KEY,
            aws_secret_access_key=settings.AWS_CUBE_DASHBOARD_ACCESS_KEY,
            region_name=settings.AWS_REGION).client('iot')

    def get_plantcubes(self, query: str = None) -> dict:
        """Gets the device shadow and metadata of cubes.
           If query is not specified the following filters are applied:
              state = 'Active'
              scope != 'Agrilution'
              thing name not starting with 'bugbash' or 'plantcube'
              there must have been a user connected once which is reflected in the first_registration_date
           It can be seen as installed base of plantcubes in the field.

        Args:
            query (str): query to filter for. Only 5 query terms are supported

        Returns:
            [dict] -- Dict with things, attributes & shadow
        """

        # number of items returned by iot client at once (doesnt limit the maximum amount)
        maxResults = 250

        # Filters for installed base described in the docstring
        if not query:
            query = f'thingName:(NOT bugbash* AND NOT plantcube*) AND attributes.state:Active AND -attributes.scope:Agrilution AND attributes.first_registration_date:*'

        # initialize
        response = self.iot_client.search_index(
            maxResults=maxResults, queryString=query)
        cubes = response['things']

        # loop until no more response received
        while response.get('nextToken'):
            response = self.iot_client.search_index(
                nextToken=response['nextToken'], maxResults=maxResults, queryString=query)
            cubes.extend(response['things'])

        # returns Dict with things, attributes & shadow
        return cubes
