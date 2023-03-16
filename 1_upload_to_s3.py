'''

@Authors: Vighnesh Harish Bilgi, Pavan Kalyan Yellaboina, Vipul Shete
@Date: 2023-03-10
@Last Modified by: Pavan Kalyan 
@Last Modified time: 2023-03-15
@Title : Extract the COVID-19 dataset to the S3 bucket

'''

import boto3
import pandas as pd

def lambda_handler(event, context):
    # create a pandas dataframe
    file_url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv' 
    response = pd.read_csv(file_url)

    df = pd.DataFrame(response)
    
    # convert the dataframe to CSV format
    csv_string = df.to_csv(index=False)
    
    # write the CSV file to S3
    bucket_name = 'poccovid19'
    file_name = 'raw_data/covid_19_data.csv'
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_string, Bucket=bucket_name, Key=file_name)
    
    return {
        'statusCode': 200,
        'body': 'Updated COVID-19 data is written to S3'
    }
