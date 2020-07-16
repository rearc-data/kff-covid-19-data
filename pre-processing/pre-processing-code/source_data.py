import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from zipfile import ZipFile


def source_dataset():

    source_dataset_url = 'https://github.com/KFFData/COVID-19-Data/archive/kff_master.zip'

    try:
        response = urlopen(source_dataset_url)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code)

    except URLError as e:
        raise Exception('URLError: ', e.reason)

    else:
        zip_location = '/tmp/' + 'test' + '.zip'

        with open(zip_location, 'wb') as f:
            f.write(response.read())

        with ZipFile(zip_location, 'r') as z:
            z.extractall('/tmp')

        os.remove(zip_location)

        s3_bucket = os.environ['S3_BUCKET']
        data_set_name = os.environ['DATA_SET_NAME']
        new_s3_key = data_set_name + '/dataset/'
        s3 = boto3.client('s3')

        unzipped_name = ''

        for filename in os.listdir('/tmp'):
            unzipped_name = filename

        asset_list = []

        for r, d, f in os.walk('/tmp/' + unzipped_name):
            for filename in f:

                obj_name = os.path.join(r, filename).split(
                    '/', 3).pop().replace(' ', '_').lower()
                s3.upload_file(os.path.join(r, filename),
                               s3_bucket, new_s3_key + obj_name)

                print('Uploaded: ' + obj_name)

                asset_list.append(
                    {'Bucket': s3_bucket, 'Key': new_s3_key + obj_name})

        return asset_list
