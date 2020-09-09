import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from zipfile import ZipFile
from s3_md5_compare import md5_compare


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
        s3 = boto3.client('s3')

        unzipped_name = ''

        for filename in os.listdir('/tmp'):
            unzipped_name = filename

        s3_uploads = []
        asset_list = []

        for r, d, f in os.walk('/tmp/' + unzipped_name):
            for filename in f:

                obj_name = os.path.join(r, filename).split('/', 3).pop().replace(' ', '_').lower()
                file_location = os.path.join(r, filename)
                new_s3_key = data_set_name + '/dataset/' + obj_name

                has_changes = md5_compare(s3, s3_bucket, new_s3_key, file_location)
                if has_changes:
                    s3.upload_file(file_location, s3_bucket, new_s3_key)
                    print('Uploaded: ' + filename)
                else:
                    print('No changes in: ' + filename)

                asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
                s3_uploads.append({'has_changes': has_changes, 'asset_source': asset_source})

        count_updated_data = sum(upload['has_changes'] == True for upload in s3_uploads)
        if count_updated_data > 0:
            asset_list = list(map(lambda upload: upload['asset_source'], s3_uploads))
            if len(asset_list) == 0:
                raise Exception('Something went wrong when uploading files to s3')
        # asset_list is returned to be used in lamdba_handler function
        # if it is empty, lambda_handler will not republish
        return asset_list
