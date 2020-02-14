import boto3, time, io
from boto3.s3.transfer import TransferConfig
import pandas as pd

s3 = boto3.client('s3', region_name='us-east-2')

def write_to_parquet(df, **kwargs):
    s=io.BytesIO()
    df.to_parquet(s, **kwargs)
    return s
def write_spark(df):
    return write_to_parquet(df, index=False, flavor='spark')

def write_csv(df, **kwargs):
    #s=io.BytesIO()
    return io.BytesIO(df.to_csv(**kwargs).encode())

def write_1_f(obj, filename, use_threads=True, max_concurrency=16, retry=10, transfer_func=None, 
    multipart_threshold=1*1024*1024, multipart_chunksize=512*1024, check_func=None):
    if isinstance(obj, str): obj=obj.encode('utf-8')
    if transfer_func:
        obj=transfer_func(obj)
    elif isinstance(obj, bytes): 
        obj=io.BytesIO(obj)        
    
    s3_transfer_config = TransferConfig(max_concurrency=max_concurrency, use_threads=use_threads,
            multipart_threshold=multipart_threshold, multipart_chunksize=multipart_chunksize)
    bucket, file = filename.split('/', 1)
    for _ in range(retry):
        try:
            obj.seek(0) 
            s3.upload_fileobj(obj, bucket, file, Config = s3_transfer_config)
            #set_trace()
            if check_func:
                data=check_func(filename)
            return
        except:
            time.sleep(1)
    return True #failed

def upload_1_f(source, filename, use_threads=True, max_concurrency=16, retry=10, 
    multipart_threshold=5*1024*1024, multipart_chunksize=1024*1024):
    
    s3_transfer_config = TransferConfig(max_concurrency=max_concurrency, use_threads=use_threads,
            multipart_threshold=multipart_threshold, multipart_chunksize=multipart_chunksize)
    
    bucket, file = filename.split('/', 1)
    
    for _ in range(retry):
        try:
            s3.upload_file(source, bucket, file, Config = s3_transfer_config)
            return
        except:
            time.sleep(1)
    return True #failed

def read_1_f(filename, use_threads=True, max_concurrency=16, retry=10, return_bytes=True, 
    multipart_threshold=1*1024*1024, multipart_chunksize=512*1024):
    s3_transfer_config = TransferConfig(max_concurrency=max_concurrency, use_threads=use_threads,
            multipart_threshold=multipart_threshold, multipart_chunksize=multipart_chunksize, 
            )
    bucket, file = filename.split('/', 1)
    for _ in range(retry):
        try:
            s=io.BytesIO()
            s3.download_fileobj(bucket, file, s, Config=s3_transfer_config)
            s.seek(0)
            if return_bytes:
                return s.read()
            else:
                return s
        except:
            time.sleep(1)
    return True

def rm_1_f(filename, retry=10):
    bucket, file = filename.split('/', 1)
    for _ in range(retry):
        try:
            response = s3.delete_object(Bucket=bucket, Key=file)
            return
        except:
            time.sleep(1)
    return True

from pyarrow.compat import guid
def write_1_group(group, prefix='', col_names=[], **kwargs):
    key, df = group
    df.drop(col_names, 1, inplace=True)
    suffix = '/'.join(['='.join([c,k]) for c,k in zip(col_names, [f'{k_}' for k_ in key])])
    f = '/'.join((prefix.rstrip('/'), suffix, guid()+'.parquet'))
    write_1_f(df, f, transfer_func=write_spark, **kwargs)

def read_csv(f, *args, **kwargs):
    return pd.read_csv(read_1_f(f, return_bytes=False), *args, **kwargs)
def read_parquet(f, **kwargs):
    kwargs.update(dict(return_bytes=False))
    return pd.read_parquet(read_1_f(f, **kwargs))
