__author__ = 'jonghyunc'

from boto.s3.connection import S3Connection
import time
import pdb

import os
from os import listdir
from os.path import isfile, join, isdir

import math
from filechunkio import FileChunkIO
import progressbar as pgb
import ssl


class awsutil():
    """A simple python interface for DeepMetadata AWS S3
       author: Jonghyun Choi (jonghyunc@allenai.org)
       Last updated: May 26, 2016
       Created: Nov 23, 2015

       usage example (remote ls): 
       > import awsutil
       >
       > aws_access_key_id = <your_access_key_goes_here>
       > aws_secret_access_key = <your_secret_access_key_goes_here>
       > aws_bucket_name = <desired_bucket_name_goes_here>
       >
       > awsobj = awsutil.awsutil(aws_access_key_id, aws_secret_access_key)
       >
       > awsobj.rls('.') # remote ls
       > awsobj.upload(<source_file_in_local_machine>, <target_location_in_aws>) # upload file to AWS (target location is a relative path)
       > awsobj.download(<source_location_in_aws>, <target_file_in_local_machine>)
       > awsobj.rrm(<file_in_aws>) # remove file in remote location
    """
    s3 = None
    bucket = None

    def get_s3_conn(self):
        return self.s3

    def get_bucket(self):
        return self.bucket

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_bucket_name=None):
        if aws_access_key_id is None or aws_secret_access_key is None or aws_bucket_name is None:
            print('Please specify aws_access_key_id, aws_secret_access_key and aws_bucket_name by awsutil.awsutil(aws_access_key_id, aws_secret_access_key, aws_bucket_name)')
            return
        self.s3 = S3Connection(aws_access_key_id, aws_secret_access_key)
        self.bucket = self.s3.get_bucket(aws_bucket_name, validate=False)
        # prevent SSL related error
        if hasattr(ssl, '_create_unverified_context'):
           ssl._create_default_https_context = ssl._create_unverified_context


    def upload(self, iFn, pathNfn):
        """
        Upload file or directory. If source is a directory, it recursively upload all files inside.
        """
        # todo: implement detecting remote directory exists
        if(pathNfn[-1:] == '/'):
            if(iFn is not '.' and iFn[-1:] is not '/'):
                # source is a file
                self.upload_file_to_dir(iFn, pathNfn[:-1])
            else:
                # source is a directory
                dirnameonly = os.path.basename(iFn[:-1])
                onlyfiles = [f for f in listdir(iFn) if isfile(join(iFn, f))]
                onlysubdirs = [f for f in listdir(iFn) if isdir(join(iFn, f))]
                for f in onlyfiles:
                    self.upload_file_to_dir(iFn[:-1]+'/'+f, pathNfn[:-1]+'/'+dirnameonly)
                for d in onlysubdirs:
                    self.upload(iFn[:-1]+'/'+d+'/', pathNfn[:-1]+'/'+dirnameonly+'/')
        else:
            # default: upload file to file
            self.upload_file_to_file(iFn, pathNfn)


    def upload_file_to_dir(self, iFn, pathNfn):
        """ 
        Upload file to a directory.
        """
        fn_only = os.path.basename(iFn)
        self.upload_file_to_file(iFn, pathNfn+'/'+fn_only)


    def upload_file_to_file(self, iFn, pathNfn):
        """
        Upload file to file.
        Desc: If size of file is bigger than 1GB, we use multi part upload.
        Note: Need to specify both source filename and target filename
        Usage: <obj>.upload('file.txt', 'data/file.txt')

        """
        # check if file is exist
        b = os.path.getsize(iFn)
        source_size = os.stat(iFn).st_size
        startt = time.time()
        if b < 1000000000:
            # regular upload (file size is less than 1GB)
            key = self.bucket.new_key(pathNfn)
            fsize = key.set_contents_from_filename(iFn) # TODO: check that the file is in the server, if so, abort it
            key.set_acl('public-read')
        else:
            # multi part upload (file size is larger than 1GB)
            key = self.bucket.new_key(pathNfn)
            mp = self.bucket.initiate_multipart_upload(pathNfn)
            # chunk_size = 52428800
            chunk_size = int(source_size / float(100))
            chunk_count = int(math.ceil(source_size / float(chunk_size)))
            # Send the file parts, using FileChunkIO to create a file-like object
            # that points to a certain byte range within the original file. We
            # set bytes to never exceed the original file size.
            widgets = ['Upload Progress: ', pgb.Percentage(), ' ', pgb.Bar(marker=pgb.RotatingMarker()), ' ', pgb.ETA(), ' '] #, pgb.FileTransferSpeed()]
            pbar = pgb.ProgressBar(widgets=widgets, maxval=100)
            pbar.start()
            for i in range(chunk_count):
                pbar.update(99*i/float(chunk_count) + 1)
                # print "chunk count", i, "out of", chunk_count
                offset = chunk_size * i
                bytes = min(chunk_size, source_size - offset)
                with FileChunkIO(iFn, 'r', offset=offset, bytes=bytes) as fp:
                    mp.upload_part_from_file(fp, part_num=i + 1)
            mp.complete_upload()
            pbar.finish()
            key.set_acl('public-read')
            fsize = source_size
        endt = time.time()
        print iFn,'is uploaded to',pathNfn,'(filesize:',fsize,') (',endt-startt,'sec elapsed )'


    def download(self, pathNfn, oFn):
        key = self.bucket.get_key(pathNfn)
        startt = time.time()
        key.get_contents_to_filename(oFn)
        endt = time.time()
        print pathNfn,'is downloaded to',oFn,'(',endt-startt,'sec elapsed )'


    def rls(self, pathname):
        print 'Listing: remote/'+pathname
        print '%10s %15s %30s %s' % ('<Index>', '<Size>', '<Last Modified>', '<Path and name>')
        keynames = self.bucket.list(prefix=pathname)
        retList = [None]*len([keyname for keyname in keynames])
        cnt = 0
        for keyname in keynames:
            pathnfnremote = str(keyname.name)

            if hasattr(keyname, 'size'):
                sizestr = keyname.size
            else:
                sizestr = '0'

            if hasattr(keyname, 'last_modified'):
                last_modified = keyname.last_modified
            else:
                last_modified = '<no_date>'
            #
            print '%10d %15s %30s %s' % (cnt, sizestr, last_modified, pathnfnremote)
            retList[cnt] = pathnfnremote
            cnt += 1
        #
        return retList


    def rrm(self, pathNfn):
        key = self.bucket.get_key(pathNfn)
        print 'deleting',pathNfn,'...',
        if key is not None:
            key.delete() # bucket.delete_key(pathNfn)
            print 'done.'
        else:
            print 'File is not found in remote.'
        # todo: recursive remove



# if __name__ == '__main__':
#     # test bench
#     aws_access_key_id = <your_access_key_goes_here>
#     aws_secret_access_key = <your_secret_access_key_goes_here>
#     aws_bucket_name = <desired_bucket_name_goes_here>

#     aa = awsutil()
#     remoteList = aa.rls('ai2')
