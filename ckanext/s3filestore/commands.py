import sys
import boto
import boto.s3.connection
from pylons import config
from ckan.plugins import toolkit


class TestConnection(toolkit.CkanCommand):
    '''CKAN S3 FileStore utilities

    Usage:

        paster s3 check-config

            Checks if the configuration entered in the ini file is correct

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1

    def command(self):
        self._load_config()
        if not self.args:
            print self.usage
        elif self.args[0] == 'check-config':
            self.check_config()

    def check_config(self):
        exit = False
        for key in ('ckanext.s3filestore.aws_access_key_id',
                    'ckanext.s3filestore.aws_secret_access_key',
                    'ckanext.s3filestore.aws_bucket_name',
                    'ckanext.s3filestore.host_name',
                    'ckanext.s3filestore.port_name'):
            if not config.get(key):
                print 'You must set the "{0}" option in your ini file'.format(
                    key)
                exit = True
        if exit:
            sys.exit(1)

        print 'All configuration options defined'
        bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
        public_key = config.get('ckanext.s3filestore.aws_access_key_id')
        secret_key = config.get('ckanext.s3filestore.aws_secret_access_key')
        s3_host = config.get('ckanext.s3filestore.host_name')
        s3_port = int(config.get('ckanext.s3filestore.port_name'))

        #S3_conn = boto.connect_s3(public_key, secret_key)

        S3_conn = boto.connect_s3(
        aws_access_key_id = public_key,
        aws_secret_access_key = secret_key,
        host = s3_host, port = s3_port,
        is_secure=False, calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

        # Check if bucket exists
        bucket = S3_conn.lookup(bucket_name)
        if bucket is None:
            print 'Bucket {0} does not exist, trying to create it...'.format(
                bucket_name)
            try:
                bucket = S3_conn.create_bucket(bucket_name)
            except boto.exception.StandardError as e:
                print 'An error was found while creating the bucket:'
                print str(e)
                sys.exit(1)
        print 'Configuration OK!'
