"""
The commands module contains the base definition for
a generic Qubole command and the implementation of all
the specific commands
"""

from qubole import Qubole
from resource import Resource
from account import Account

import boto

import time
import logging
import sys
import re

log = logging.getLogger("qds_commands")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class Command(Resource):

    """
    qds_sdk.Command is the base Qubole command class. Different types of Qubole
    commands can subclass this.
    """

    """all commands use the /commands endpoint"""
    rest_entity_path = "commands"

    @staticmethod
    def is_done(status):
        """
        Does the status represent a completed command
        Args:
            `status`: a status string

        Returns:
            True/False
        """
        return status == "cancelled" or status == "done" or status == "error"

    @staticmethod
    def is_success(status):
        return status == "done"

    @classmethod
    def create(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Note - this does not wait for the command to complete

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """

        conn = Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__

        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def run(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Waits until the command is complete. Repeatedly polls to check status

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """
        cmd = cls.create(**kwargs)
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)

        return cmd

    @classmethod
    def cancel_id(cls, id):
        """
        Cancels command denoted by this id

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        data = {"status": "kill"}
        return conn.put(cls.element_path(id), data)

    def cancel(self):
        """
        Cancels command represented by this object
        """
        self.__class__.cancel_id(self.id)

    def get_log(self):
        """
        Fetches log for the command represented by this object

        Returns:
            The log as a string
        """
        log_path = self.meta_data['logs_resource']
        conn = Qubole.agent()
        r = conn.get_raw(log_path)
        return r.text

    def get_results(self, fp=sys.stdout, inline=True, delim=None):
        """
        Fetches the result for the command represented by this object

        Args:
            `fp`: a file object to write the results to directly
        """
        result_path = self.meta_data['results_resource']

        conn = Qubole.agent()

        r = conn.get(result_path, {'inline': inline})
        if r.get('inline'):
            fp.write(r['results'].encode('utf8'))
        else:
            acc = Account.find()
            boto_conn = boto.connect_s3(aws_access_key_id=acc.storage_access_key,
                                        aws_secret_access_key=acc.storage_secret_key)

            log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
            #fetch latest value of num_result_dir
            num_result_dir = Command.find(self.id).num_result_dir
            for s3_path in r['result_location']:
                _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=delim)


class HiveCommand(Command):
    pass


class PrestoCommand(Command):
    pass


class HadoopCommand(Command):
    pass


class ShellCommand(Command):
    pass


class PigCommand(Command):
    pass


class DbExportCommand(Command):
    pass


class DbImportCommand(Command):
    def __init__(self):
        raise Exception("dbimport command not implemented yet")


def _read_iteratively(key_instance, fp, delim):
    key_instance.open_read()
    while True:
        try:
            # Default buffer size is 8192 bytes
            data = key_instance.next()
            fp.write(str(data).replace(chr(1), delim))
        except StopIteration:
            # Stream closes itself when the exception is raised
            return


def _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=None):
    '''
    Downloads the contents of all objects in s3_path into fp

    Args:
        `boto_conn`: S3 connection object

        `s3_path`: S3 path to be downloaded

        `fp`: The file object where data is to be downloaded
    '''
    #Progress bar to display download progress
    def _callback(downloaded, total):
        '''
        Call function for upload.

        `downloaded`: File size already downloaded (int)

        `total`: Total file size to be downloaded (int)
        '''
        if (total is 0) or (downloaded == total):
            return
        progress = downloaded*100/total
        sys.stderr.write('\r[{0}] {1}%'.format('#'*progress, progress))
        sys.stderr.flush()

    def _is_complete_data_available(bucket_paths, num_result_dir):
        if num_result_dir == -1:
            return True
        unique_paths = set()
        files = {}
        for one_path in bucket_paths:
            name = one_path.name.replace(key_prefix, "", 1)
            if name.startswith('_tmp.'):
                continue
            path = name.split("/")
            dir = path[0].replace("_$folder$", "", 1)
            unique_paths.add(dir)
            if len(path) > 1:
                file = int(path[1])
                if files.has_key(dir) is False:
                    files[dir] = []
                files[dir].append(file)
        if len(unique_paths) < num_result_dir:
            return False
        for k in files:
            v = files.get(k)
            if len(v) > 0 and max(v) + 1 > len(v):
                return False
        return True

    m = _URI_RE.match(s3_path)
    bucket_name = m.group(1)
    bucket = boto_conn.get_bucket(bucket_name)
    retries = 6
    if s3_path.endswith('/') is False:
        #It is a file
        key_name = m.group(2)
        key_instance = bucket.get_key(key_name)
        while key_instance is None and retries > 0:
            retries = retries - 1
            log.info("Results file is not available on s3. Retry: " + str(6-retries))
            time.sleep(10)
            key_instance = bucket.get_key(key_name)
        if key_instance is None:
            raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")
        log.info("Downloading file from %s" % s3_path)
        if delim is None:
            key_instance.get_contents_to_file(fp)  # cb=_callback
        else:
            # Get contents as string. Replace parameters and write to file.
            _read_iteratively(key_instance, fp, delim=delim)

    else:
        #It is a folder
        key_prefix = m.group(2)
        bucket_paths = bucket.list(key_prefix)
        complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
        while complete_data_available is False and retries > 0:
            retries = retries - 1
            log.info("Results dir is not available on s3. Retry: " + str(6-retries))
            time.sleep(10)
            complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
        if complete_data_available is False:
            raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")

        for one_path in bucket_paths:
            name = one_path.name

            # Eliminate _tmp_ files which ends with $folder$
            if name.endswith('$folder$'):
                continue

            log.info("Downloading file from %s" % name)
            if delim is None:
                one_path.get_contents_to_file(fp)  # cb=_callback
            else:
                _read_iteratively(one_path, fp, delim=delim)
