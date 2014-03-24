#!/bin/env python

from qds_sdk.qubole import Qubole
from qds_sdk.commands import Command
from qds_sdk.commands import HiveCommand
from qds_sdk.commands import PrestoCommand
from qds_sdk.commands import HadoopCommand
from qds_sdk.commands import ShellCommand
from qds_sdk.commands import PigCommand
from qds_sdk.commands import DbExportCommand
from qds_sdk.cluster import Cluster
from qds_sdk.cluster import ClusterInfo
from qds_sdk.hadoop_cluster import HadoopCluster
import qds_sdk.exception

import os
import sys
import traceback
import logging
import json
import pipes
import argparse

log = logging.getLogger("qds")


def submitaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    payload = globals()["_verify_"+args.subcommand+"_arguments"](args)
    cmd = cmdclass.create(**payload)
    print "Submitted %s, Id: %s" % (cmdclass.__name__, cmd.id)
    return 0


def _getresult(cmdclass, cmd):
    if Command.is_success(cmd.status):
        log.info("Fetching results for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        cmd.get_results(sys.stdout, delim='\t')
        return 0
    else:
        log.error("Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status))
        return 1


def runaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    payload = globals()["_verify_"+args.subcommand+"_arguments"](args)
    cmd = cmdclass.run(**payload)
    return _getresult(cmdclass, cmd)


def checkaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    o = cmdclass.find(args.command_id)
    print str(o)
    return 0


def cancelaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    r = cmdclass.cancel_id(args.command_id)
    skey = 'kill_succeeded'
    if r.get(skey) is None:
        sys.stderr.write("Invalid Json Response %s - missing field '%s'" % (str(r), skey))
        return 11
    elif r['kill_succeeded']:
        print "Command killed successfully"
        return 0
    else:
        sys.stderr.write("Cancel failed with reason '%s'\n" % r.get('result'))
        return 12


def getresultaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    cmd = cmdclass.find(args.command_id)
    return _getresult(cmdclass, cmd)


def getlogaction(args):
    cmdclass = get_cmdclass(args.subcommand)
    o = cmdclass.find(args.command_id)
    print o.get_log()
    return 0


def get_cmdclass(command):
    if command == "hivecmd":
        return HiveCommand
    elif command == "prestocmd":
        return PrestoCommand
    elif command == "hadoopcmd":
        return HadoopCommand
    elif command == "shellcmd":
        return ShellCommand
    elif command == "pigcmd":
        return PigCommand
    elif command == "dbexportcmd":
        return DbExportCommand
    # Why not just use the globals() dict:
    # cmdclassname = command[0].upper() + command[1:-3] + "Command"
    # return globals()[cmdclassname]
    # Because of dbexportcmd => DbexportCommand and not DbExportCommand
    # The backend expects DbExportCommand (that's why not a good idea to rename
    # the class)


def cluster_create_action(args):
    cluster_info = _create_cluster_info(args)
    result = Cluster.create(cluster_info.minimal_payload())
    print json.dumps(result, indent=4)
    return 0


def cluster_update_action(args):
    cluster_info = _create_cluster_info(args)
    result = Cluster.update(args.cluster_id, cluster_info.minimal_payload())
    print json.dumps(result, indent=4)
    return 0


def _create_cluster_info(arguments):
    cluster_info = ClusterInfo(arguments.label,
                               arguments.aws_access_key_id,
                               arguments.aws_secret_access_key,
                               arguments.disallow_cluster_termination,
                               arguments.enable_ganglia_monitoring)

    cluster_info.set_ec2_settings(arguments.aws_region,
                                  arguments.aws_availability_zone)

    custom_config = None
    if arguments.custom_config_file is not None:
        try:
            custom_config = open(arguments.custom_config_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read custom config file: %s\n" %
                             str(e))
            sys.exit(1)
    cluster_info.set_hadoop_settings(arguments.master_instance_type,
                                     arguments.slave_instance_type,
                                     arguments.initial_nodes,
                                     arguments.max_nodes,
                                     custom_config,
                                     arguments.slave_request_type)

    cluster_info.set_spot_instance_settings(
            arguments.maximum_bid_price_percentage,
            arguments.timeout_for_request,
            arguments.maximum_spot_instance_percentage)

    fairscheduler_config_xml = None
    if arguments.fairscheduler_config_xml_file is not None:
        try:
            fairscheduler_config_xml = open(arguments.fairscheduler_config_xml_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read config xml file: %s\n" %
                             str(e))
            sys.exit(1)
    cluster_info.set_fairscheduler_settings(fairscheduler_config_xml,
                                            arguments.default_pool)

    customer_ssh_key = None
    if arguments.customer_ssh_key_file is not None:
        try:
            customer_ssh_key = open(arguments.customer_ssh_key_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read customer ssh key file: %s\n" %
                             str(e))
            sys.exit(1)
    cluster_info.set_security_settings(arguments.persistent_security_groups,
                                       arguments.encrypted_ephemerals,
                                       customer_ssh_key)

    cluster_info.set_presto_settings(arguments.presto_jvm_memory,
                                     arguments.presto_task_memory)

    return cluster_info


def cluster_delete_action(args):
    result = Cluster.delete(args.cluster_id)
    print json.dumps(result, indent=4)
    return 0


def cluster_list_action(args):
    if args.cluster_id is not None:
        result = Cluster.show(args.cluster_id)
    elif args.state is not None:
        result = Cluster.list(state=args.state)
    elif args.label is not None:
        result = Cluster.list(label=args.label)
    else:
        result = Cluster.list()
    print json.dumps(result, indent=4)
    return 0


def cluster_start_action(args):
    result = Cluster.start(args.cluster_id)
    print json.dumps(result, indent=4)
    return 0


def cluster_terminate_action(args):
    result = Cluster.terminate(args.cluster_id)
    print json.dumps(result, indent=4)
    return 0


def cluster_status_action(args):
    result = Cluster.status(args.cluster_id)
    print json.dumps(result, indent=4)
    return 0


def cluster_check_action(args):
    log.warn("'hadoop_cluster check' command is deprecated and will be"
            " removed in the next version. Please use 'cluster status'"
            " instead.\n")
    o = HadoopCluster.find()
    print str(o)
    return 0


def command_check_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    argparser.add_argument("command_id", help="id of the command to check")
    argparser.set_defaults(func=checkaction)
    return argparser


def command_cancel_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    argparser.add_argument("command_id", help="id of the command to cancel")
    argparser.set_defaults(func=cancelaction)
    return argparser


def command_getresult_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    argparser.add_argument("command_id",
            help="id of the command whose results are to be fetched")
    argparser.set_defaults(func=getresultaction)
    return argparser


def command_getlog_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    argparser.add_argument("command_id",
            help="id of the command whose logs are to be fetched")
    argparser.set_defaults(func=getlogaction)
    return argparser


def hive_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("-q", "--query", dest="query", help="query string")

    group.add_argument("-f", "--script_location", dest="script_location",
            help="Path where hive query to run is stored. Can be S3 URI or local file path")

    argparser.add_argument("--macros", dest="macros",
            help="expressions to expand macros used in query")

    argparser.add_argument("--sample_size", dest="sample_size",
            help="size of sample in bytes on which to run query")

    argparser.add_argument("--cluster-label", dest="label",
            help="the label of the cluster to run the command on")

    return argparser


def _verify_hivecmd_arguments(arguments):
    if arguments.script_location is not None:
        if ((arguments.script_location.find("s3://") != 0) and
            (arguments.script_location.find("s3n://") != 0)):
            # script location is local file
            try:
                q = open(arguments.script_location).read()
            except IOError, e:
                sys.stderr.write("Unable to open script location: %s\n" %
                                 str(e))
                sys.exit(1)
            arguments.script_location = None
            arguments.query = q

    if arguments.macros is not None:
        arguments.macros = json.loads(arguments.macros)

    payload = {
            "sample_size": arguments.sample_size,
            "label": arguments.label,
            "macros": arguments.macros,
            "query": arguments.query,
            "script_location": arguments.script_location
    }
    return payload


def presto_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("-q", "--query", dest="query", help="query string")

    group.add_argument("-f", "--script_location", dest="script_location",
            help="Path where presto query to run is stored. Can be S3 URI or local file path")

    argparser.add_argument("--macros", dest="macros",
            help="expressions to expand macros used in query")

    argparser.add_argument("--cluster-label", dest="label",
            help="the label of the cluster to run the command on")

    return argparser


def _verify_prestocmd_arguments(arguments):
    if arguments.script_location is not None:
        if ((arguments.script_location.find("s3://") != 0) and
            (arguments.script_location.find("s3n://") != 0)):
            # script location is local file
            try:
                q = open(arguments.script_location).read()
            except IOError, e:
                sys.stderr.write("Unable to open script location: %s\n" %
                                 str(e))
                sys.exit(1)
            arguments.script_location = None
            arguments.query = q

    if arguments.macros is not None:
        arguments.macros = json.loads(arguments.macros)

    payload = {
            "label": arguments.label,
            "macros": arguments.macros,
            "query": arguments.query,
            "script_location": arguments.script_location
    }
    return payload


def hadoop_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)

    argparser.add_argument("--cluster-label", dest="label",
            help="the label of the cluster to run the command on")

    argparser.add_argument("hadoop_sub_command",
            choices=["jar", "s3distcp", "streaming"])

    argparser.add_argument("hadoop_sub_command_arg", nargs="+")
    return argparser


def _verify_hadoopcmd_arguments(arguments):
    payload = {
            "label": arguments.label,
            "sub_command": arguments.hadoop_sub_command,
            "sub_command_args": " ".join("'" + a + "'" for a in arguments.hadoop_sub_command_arg)
    }
    return payload


def shell_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)

    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--script", dest="inline", help="inline script that can be executed by bash")

    group.add_argument("-f", "--script_location", dest="script_location",
            help="Path where bash script to run is stored. Can be S3 URI or local file path")

    argparser.add_argument("-i", "--files", dest="files",
            help="List of files [optional] Format : file1,file2 (files in s3 bucket) These files will be copied to the working directory where the command is executed")

    argparser.add_argument("-a", "--archive", dest="archive",
            help="List of archives [optional] Format : archive1,archive2 (archives in s3 bucket) These are unarchived in the working directory where the command is executed")

    argparser.add_argument("--cluster-label", dest="label",
            help="the label of the cluster to run the command on")

    argparser.add_argument("parameters", nargs="*", default=None)

    return argparser


def _verify_shellcmd_arguments(arguments):
    if arguments.script_location is not None:
        if ((arguments.script_location.find("s3://") != 0) and
            (arguments.script_location.find("s3n://") != 0)):
            # script location is local file
            try:
                s = open(arguments.script_location).read()
            except IOError, e:
                sys.stderr.write("Unable to open script location: %s\n" %
                                 str(e))
                sys.exit(1)
            arguments.script_location = None
            arguments.inline = s

        if arguments.parameters is not None:
            if arguments.inline is not None:
                sys.stderr.write(
                    "This sucks - but extra arguments can only be "
                    "supplied with a script_location in S3 right now\n")
                sys.exit(1)

            arguments.parameters = " ".join([pipes.quote(a) for a in arguments.parameters])

    else:
        if arguments.parameters is not None and len(arguments.parameters) > 0:
            sys.stderr.write(
                "Extra arguments can only be supplied with a script_location\n")
            sys.exit(1)

    payload = {
            "label": arguments.label,
            "parameters": arguments.parameters,
            "archive": arguments.archive,
            "files": arguments.files,
            "inline": arguments.inline,
            "script_location": arguments.script_location
    }
    return payload


def pig_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)

    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--script", dest="latin_statements",
            help="latin statements that have to be executed")

    group.add_argument("-f", "--script_location", dest="script_location",
            help="Path where pig script to run is stored. Can be S3 URI or local file path")

    argparser.add_argument("--cluster-label", dest="label",
            help="the label of the cluster to run the command on")

    argparser.add_argument("parameters", nargs="*", default=None,
            help="Arguments to pig script (format k1=v1 k2=v2 k3=v3...)")

    return argparser


def _verify_pigcmd_arguments(arguments):
    if arguments.script_location is not None:
        if ((arguments.script_location.find("s3://") != 0) and
            (arguments.script_location.find("s3n://") != 0)):
            # script location is local file
            try:
                s = open(arguments.script_location).read()
            except IOError, e:
                sys.stderr.write("Unable to open script location: %s\n" %
                                 str(e))
                sys.exit(1)
            arguments.script_location = None
            arguments.latin_statements = s

        if arguments.parameters is not None:
            if arguments.latin_statements is not None:
                sys.stderr.write(
                    "This sucks - but extra arguments can only be "
                    "supplied with a script_location in S3 right now\n")
                sys.exit(1)

            p = {}
            for a in arguments.parameters:
                kv = a.split('=')
                if len(kv) != 2:
                    sys.stderr.write("Arguments to pig script must be of this format k1=v1 k2=v2 k3=v3...\n")
                    sys.exit(1)
                p[kv[0]] = kv[1]
            arguments.parameters = p

    else:
        if arguments.parameters is not None and len(arguments.parameters) > 0:
            sys.stderr.write(
                "Extra arguments can only be supplied with a script_location\n")
            sys.exit(1)

    payload = {
            "label": arguments.label,
            "parameters": arguments.parameters,
            "latin_statements": arguments.latin_statements,
            "script_location": arguments.script_location
    }
    return payload


def dbexport_run_submit_parser():
    argparser = argparse.ArgumentParser(add_help=False)

    argparser.add_argument("-m", "--mode", dest="mode", required=True,
            choices=["1", "2"],
            help="Can be 1 for Hive export or 2 for HDFS/S3 export")

    argparser.add_argument("--hive_table", dest="hive_table",
            help="Mode 1: Name of the Hive Table from which data will be exported")

    argparser.add_argument("--partition_spec", dest="partition_spec",
            help="Mode 1: (optional) Partition specification for Hive table")

    argparser.add_argument("--dbtap_id", dest="dbtap_id", required=True,
            help="Modes 1 and 2: DbTap Id of the target database in Qubole")

    argparser.add_argument("--db_table", dest="db_table", required=True,
            help="Modes 1 and 2: Table to export to in the target database")

    argparser.add_argument("--db_update_mode", dest="db_update_mode",
            choices=['allowinsert', 'updateonly'],
            help="""Modes 1 and 2: (optional) can be 'allowinsert' or
                    'updateonly'. If updateonly is specified - only existing
                    rows are updated. If allowinsert is specified - then
                    existing rows are updated and non existing rows are
                    inserted. If this option is not specified - then the
                    given the data will be appended to the table""")

    argparser.add_argument("--db_update_keys", dest="db_update_keys",
            help="""Modes 1 and 2: Columns used to determine the uniqueness of
                   rows for 'updateonly' mode""")

    argparser.add_argument("--export_dir", dest="export_dir",
            help="Mode 2: HDFS/S3 location from which data will be exported")

    argparser.add_argument("--fields_terminated_by", dest="fields_terminated_by",
            help="""Mode 2: Hex of the char used as column separator in the
                    dataset, for eg. \0x20 for space""")

    return argparser


def _verify_dbexportcmd_arguments(arguments):
    if arguments.mode is "1":
        if arguments.hive_table is None:
            sys.stderr.write("hive_table is required for mode 1\n")
            sys.exit(1)
    elif arguments.export_dir is None:    # mode 2
        sys.stderr.write("export_dir is required for mode 2\n")
        sys.exit(1)

    if arguments.db_update_mode is not None:
        if arguments.db_update_mode is "updateonly":
            if arguments.db_update_keys is None:
                sys.stderr.write("db_update_keys is required when db_update_mode "
                                 "is 'updateonly'\n")
                sys.exit(1)
        elif arguments.db_update_keys is not None:
            sys.stderr.write("db_update_keys is used only when db_update_mode "
                             "is 'updateonly'\n")
            sys.exit(1)

    payload = {
            "mode": arguments.mode,
            "dbtap_id": arguments.dbtap_id,
            "db_table": arguments.db_table,
            "hive_table": arguments.hive_table,
            "partition_spec": arguments.partition_spec,
            "db_update_mode": arguments.db_update_mode,
            "db_update_keys": arguments.db_update_keys,
            "export_dir": arguments.export_dir,
            "fields_terminated_by": arguments.fields_terminated_by,
    }
    return payload


def command_parsers(subparsers):
    hive_parser = subparsers.add_parser("hivecmd")
    hive_subparsers = hive_parser.add_subparsers(title="actions")
    hive_run_parser = hive_subparsers.add_parser("run",
            parents=[hive_run_submit_parser()],
            help="submit command and wait; print results")
    hive_run_parser.set_defaults(func=runaction)
    hive_submit_parser = hive_subparsers.add_parser("submit",
            parents=[hive_run_submit_parser()],
            help="submit command and print id")
    hive_submit_parser.set_defaults(func=submitaction)
    hive_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    hive_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    hive_subparsers.add_parser("getresult",
            parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    hive_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")

    pig_parser = subparsers.add_parser("pigcmd")
    pig_subparsers = pig_parser.add_subparsers(title="actions")
    pig_run_parser = pig_subparsers.add_parser("run",
            parents=[pig_run_submit_parser()],
            help="submit command and wait; print results")
    pig_run_parser.set_defaults(func=runaction)
    pig_submit_parser = pig_subparsers.add_parser("submit",
            parents=[pig_run_submit_parser()],
            help="submit command and print id")
    pig_submit_parser.set_defaults(func=submitaction)
    pig_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    pig_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    pig_subparsers.add_parser("getresult", parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    pig_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")

    hadoop_parser = subparsers.add_parser("hadoopcmd")
    hadoop_subparsers = hadoop_parser.add_subparsers(title="actions")
    hadoop_run_parser = hadoop_subparsers.add_parser("run",
            parents=[hadoop_run_submit_parser()],
            help="submit command and wait; print results")
    hadoop_run_parser.set_defaults(func=runaction)
    hadoop_submit_parser = hadoop_subparsers.add_parser("submit",
            parents=[hadoop_run_submit_parser()],
            help="submit command and print id")
    hadoop_submit_parser.set_defaults(func=submitaction)
    hadoop_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    hadoop_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    hadoop_subparsers.add_parser("getresult",
            parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    hadoop_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")

    shell_parser = subparsers.add_parser("shellcmd")
    shell_subparsers = shell_parser.add_subparsers(title="actions")
    shell_run_parser = shell_subparsers.add_parser("run",
            parents=[shell_run_submit_parser()],
            help="submit command and wait; print results")
    shell_run_parser.set_defaults(func=runaction)
    shell_submit_parser = shell_subparsers.add_parser("submit",
            parents=[shell_run_submit_parser()],
            help="submit command and print id")
    shell_submit_parser.set_defaults(func=submitaction)
    shell_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    shell_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    shell_subparsers.add_parser("getresult",
            parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    shell_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")

    dbexport_parser = subparsers.add_parser("dbexportcmd")
    dbexport_subparsers = dbexport_parser.add_subparsers(title="actions")
    dbexport_run_parser = dbexport_subparsers.add_parser("run",
            parents=[dbexport_run_submit_parser()],
            help="submit command and wait; print results")
    dbexport_run_parser.set_defaults(func=runaction)
    dbexport_submit_parser = dbexport_subparsers.add_parser("submit",
            parents=[dbexport_run_submit_parser()],
            help="submit command and print id")
    dbexport_submit_parser.set_defaults(func=submitaction)
    dbexport_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    dbexport_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    dbexport_subparsers.add_parser("getresult",
            parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    dbexport_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")

    presto_parser = subparsers.add_parser("prestocmd")
    presto_subparsers = presto_parser.add_subparsers(title="actions")
    presto_run_parser = presto_subparsers.add_parser("run",
            parents=[presto_run_submit_parser()],
            help="submit command and wait; print results")
    presto_run_parser.set_defaults(func=runaction)
    presto_submit_parser = presto_subparsers.add_parser("submit",
            parents=[presto_run_submit_parser()],
            help="submit command and print id")
    presto_submit_parser.set_defaults(func=submitaction)
    presto_subparsers.add_parser("check", parents=[command_check_parser()],
            help="print the command object for the given id")
    presto_subparsers.add_parser("cancel", parents=[command_cancel_parser()],
            help="cancels the command with given id")
    presto_subparsers.add_parser("getresult",
            parents=[command_getresult_parser()],
            help="fetch the results for the command with given id")
    presto_subparsers.add_parser("getlog", parents=[command_getlog_parser()],
            help="fetch the logs for the command with given id")


def hadoop_cluster_parsers(subparsers):
    hadoop_cluster_parser = subparsers.add_parser("hadoop_cluster",
            description="""'hadoop_cluster check' command is deprecated and
                           will be removed in the next version. Please use
                           'cluster status' instead.""")
    hc_subparsers = hadoop_cluster_parser.add_subparsers(title="actions")

    hc_check_parser = hc_subparsers.add_parser("check",
            description="""'hadoop_cluster check' command is deprecated and
                           will be removed in the next version. Please use
                           'cluster status' instead.""")
    hc_check_parser.set_defaults(func=cluster_check_action)


def cluster_parsers(subparsers):
    cluster_parser = subparsers.add_parser("cluster")
    cluster_subparsers = cluster_parser.add_subparsers(title="actions")

    cluster_create_parser = cluster_subparsers.add_parser("create",
            parents=[cluster_create_update_parent_parser()],
            help="create a new cluster")
    cluster_create_parser.set_defaults(func=cluster_create_action)
    cluster_create_parser.add_argument("--label", dest="label",
            nargs="+", required=True,
            help="""list of label for the cluster
                    (atleast one label is required)""")
    cluster_create_parser.add_argument("--access-key-id",
            dest="aws_access_key_id", required=True,
            help="""access key id for customer's aws account.
                    This is required while creating the cluster""",)
    cluster_create_parser.add_argument("--secret-access-key",
            dest="aws_secret_access_key", required=True,
            help="""secret access key for customer's aws account.
                    This is required while creating the cluster""",)

    cluster_update_parser = cluster_subparsers.add_parser("update",
            parents=[cluster_create_update_parent_parser()],
            help="update the settings of an existing cluster")
    cluster_update_parser.set_defaults(func=cluster_update_action)
    cluster_update_parser.add_argument("cluster_id",
            help="id of the cluster to update")
    cluster_update_parser.add_argument("--label", dest="label", nargs="+",
            help="list of label for the cluster")
    cluster_update_parser.add_argument("--access-key-id",
            dest="aws_access_key_id",
            help="access key id for customer's aws account",)
    cluster_update_parser.add_argument("--secret-access-key",
            dest="aws_secret_access_key",
            help="secret access key for customer's aws account",)

    cluster_list_parser = cluster_subparsers.add_parser("list",
            help="""list existing clusters; you can list all clusters or filter
                     them by id or label or state""")
    cluster_list_parser.set_defaults(func=cluster_list_action)
    clp_group = cluster_list_parser.add_mutually_exclusive_group()
    clp_group.add_argument("--id", dest="cluster_id",
            help="show cluster with this id")
    clp_group.add_argument("--label", dest="label",
            help="show cluster with this label")
    clp_group.add_argument("--state", dest="state", action="store",
            choices=['up', 'down', 'pending', 'terminating'],
            help="list only clusters in the given state")

    cluster_delete_parser = cluster_subparsers.add_parser("delete",
            help="delete an existing cluster")
    cluster_delete_parser.set_defaults(func=cluster_delete_action)
    cluster_delete_parser.add_argument("cluster_id",
            help="id of the cluster to delete")

    cluster_start_parser = cluster_subparsers.add_parser("start",
            help="start an existing cluster")
    cluster_start_parser.set_defaults(func=cluster_start_action)
    cluster_start_parser.add_argument("cluster_id",
            help="id of the cluster to start")

    cluster_terminate_parser = cluster_subparsers.add_parser("terminate",
            help="terminate a running cluster")
    cluster_terminate_parser.set_defaults(func=cluster_terminate_action)
    cluster_terminate_parser.add_argument("cluster_id",
            help="id of the cluster to terminate")

    cluster_status_parser = cluster_subparsers.add_parser("status",
            help="""check the status of an existing cluster:
                    up, down, pending, terminating""")
    cluster_status_parser.set_defaults(func=cluster_status_action)
    cluster_status_parser.add_argument("cluster_id",
            help="id of the cluster whose status you want to check")


def cluster_create_update_parent_parser():
    argparser = argparse.ArgumentParser(add_help=False)
    ec2_group = argparser.add_argument_group("ec2 settings")
    ec2_group.add_argument("--aws-region",
            dest="aws_region",
            choices=["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
            help="aws region to create the cluster in",)
    ec2_group.add_argument("--aws-availability-zone",
            dest="aws_availability_zone",
            help="availability zone to create the cluster in",)

    hadoop_group = argparser.add_argument_group("hadoop settings")
    hadoop_group.add_argument("--master-instance-type",
            dest="master_instance_type",
            help="instance type to use for the hadoop master node",)
    hadoop_group.add_argument("--slave-instance-type",
            dest="slave_instance_type",
            help="instance type to use for the hadoop slave nodes",)
    hadoop_group.add_argument("--initial-nodes",
            dest="initial_nodes", type=int,
            help="number of nodes to start the cluster with",)
    hadoop_group.add_argument("--max-nodes",
            dest="max_nodes", type=int,
            help="maximum number of nodes the cluster may be auto-scaled up to")
    hadoop_group.add_argument("--custom-config",
            dest="custom_config_file",
            help="location of file containg custom hadoop configuration")
    hadoop_group.add_argument("--slave-request-type",
            dest="slave_request_type", choices=["on-demand", "spot", "hybid"],
            help="purchasing option for slave instaces",)

    spot_group = argparser.add_argument_group("spot instance settings" +
            " (valid only when slave-request-type is hybrid or spot)")
    spot_group.add_argument("--maximum-bid-price-percentage",
            dest="maximum_bid_price_percentage", type=float,
            help="""maximum value to bid for spot instances
                    expressed as a percentage of the base
                    price for the slave node instance type""",)
    spot_group.add_argument("--timeout-for-spot-request",
            dest="timeout_for_request", type=int,
            help="timeout for a spot instance request unit: minutes")
    spot_group.add_argument("--maximum-spot-instance-percentage",
            dest="maximum_spot_instance_percentage", type=int,
            help="""maximum percentage of instances that may be purchased from
                    the aws spot market, valid only when slave-request-type is
                    'hybrid'""",)

    fairscheduler_group = argparser.add_argument_group(
            "fairscheduler configuration options")
    fairscheduler_group.add_argument("--fairscheduler-config-xml",
            dest="fairscheduler_config_xml_file",
            help="""location for file containing xml with custom configuration
                    for the fairscheduler""",)
    fairscheduler_group.add_argument("--fairscheduler-default-pool",
            dest="default_pool",
            help="default pool for the fairscheduler",)

    security_group = argparser.add_argument_group("security setttings")
    security_group.add_argument("--persistent-security-group",
            dest="persistent_security_groups", nargs="+",
            help="list of persistent security groups for the cluster",)
    ephemerals = security_group.add_mutually_exclusive_group()
    ephemerals.add_argument("--encrypted-ephemerals",
            dest="encrypted_ephemerals", action="store_true", default=None,
            help="encrypt the ephemeral drives on the instance",)
    ephemerals.add_argument("--no-encrypted-ephemerals",
            dest="encrypted_ephemerals", action="store_false", default=None,
            help="don't encrypt the ephemeral drives on the instance",)
    security_group.add_argument("--customer-ssh-key",
            dest="customer_ssh_key_file",
            help="location for ssh key to use to login to the instance")

    presto_group = argparser.add_argument_group("presto settings")
    presto_group.add_argument("--presto-jvm-memory",
            dest="presto_jvm_memory",
            help="maximum memory that presto jvm can use",)
    presto_group.add_argument("--presto-task-memory",
            dest="presto_task_memory",
            help="maximum memory a presto worker task can use",)

    termination = argparser.add_mutually_exclusive_group()
    termination.add_argument("--disallow-cluster-termination",
            dest="disallow_cluster_termination", action="store_true",
            default=None,
            help="""don't auto-terminate idle clusters,
                    use this with extreme caution""",)
    termination.add_argument("--allow-cluster-termination",
            dest="disallow_cluster_termination", action="store_false",
            default=None,
            help="auto-terminate idle clusters,")

    ganglia = argparser.add_mutually_exclusive_group()
    ganglia.add_argument("--enable-ganglia-monitoring",
            dest="enable_ganglia_monitoring", action="store_true", default=None,
            help="enable ganglia monitoring for the cluster",)
    ganglia.add_argument("--disable-ganglia-monitoring",
            dest="enable_ganglia_monitoring", action="store_false",
            default=None,
            help="disable ganglia monitoring for the cluster",)

    return argparser


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--token", dest="api_token",
            default=os.getenv('QDS_API_TOKEN'),
            help="""api token for accessing Qubole. must be specified via
                    command line or passed in via environment variable
                    QDS_API_TOKEN""")

    argparser.add_argument("--url", dest="api_url",
            default=os.getenv('QDS_API_URL'),
            help="""base url for QDS REST API. can be specified via
                    environment variable QDS_API_URL also.
                    defaults to https://api.qubole.com/api""")

    argparser.add_argument("--version", dest="api_version",
            default=os.getenv('QDS_API_VERSION'),
            help="""version of REST API to access. can be specified via
                  environment variable QDS_API_VERSION also.
                  defaults to v1.2""")

    argparser.add_argument("--poll_interval", dest="poll_interval",
            default=os.getenv('QDS_POLL_INTERVAL'),
            help="""interval for polling API for completion and other events.
                    can be specified via environment variable QDS_POLL_INTERVAL
                    alse. defaults to 5s""")

    argparser.add_argument("--skip_ssl_cert_check", dest="skip_ssl_cert_check",
            action="store_true",
            help="""skip verification of server SSL certificate.
                    Insecure: use with caution.""")

    argparser.add_argument("-v", dest="verbose", action="count", default=0,
            help="verbosity level. -v for info level, -vv for debug level")

    subparsers = argparser.add_subparsers(title="sub commands",
            dest="subcommand")
    cluster_parsers(subparsers)
    hadoop_cluster_parsers(subparsers)
    command_parsers(subparsers)

    arguments = argparser.parse_args()

    if arguments.verbose == 0:
        logging.basicConfig(level=logging.WARN)
    elif arguments.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif arguments.verbose > 1:
        logging.basicConfig(level=logging.DEBUG)

    if arguments.api_token is None:
        sys.stderr.write("No API Token provided\n")
        argparser.print_help()

    if arguments.api_url is None:
        arguments.api_url = "https://api.qubole.com/api/"

    if arguments.api_version is None:
        arguments.api_version = "v1.2"

    if arguments.poll_interval is None:
        arguments.poll_interval = 5

    if arguments.skip_ssl_cert_check is None:
        arguments.skip_ssl_cert_check = False
    elif arguments.skip_ssl_cert_check:
        log.warn("Insecure mode enabled: skipping SSL cert verification\n")

    Qubole.configure(api_token=arguments.api_token,
                     api_url=arguments.api_url,
                     version=arguments.api_version,
                     poll_interval=arguments.poll_interval,
                     skip_ssl_cert_check=arguments.skip_ssl_cert_check)

    arguments.func(arguments)

if __name__ == '__main__':
    try:
        sys.exit(main())
    except qds_sdk.exception.Error as e:
        sys.stderr.write("Error: Status code %s (%s) from url %s\n" %
                         (e.response.status_code, e.__class__.__name__,
                          e.response.url))
        sys.exit(1)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
