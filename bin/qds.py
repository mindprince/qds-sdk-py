#!/bin/env python

from __future__ import print_function
from qds_sdk.qubole import Qubole
from qds_sdk.commands import *
from qds_sdk.cluster import *
import qds_sdk.exception
from qds_sdk.scheduler import SchedulerCmdLine
from qds_sdk.actions import ActionCmdLine
from qds_sdk.report import ReportCmdLine
from qds_sdk.dbtaps import DbTapCmdLine
from qds_sdk.role import RoleCmdLine
from qds_sdk.group import GroupCmdLine

import os
import sys
import traceback
import logging
import json
from optparse import OptionParser

log = logging.getLogger("qds")
CommandClasses = {
    "hivecmd": HiveCommand,
    "sparkcmd": SparkCommand,
    "dbtapquerycmd": DbTapQueryCommand,
    "pigcmd":  PigCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand,
    "prestocmd": PrestoCommand
}

usage_str = ("Usage: \n"
             "qds.py [options] <CmdArgs|ClusterArgs|ReportArgs>\n"
             "\nCmdArgs:\n" +
             "  <hivecmd|hadoopcmd|prestocmd|pigcmd|shellcmd|dbexportcmd|dbtapquerycmd|sparkcmd> <submit|run|check|cancel|getresult|getlog> [args .. ]\n"
             "  submit [cmd-specific-args .. ] : submit cmd & print id \n"
             "  run [cmd-specific-args .. ] : submit cmd & wait. print results \n"
             "  check <id> : print the cmd object for this Id\n"
             "  cancel <id> : cancels the cmd with this Id\n"
             "  getresult <id> : get the results for the cmd with this Id\n"
             "  getlog <id> : get the logs for the cmd with this Id\n"
             "\nCluster:\n" +
             "  cluster --help\n"
             "\nDbTap:\n" +
             "  dbtap --help\n" +
             "\nReportArgs:\n" +
             "  report --help\n" +
             "\nGroup:\n" +
             "  group --help\n" +
             "\nRole:\n" +
             "  role --help\n" +
            "\nScheduler:\n" +
             "  scheduler --help\n")


def usage(parser=None):
    if parser is None:
        sys.stderr.write(usage_str)
    else:
        parser.print_help()
    sys.exit(1)


def checkargs_id(args):
    if len(args) != 1:
        sys.stderr.write("expecting single argument command id\n")
        usage()


def submitaction(cmdclass, args):
    args = cmdclass.parse(args)
    if args is not None:
        args.pop("print_logs") # This is only useful while using the 'run' action.
        cmd = cmdclass.create(**args)
        print("Submitted %s, Id: %s" % (cmdclass.__name__, cmd.id))
        return 0


def _getresult(cmdclass, cmd):
    if Command.is_success(cmd.status):
        log.info("Fetching results for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        cmd.get_results(sys.stdout, delim='\t')
        return 0
    else:
        log.error("Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status))
        return 1


def runaction(cmdclass, args):
    args = cmdclass.parse(args)
    if args is not None:
        print_logs = args.pop("print_logs") # We don't want to send this to the API.
        cmd = cmdclass.run(**args)
        if print_logs:
            sys.stderr.write(cmd.get_log())
        return _getresult(cmdclass, cmd)


def checkaction(cmdclass, args):
    checkargs_id(args)
    o = cmdclass.find(args.pop(0))
    print(str(o))
    return 0


def cancelaction(cmdclass, args):
    checkargs_id(args)
    r = cmdclass.cancel_id(args.pop(0))
    skey = 'kill_succeeded'
    if r.get(skey) is None:
        sys.stderr.write("Invalid Json Response %s - missing field '%s'" % (str(r), skey))
        return 11
    elif r['kill_succeeded']:
        print("Command killed successfully")
        return 0
    else:
        sys.stderr.write("Cancel failed with reason '%s'\n" % r.get('result'))
        return 12


def getresultaction(cmdclass, args):
    checkargs_id(args)
    cmd = cmdclass.find(args.pop(0))
    return _getresult(cmdclass, cmd)


def getlogaction(cmdclass, args):
    checkargs_id(args)
    print(cmdclass.get_log_id(args.pop(0)))
    return 0


def getjobsaction(cmdclass, args):
    checkargs_id(args)
    cmd = cmdclass.find(args.pop(0))
    if Command.is_done(cmd.status):
        log.info("Fetching jobs for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        print(cmdclass.get_jobs_id(cmd.id))
        return 0
    else:
        log.error("Cannot fetch jobs - command Id: %s is not done. Status: %s" % (cmd.id, cmd.status))
        return 1


def cmdmain(cmd, args):
    cmdclass = CommandClasses[cmd]

    actionset = set(["submit", "run", "check", "cancel", "getresult", "getlog", "getjobs"])
    if len(args) < 1:
        sys.stderr.write("missing argument containing action\n")
        usage()

    action = args.pop(0)
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()

    return globals()[action + "action"](cmdclass, args)


def clustermain(args):
    result = ClusterCmdLine.run(args)
    print(json.dumps(result, indent=4))
    return 0


def reportmain(args):
    result = ReportCmdLine.run(args)
    print(result)


def actionmain(args):
    result = ActionCmdLine.run(args)
    print(result)

def schedulermain(args):
    result = SchedulerCmdLine.run(args)
    print(result)

def dbtapmain(args):
    result = DbTapCmdLine.run(args)
    print(result)

def rolemain(args):
    result = RoleCmdLine.run(args)
    print(result)

def groupmain(args):
    result = GroupCmdLine.run(args)
    print(result)

def main():

    optparser = OptionParser(usage=usage_str)
    optparser.add_option("--token", dest="api_token",
                         default=os.getenv('QDS_API_TOKEN'),
                         help="api token for accessing Qubole. must be specified via command line or passed in via environment variable QDS_API_TOKEN")

    optparser.add_option("--url", dest="api_url",
                         default=os.getenv('QDS_API_URL'),
                         help="base url for QDS REST API. defaults to https://api.qubole.com/api ")

    optparser.add_option("--version", dest="api_version",
                         default=os.getenv('QDS_API_VERSION'),
                         help="version of REST API to access. defaults to v1.2")

    optparser.add_option("--poll_interval", dest="poll_interval",
                         default=os.getenv('QDS_POLL_INTERVAL'),
                         help="interval for polling API for completion and other events. defaults to 5s")

    optparser.add_option("--skip_ssl_cert_check", dest="skip_ssl_cert_check", action="store_true",
                         default=False,
                         help="skip verification of server SSL certificate. Insecure: use with caution.")

    optparser.add_option("-v", dest="verbose", action="store_true",
                         default=False,
                         help="verbose mode - info level logging")

    optparser.add_option("--vv", dest="chatty", action="store_true",
                         default=False,
                         help="very verbose mode - debug level logging")

    optparser.disable_interspersed_args()
    (options, args) = optparser.parse_args()

    if options.chatty:
        logging.basicConfig(level=logging.DEBUG)
    elif options.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if options.api_token is None:
        sys.stderr.write("No API Token provided\n")
        usage(optparser)

    if options.api_url is None:
        options.api_url = "https://api.qubole.com/api/"

    if options.api_version is None:
        options.api_version = "v1.2"

    if options.poll_interval is None:
        options.poll_interval = 5

    if options.skip_ssl_cert_check is None:
        options.skip_ssl_cert_check = False
    elif options.skip_ssl_cert_check:
        log.warn("Insecure mode enabled: skipping SSL cert verification\n")

    Qubole.configure(api_token=options.api_token,
                     api_url=options.api_url,
                     version=options.api_version,
                     poll_interval=options.poll_interval,
                     skip_ssl_cert_check=options.skip_ssl_cert_check)

    if len(args) < 1:
        sys.stderr.write("Missing first argument containing command type\n")
        usage(optparser)

    a0 = args.pop(0)
    if a0 in CommandClasses:
        return cmdmain(a0, args)

    if a0 == "cluster":
        return clustermain(args)

    if a0 == "action":
        return actionmain(args)

    if a0 == "scheduler":
        return schedulermain(args)

    if a0 == "report":
        return reportmain(args)

    if a0 == "dbtap":
        return dbtapmain(args)

    if a0 == "group":
        return groupmain(args)

    if a0 == "role":
        return rolemain(args)

    cmdset = set(CommandClasses.keys())
    sys.stderr.write("First command must be one of <%s>\n" %
                     "|".join(cmdset.union(["cluster", "scheduler", "report",
                       "dbtap", "role", "group"])))
    usage(optparser)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except qds_sdk.exception.Error as e:
        sys.stderr.write("Error: Status code %s (%s) from url %s\n" %
                         (e.request.status_code, e.__class__.__name__,
                          e.request.url))
        sys.exit(1)
    except qds_sdk.exception.ParseError as e:
        sys.stderr.write("Error: %s\n" % str(e))
        sys.stderr.write("Usage: %s\n" % e.usage)
        sys.exit(2)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
