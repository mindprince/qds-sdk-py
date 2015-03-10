"""
The cluster module contains the definitions for retrieving and manipulating
cluster information.
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import sys
import logging
import json

log = logging.getLogger("qds_cluster")

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


class ClusterCmdLine:
    """
    qds_sdk.ClusterCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def run(args):
        parser = ClusterCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py cluster",
                description="Cluster client for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="cluster actions")

        cluster_create = subparsers.add_parser("create",
                description="create a new cluster")
        ClusterCmdLine._parse_create_update(cluster_create, "create")
        cluster_create.set_defaults(func=ClusterCmdLine.cluster_create_action)

        cluster_update = subparsers.add_parser("update",
                description="update the settings of an existing cluster")
        ClusterCmdLine._parse_create_update(cluster_update, "update")
        cluster_update.set_defaults(func=ClusterCmdLine.cluster_update_action)

        cluster_clone = subparsers.add_parser("clone",
                description="clone a cluster from an existing one")
        ClusterCmdLine._parse_create_update(cluster_clone, "clone")
        cluster_clone.set_defaults(func=ClusterCmdLine.cluster_clone_action)

        cluster_list = subparsers.add_parser("list",
                description="list existing cluster(s)")
        group = cluster_list.add_mutually_exclusive_group()
        group.add_argument("--id", dest="cluster_id",
                help="show cluster with this id")
        group.add_argument("--label", dest="label",
                help="show cluster with this label")
        group.add_argument("--state", dest="state", action="store",
                choices=['up', 'down', 'pending', 'terminating'],
                help="list only clusters in the given state")
        cluster_list.set_defaults(func=ClusterCmdLine.cluster_list_action)

        cluster_delete = subparsers.add_parser("delete",
                description="delete an existing cluster")
        cluster_delete.add_argument("cluster_id_or_label",
                help="id/label of the cluster")
        cluster_delete.set_defaults(func=ClusterCmdLine.cluster_delete_action)

        cluster_start = subparsers.add_parser("start",
                description="start an existing cluster")
        cluster_start.add_argument("cluster_id_or_label",
                help="id/label of the cluster")
        cluster_start.set_defaults(func=ClusterCmdLine.cluster_start_action)

        cluster_terminate = subparsers.add_parser("terminate",
                description="terminate a running cluster")
        cluster_terminate.add_argument("cluster_id_or_label",
                help="id/label of the cluster")
        cluster_terminate.set_defaults(func=ClusterCmdLine.cluster_terminate_action)

        cluster_status = subparsers.add_parser("status",
                description="show whether the cluster is up or down")
        cluster_status.add_argument("cluster_id_or_label",
                help="id/label of the cluster")
        cluster_status.set_defaults(func=ClusterCmdLine.cluster_status_action)

        cluster_reassign_label = subparsers.add_parser("reassign_label",
                description="reassign label from one cluster to another")
        cluster_reassign_label.add_argument("destination_cluster",
                metavar="destination_cluster_id_label",
                help="id/label of the cluster to move the label to")
        cluster_reassign_label.add_argument("label",
                help="label to be moved from the source cluster")
        cluster_reassign_label.set_defaults(func=ClusterCmdLine.cluster_reassign_label_action)

        return argparser

    @staticmethod
    def _parse_create_update(argparser, action):
        """
        Parse command line arguments to determine cluster parameters that can
        be used to create or update a cluster.

        Args:
            `args`: sequence of arguments

            `action`: "create", "update" or "clone"

        Returns:
            Object that contains cluster parameters
        """
        create_required = False
        label_required = False

        if action == "create":
            create_required = True
        elif action == "update":
            argparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
        elif action == "clone":
            argparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
            label_required = True

        argparser.add_argument("--label", dest="label",
                               nargs="+", required=(create_required or label_required),
                               help="list of labels for the cluster" +
                                    " (atleast one label is required)")

        ec2_group = argparser.add_argument_group("ec2 settings")
        ec2_group.add_argument("--access-key-id",
                               dest="aws_access_key_id",
                               required=create_required,
                               help="access key id for customer's aws" +
                                    " account. This is required while" +
                                    " creating the cluster",)
        ec2_group.add_argument("--secret-access-key",
                               dest="aws_secret_access_key",
                               required=create_required,
                               help="secret access key for customer's aws" +
                                    " account. This is required while" +
                                    " creating the cluster",)
        ec2_group.add_argument("--aws-region",
                               dest="aws_region",
                               choices=["us-east-1", "us-west-2", "ap-northeast-1",
                                        "eu-west-1", "ap-southeast-1", "us-west-1"],
                               help="aws region to create the cluster in",)
        ec2_group.add_argument("--aws-availability-zone",
                               dest="aws_availability_zone",
                               help="availability zone to" +
                                    " create the cluster in",)
        ec2_group.add_argument("--subnet-id",
                               dest="subnet_id",
                               help="subnet to create the cluster in",)
        ec2_group.add_argument("--vpc-id",
                               dest="vpc_id",
                               help="vpc to create the cluster in",)

        hadoop_group = argparser.add_argument_group("hadoop settings")
        hadoop_group.add_argument("--master-instance-type",
                                  dest="master_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " master node",)
        hadoop_group.add_argument("--slave-instance-type",
                                  dest="slave_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " slave nodes",)
        hadoop_group.add_argument("--initial-nodes",
                                  dest="initial_nodes",
                                  type=int,
                                  help="number of nodes to start the" +
                                       " cluster with",)
        hadoop_group.add_argument("--max-nodes",
                                  dest="max_nodes",
                                  type=int,
                                  help="maximum number of nodes the cluster" +
                                       " may be auto-scaled up to")
        hadoop_group.add_argument("--custom-config",
                                  dest="custom_config_file",
                                  help="location of file containg custom" +
                                       " hadoop configuration overrides")
        hadoop_group.add_argument("--slave-request-type",
                                  dest="slave_request_type",
                                  choices=["ondemand", "spot", "hybrid"],
                                  help="purchasing option for slave instaces",)
        hadoop_group.add_argument("--use-hbase", dest="use_hbase",
                                  action="store_true", default=None,
                                  help="Use hbase on this cluster",)

        spot_group = argparser.add_argument_group("spot instance settings" +
                    " (valid only when slave-request-type is hybrid or spot)")
        spot_group.add_argument("--maximum-bid-price-percentage",
                                dest="maximum_bid_price_percentage",
                                type=float,
                                help="maximum value to bid for spot instances" +
                                     " expressed as a percentage of the base" +
                                     " price for the slave node instance type",)
        spot_group.add_argument("--timeout-for-spot-request",
                                dest="timeout_for_request",
                                type=int,
                                help="timeout for a spot instance request" +
                                     " unit: minutes")
        spot_group.add_argument("--maximum-spot-instance-percentage",
                                dest="maximum_spot_instance_percentage",
                                type=int,
                                help="maximum percentage of instances that may" +
                                     " be purchased from the aws spot market," +
                                     " valid only when slave-request-type" +
                                     " is 'hybrid'",)

        stable_spot_group = argparser.add_argument_group("stable spot instance settings")
        stable_spot_group.add_argument("--stable-maximum-bid-price-percentage",
                                       dest="stable_maximum_bid_price_percentage",
                                       type=float,
                                       help="maximum value to bid for stable node spot instances" +
                                       " expressed as a percentage of the base" +
                                       " price for the master and slave node instance types",)
        stable_spot_group.add_argument("--stable-timeout-for-spot-request",
                                       dest="stable_timeout_for_request",
                                       type=int,
                                       help="timeout for a stable node spot instance request" +
                                       " unit: minutes")
        stable_spot_group.add_argument("--stable-allow-fallback",
                                       dest="stable_allow_fallback", default=None,
                                       type=str2bool,
                                       help="whether to fallback to on-demand instances for stable nodes" +
                                       " if spot instances aren't available")

        fairscheduler_group = argparser.add_argument_group(
                              "fairscheduler configuration options")
        fairscheduler_group.add_argument("--fairscheduler-config-xml",
                                         dest="fairscheduler_config_xml_file",
                                         help="location for file containing" +
                                              " xml with custom configuration" +
                                              " for the fairscheduler",)
        fairscheduler_group.add_argument("--fairscheduler-default-pool",
                                         dest="default_pool",
                                         help="default pool for the" +
                                              " fairscheduler",)

        security_group = argparser.add_argument_group("security setttings")
        ephemerals = security_group.add_mutually_exclusive_group()
        ephemerals.add_argument("--encrypted-ephemerals",
                                 dest="encrypted_ephemerals",
                                 action="store_true",
                                 default=None,
                                 help="encrypt the ephemeral drives on" +
                                      " the instance",)
        ephemerals.add_argument("--no-encrypted-ephemerals",
                                 dest="encrypted_ephemerals",
                                 action="store_false",
                                 default=None,
                                 help="don't encrypt the ephemeral drives on" +
                                      " the instance",)
        security_group.add_argument("--customer-ssh-key",
                                    dest="customer_ssh_key_file",
                                    help="location for ssh key to use to" +
                                         " login to the instance")

        presto_group = argparser.add_argument_group("presto settings")
        enabling_presto = presto_group.add_mutually_exclusive_group()
        enabling_presto.add_argument("--enable-presto",
                                  dest="enable_presto",
                                  action="store_true",
                                  default=None,
                                  help="Enable presto for this cluster",)
        enabling_presto.add_argument("--disable-presto",
                                  dest="enable_presto",
                                  action="store_false",
                                  default=None,
                                  help="Disable presto for this cluster",)
        presto_group.add_argument("--presto-custom-config",
                                  dest="presto_custom_config_file",
                                  help="location of file containg custom" +
                                       " presto configuration overrides")

        termination = argparser.add_mutually_exclusive_group()
        termination.add_argument("--disallow-cluster-termination",
                                 dest="disallow_cluster_termination",
                                 action="store_true",
                                 default=None,
                                 help="don't auto-terminate idle clusters," +
                                      " use this with extreme caution",)
        termination.add_argument("--allow-cluster-termination",
                                 dest="disallow_cluster_termination",
                                 action="store_false",
                                 default=None,
                                 help="auto-terminate idle clusters,")

        ganglia = argparser.add_mutually_exclusive_group()
        ganglia.add_argument("--enable-ganglia-monitoring",
                             dest="enable_ganglia_monitoring",
                             action="store_true",
                             default=None,
                             help="enable ganglia monitoring for the" +
                                  " cluster",)
        ganglia.add_argument("--disable-ganglia-monitoring",
                             dest="enable_ganglia_monitoring",
                             action="store_false",
                             default=None,
                             help="disable ganglia monitoring for the" +
                                  " cluster",)

        argparser.add_argument("--node-bootstrap-file",
                dest="node_bootstrap_file",
                help="""name of the node bootstrap file for this cluster. It
                should be in stored in S3 at
                <account-default-location>/scripts/hadoop/NODE_BOOTSTRAP_FILE
                """,)

        argparser.add_argument("--custom-ec2-tags",
                               dest="custom_ec2_tags",
                               help="""Custom ec2 tags to be set on all instances
                               of the cluster. Specified as JSON object (key-value pairs)
                               e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                               """,)

    @staticmethod
    def cluster_create_action(args):
        cluster_info = ClusterCmdLine._create_cluster_info(args)
        return Cluster.create(cluster_info.minimal_payload())

    @staticmethod
    def cluster_update_action(args):
        cluster_info = ClusterCmdLine._create_cluster_info(args)
        return Cluster.update(args.cluster_id_label, cluster_info.minimal_payload())

    @staticmethod
    def cluster_clone_action(args):
        cluster_info = ClusterCmdLine._create_cluster_info(args)
        return Cluster.clone(args.cluster_id_label, cluster_info.minimal_payload())

    @staticmethod
    def _create_cluster_info(arguments):
        cluster_info = ClusterInfo(arguments.label,
                                   arguments.aws_access_key_id,
                                   arguments.aws_secret_access_key,
                                   arguments.disallow_cluster_termination,
                                   arguments.enable_ganglia_monitoring,
                                   arguments.node_bootstrap_file,)

        cluster_info.set_ec2_settings(arguments.aws_region,
                                      arguments.aws_availability_zone,
                                      arguments.vpc_id,
                                      arguments.subnet_id)

        custom_config = None
        if arguments.custom_config_file is not None:
            try:
                custom_config = open(arguments.custom_config_file).read()
            except IOError as e:
                sys.stderr.write("Unable to read custom config file: %s\n" %
                                 str(e))
                sys.exit(1)

        cluster_info.set_hadoop_settings(arguments.master_instance_type,
                                         arguments.slave_instance_type,
                                         arguments.initial_nodes,
                                         arguments.max_nodes,
                                         custom_config,
                                         arguments.slave_request_type,
                                         arguments.use_hbase,
                                         arguments.custom_ec2_tags)

        cluster_info.set_spot_instance_settings(
              arguments.maximum_bid_price_percentage,
              arguments.timeout_for_request,
              arguments.maximum_spot_instance_percentage)

        cluster_info.set_stable_spot_instance_settings(
              arguments.stable_maximum_bid_price_percentage,
              arguments.stable_timeout_for_request,
              arguments.stable_allow_fallback)

        fairscheduler_config_xml = None
        if arguments.fairscheduler_config_xml_file is not None:
            try:
                fairscheduler_config_xml = open(arguments.fairscheduler_config_xml_file).read()
            except IOError as e:
                sys.stderr.write("Unable to read config xml file: %s\n" %
                                 str(e))
                sys.exit(1)
        cluster_info.set_fairscheduler_settings(fairscheduler_config_xml,
                                                arguments.default_pool)

        customer_ssh_key = None
        if arguments.customer_ssh_key_file is not None:
            try:
                customer_ssh_key = open(arguments.customer_ssh_key_file).read()
            except IOError as e:
                sys.stderr.write("Unable to read customer ssh key file: %s\n" %
                                 str(e))
                sys.exit(1)
        cluster_info.set_security_settings(arguments.encrypted_ephemerals,
                                           customer_ssh_key)

        presto_custom_config = None
        if arguments.presto_custom_config_file is not None:
            try:
                presto_custom_config = open(arguments.presto_custom_config_file).read()
            except IOError as e:
                sys.stderr.write("Unable to read presto custom config file: %s\n" %
                                 str(e))
                sys.exit(1)
        cluster_info.set_presto_settings(arguments.enable_presto,
                                         presto_custom_config)

        return cluster_info

    @staticmethod
    def cluster_delete_action(args):
        return Cluster.delete(args.cluster_id_or_label)

    @staticmethod
    def cluster_list_action(args):
        arguments = vars(args)
        if arguments['cluster_id'] is not None:
            result = Cluster.show(arguments['cluster_id'])
        elif arguments['label'] is not None:
            result = Cluster.show(arguments['label'])
        elif arguments['state'] is not None:
            result = Cluster.list(state=arguments['state'])
        else:
            result = Cluster.list()
        return result

    @staticmethod
    def cluster_start_action(args):
        return Cluster.start(args.cluster_id_or_label)

    @staticmethod
    def cluster_terminate_action(args):
        return Cluster.terminate(args.cluster_id_or_label)

    @staticmethod
    def cluster_status_action(args):
        return Cluster.status(args.cluster_id_or_label)

    @staticmethod
    def cluster_reassign_label_action(args):
        return Cluster.reassign_label(args.destination_cluster, args.label)


class Cluster(Resource):
    """
    qds_sdk.Cluster is the class for retrieving and manipulating cluster
    information.
    """

    rest_entity_path = "clusters"

    @classmethod
    def list(cls, state=None):
        """
        List existing clusters present in your account.

        Kwargs:
            `state`: list only those clusters which are in this state

        Returns:
            List of clusters satisfying the given criteria
        """
        conn = Qubole.agent()
        if state is None:
            return conn.get(cls.rest_entity_path)
        elif state is not None:
            cluster_list = conn.get(cls.rest_entity_path)
            result = []
            for cluster in cluster_list:
                if state.lower() == cluster['cluster']['state'].lower():
                    result.append(cluster)
            return result

    @classmethod
    def show(cls, cluster_id_label):
        """
        Show information about the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id_label))

    @classmethod
    def status(cls, cluster_id_label):
        """
        Show the status of the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id_label) + "/state")

    @classmethod
    def start(cls, cluster_id_label):
        """
        Start the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        data = {"state": "start"}
        return conn.put(cls.element_path(cluster_id_label) + "/state", data)

    @classmethod
    def terminate(cls, cluster_id_label):
        """
        Terminate the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        data = {"state": "terminate"}
        return conn.put(cls.element_path(cluster_id_label) + "/state", data)


    @classmethod
    def create(cls, cluster_info):
        """
        Create a new cluster using information provided in `cluster_info`.
        """
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path, data=cluster_info)

    @classmethod
    def update(cls, cluster_id_label, cluster_info):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.
        """
        conn = Qubole.agent()
        return conn.put(cls.element_path(cluster_id_label), data=cluster_info)

    @classmethod
    def clone(cls, cluster_id_label, cluster_info):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.
        """
        conn = Qubole.agent()
        return conn.post(cls.element_path(cluster_id_label) + '/clone', data=cluster_info)

    @classmethod
    def reassign_label(cls, destination_cluster, label):
        """
        Reassign a label from one cluster to another.

        Args:
            `destination_cluster`: id/label of the cluster to move the label to

            `label`: label to be moved from the source cluster
        """
        conn = Qubole.agent()
        data = {
                    "destination_cluster": destination_cluster,
                    "label": label
                }
        return conn.put(cls.rest_entity_path + "/reassign-label", data)

    @classmethod
    def delete(cls, cluster_id_label):
        """
        Delete the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        return conn.delete(cls.element_path(cluster_id_label))


class ClusterInfo():
    """
    qds_sdk.ClusterInfo is the class which stores information about a cluster.
    You can use objects of this class to create or update a cluster.
    """

    def __init__(self, label, aws_access_key_id, aws_secret_access_key,
                 disallow_cluster_termination=None,
                 enable_ganglia_monitoring=None,
                 node_bootstrap_file=None):
        """
        Args:

        `label`: A list of labels that identify the cluster. At least one label
            must be provided when creating a cluster.

        `aws_access_key_id`: The access key id for customer's aws account. This
            is required for creating the cluster.

        `aws_secret_access_key`: The secret access key for customer's aws
            account. This is required for creating the cluster.

        `disallow_cluster_termination`: Set this to True if you don't want
            qubole to auto-terminate idle clusters. Use this option with
            extreme caution.

        `enable_ganglia_monitoring`: Set this to True if you want to enable
            ganglia monitoring for the cluster.

        `node_bootstrap_file`: name of the node bootstrap file for this
            cluster. It should be in stored in S3 at
            <your-default-location>/scripts/hadoop/
        """
        self.label = label
        self.ec2_settings = {}
        self.ec2_settings['compute_access_key'] = aws_access_key_id
        self.ec2_settings['compute_secret_key'] = aws_secret_access_key
        self.disallow_cluster_termination = disallow_cluster_termination
        self.enable_ganglia_monitoring = enable_ganglia_monitoring
        self.node_bootstrap_file = node_bootstrap_file
        self.hadoop_settings = {}
        self.security_settings = {}
        self.presto_settings = {}

    def set_ec2_settings(self,
                         aws_region=None,
                         aws_availability_zone=None,
                         vpc_id=None,
                         subnet_id=None):
        """
        Kwargs:

        `aws_region`: AWS region to create the cluster in.

        `aws_availability_zone`: The availability zone to create the cluster
            in.

        `vpc_id`: The vpc to create the cluster in.

        `subnet_id`: The subnet to create the cluster in.
        """
        self.ec2_settings['aws_region'] = aws_region
        self.ec2_settings['aws_preferred_availability_zone'] = aws_availability_zone
        self.ec2_settings['vpc_id'] = vpc_id
        self.ec2_settings['subnet_id'] = subnet_id

    def set_hadoop_settings(self, master_instance_type=None,
                            slave_instance_type=None,
                            initial_nodes=None,
                            max_nodes=None,
                            custom_config=None,
                            slave_request_type=None,
                            use_hbase=None,
                            custom_ec2_tags=None):
        """
        Kwargs:

        `master_instance_type`: The instance type to use for the Hadoop master
            node.

        `slave_instance_type`: The instance type to use for the Hadoop slave
            nodes.

        `initial_nodes`: Number of nodes to start the cluster with.

        `max_nodes`: Maximum number of nodes the cluster may be auto-scaled up
            to.

        `custom_config`: Custom Hadoop configuration overrides.

        `slave_request_type`: Purchasing option for slave instances.
            Valid values: "ondemand", "hybrid", "spot".

        `use_hbase`: Start hbase daemons on the cluster. Uses Hadoop2
        """
        self.hadoop_settings['master_instance_type'] = master_instance_type
        self.hadoop_settings['slave_instance_type'] = slave_instance_type
        self.hadoop_settings['initial_nodes'] = initial_nodes
        self.hadoop_settings['max_nodes'] = max_nodes
        self.hadoop_settings['custom_config'] = custom_config
        self.hadoop_settings['slave_request_type'] = slave_request_type
        self.hadoop_settings['use_hbase'] = use_hbase

        if custom_ec2_tags and custom_ec2_tags.strip():
            try:
                self.hadoop_settings['custom_ec2_tags'] = json.loads(custom_ec2_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

    def set_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                   timeout_for_request=None,
                                   maximum_spot_instance_percentage=None):
        """
        Purchase options for spot instances. Valid only when
        `slave_request_type` is hybrid or spot.

        `maximum_bid_price_percentage`: Maximum value to bid for spot
            instances, expressed as a percentage of the base price for the
            slave node instance type.

        `timeout_for_request`: Timeout for a spot instance request (Unit:
            minutes)

        `maximum_spot_instance_percentage`: Maximum percentage of instances
            that may be purchased from the AWS Spot market. Valid only when
            slave_request_type is "hybrid".
        """
        self.hadoop_settings['spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'maximum_spot_instance_percentage': maximum_spot_instance_percentage}


    def set_stable_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                          timeout_for_request=None,
                                          allow_fallback=True):
        """
        Purchase options for stable spot instances.

        `maximum_bid_price_percentage`: Maximum value to bid for stable node spot
            instances, expressed as a percentage of the base price
            (applies to both master and slave nodes).

        `timeout_for_request`: Timeout for a stable node spot instance request (Unit:
            minutes)

        `allow_fallback`: Whether to fallback to on-demand instances for
            stable nodes if spot instances are not available
        """
        self.hadoop_settings['stable_spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'allow_fallback': allow_fallback}


    def set_fairscheduler_settings(self, fairscheduler_config_xml=None,
                                   default_pool=None):
        """
        Fair scheduler configuration options.

        `fairscheduler_config_xml`: XML string with custom configuration
            parameters for the fair scheduler.

        `default_pool`: The default pool for the fair scheduler.
        """
        self.hadoop_settings['fairscheduler_settings'] = {
               'fairscheduler_config_xml': fairscheduler_config_xml,
               'default_pool': default_pool}

    def set_security_settings(self,
                              encrypted_ephemerals=None,
                              customer_ssh_key=None):
        """
        Kwargs:

        `encrypted_ephemerals`: Encrypt the ephemeral drives on the instance.

        `customer_ssh_key`: SSH key to use to login to the instances.
        """
        self.security_settings['encrypted_ephemerals'] = encrypted_ephemerals
        self.security_settings['customer_ssh_key'] = customer_ssh_key

    def set_presto_settings(self, enable_presto=None, presto_custom_config=None):
        """
        Kwargs:

        `enable_presto`: Enable Presto on the cluster.

        `presto_custom_config`: Custom Presto configuration overrides.
        """
        self.presto_settings['enable_presto'] = enable_presto
        self.presto_settings['custom_config'] = presto_custom_config

    def minimal_payload(self):
        """
        This method can be used to create the payload which is sent while
        creating or updating a cluster.
        """
        payload = {"cluster": self.__dict__}
        return _make_minimal(payload)


def _make_minimal(dictionary):
    """
    This function removes all the keys whose value is either None or an empty
    dictionary.
    """
    new_dict = {}
    for key, value in dictionary.items():
        if value is not None:
            if isinstance(value, dict):
                new_value = _make_minimal(value)
                if new_value:
                    new_dict[key] = new_value
            else:
                new_dict[key] = value
    return new_dict
