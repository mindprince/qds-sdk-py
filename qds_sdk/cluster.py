"""
The cluster module contains the definitions for retrieving and manipulating
cluster information.
"""

from qubole import Qubole
from resource import Resource

import logging

log = logging.getLogger("qds_cluster")


class Cluster(Resource):
    """
    qds_sdk.Cluster is the class for retrieving and manipulating cluster
    information.
    """

    rest_entity_path = "clusters"

    @classmethod
    def list(cls, label=None, state=None):
        """
        List existing clusters present in your account.

        Kwargs:
            `label`: list cluster with this label

            `state`: list only those clusters which are in this state

        Returns:
            List of clusters satisfying the given criteria

        Raises:
            Exception if both label and state options are provided
        """
        conn = Qubole.agent()
        if label is None and state is None:
            return conn.get(cls.rest_entity_path)
        elif label is not None and state is None:
            cluster_list = conn.get(cls.rest_entity_path)
            result = []
            for cluster in cluster_list:
                if label in cluster['cluster']['label']:
                    result.append(cluster)
            return result
        elif label is None and state is not None:
            cluster_list = conn.get(cls.rest_entity_path)
            result = []
            for cluster in cluster_list:
                if state.lower() == cluster['cluster']['state'].lower():
                    result.append(cluster)
            return result
        else:
            raise Exception("Can filter either by label or" +
                            " by state but not both")

    @classmethod
    def show(cls, cluster_id):
        """
        Show information about the cluster with id `cluster_id`.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id))

    @classmethod
    def status(cls, cluster_id):
        """
        Show the status of the cluster with id `cluster_id`.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id) + "/state")

    @classmethod
    def start(cls, cluster_id):
        """
        Start the cluster with id `cluster_id`.
        """
        conn = Qubole.agent()
        data = {"state": "start"}
        return conn.put(cls.element_path(cluster_id) + "/state", data)

    @classmethod
    def terminate(cls, cluster_id):
        """
        Terminate the cluster with id `cluster_id`.
        """
        conn = Qubole.agent()
        data = {"state": "terminate"}
        return conn.put(cls.element_path(cluster_id) + "/state", data)

    @classmethod
    def create(cls, cluster_info):
        """
        Create a new cluster using information provided in `cluster_info`.
        """
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path, data=cluster_info)

    @classmethod
    def update(cls, cluster_id, cluster_info):
        """
        Update the cluster with id `cluster_id` using information provided in
        `cluster_info`.
        """
        conn = Qubole.agent()
        return conn.put(cls.element_path(cluster_id), data=cluster_info)

    @classmethod
    def delete(cls, cluster_id):
        """
        Delete the cluster with id `cluster_id`.
        """
        conn = Qubole.agent()
        return conn.delete(cls.element_path(cluster_id))


class ClusterInfo():
    """
    qds_sdk.ClusterInfo is the class which stores information about a cluster.
    You can use objects of this class to create or update a cluster.
    """

    def __init__(self, label, aws_access_key_id, aws_secret_access_key,
                 disallow_cluster_termination=None,
                 enable_ganglia_monitoring=None):
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
        """
        self.label = label
        self.ec2_settings = {}
        self.ec2_settings['compute_access_key'] = aws_access_key_id
        self.ec2_settings['compute_secret_key'] = aws_secret_access_key
        self.disallow_cluster_termination = disallow_cluster_termination
        self.enable_ganglia_monitoring = enable_ganglia_monitoring
        self.hadoop_settings = {}
        self.security_settings = {}
        self.presto_settings = {}

    def set_ec2_settings(self,
                         aws_region=None,
                         aws_availability_zone=None):
        """
        Kwargs:

        `aws_region`: AWS region to create the cluster in.

        `aws_availability_zone`: The availability zone to create the cluster
            in.
        """
        self.ec2_settings['aws_region'] = aws_region
        self.ec2_settings['aws_preferred_availability_zone'] = aws_availability_zone

    def set_hadoop_settings(self, master_instance_type=None,
                            slave_instance_type=None,
                            initial_nodes=None,
                            max_nodes=None,
                            custom_config=None,
                            slave_request_type=None):
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
            Valid values: "on-demand", "hybrid", "spot".
        """
        self.hadoop_settings['master_instance_type'] = master_instance_type
        self.hadoop_settings['slave_instance_type'] = slave_instance_type
        self.hadoop_settings['initial_nodes'] = initial_nodes
        self.hadoop_settings['max_nodes'] = max_nodes
        self.hadoop_settings['custom_config'] = custom_config
        self.hadoop_settings['slave_request_type'] = slave_request_type

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

    def set_security_settings(self, persistent_security_groups=None,
                              encrypted_ephemerals=None,
                              customer_ssh_key=None):
        """
        Kwargs:

        `persistent_security_groups`: List of persistent security groups for
            the cluster.

        `encrypted_ephemerals`: Encrypt the ephemeral drives on the instance.

        `customer_ssh_key`: SSH key to use to login to the instances.
        """
        self.security_settings['persistent_security_groups'] = persistent_security_groups
        self.security_settings['encrypted_ephemerals'] = encrypted_ephemerals
        self.security_settings['customer_ssh_key'] = customer_ssh_key

    def set_presto_settings(self, jvm_memory=None, task_memory=None):
        """
        Kwargs:

        `jvm_memory`: The maximum memory that Presto JVM can use.

        `task_memory`: The maximum memory a worker task can use in Presto.
        """
        self.presto_settings['jvm_memory'] = jvm_memory
        self.presto_settings['task_memory'] = task_memory

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
    for key, value in dictionary.iteritems():
        if value is not None:
            if isinstance(value, dict):
                new_value = _make_minimal(value)
                if new_value:
                    new_dict[key] = new_value
            else:
                new_dict[key] = value
    return new_dict
