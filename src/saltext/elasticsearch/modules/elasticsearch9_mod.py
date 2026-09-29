"""
Salt execution module

Elasticsearch - A distributed RESTful search and analytics server for Elasticsearch 9
Module to provide Elasticsearch compatibility to Salt
(compatible with Elasticsearch version 9+).  Copied from elasticsearch8.py module and updated.

.. versionadded:: 1.3.0

:codeauthor: Cesar Sanchez <cesan3@gmail.com>

:depends: elasticsearch-py <https://elasticsearch-py.readthedocs.io/en/latest/>

:configuration: This module accepts connection configuration details either as
                parameters or as configuration settings in /etc/salt/minion on the relevant
                minions:

.. code-block:: yaml

    elasticsearch:
      host: '10.10.10.100:9200'

    elasticsearch-cluster:
      hosts:
        - '10.10.10.100:9200'
        - '10.10.10.101:9200'
        - '10.10.10.102:9200'

    elasticsearch-extra:
      hosts:
        - '10.10.10.100:9200'
      use_ssl: True
      verify_certs: True
      ca_certs: /path/to/custom_ca_bundle.pem
      number_of_shards: 1
      number_of_replicas: 0
      functions_blacklist:
        - 'saltutil.find_job'
        - 'pillar.items'
        - 'grains.items'

      proxies:
        - http: http://proxy:3128
        - https: http://proxy:1080

When specifying proxies the requests backend will be used and the 'proxies'
data structure is passed as-is to that module.

This data can also be passed into pillar. Options passed into opts will
overwrite options passed into pillar.

Some functionality might be limited by elasticsearch-py and Elasticsearch server versions.
"""

# pylint: disable=too-many-lines
import logging
import re

import salt.utils.json
from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError

log = logging.getLogger(__name__)

try:
    import elasticsearch8 as elasticsearch
    from elastic_transport import RequestsHttpNode

    HAS_ELASTICSEARCH = True
    ES_MAJOR_VERSION = elasticsearch.__version__[0]
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)
except ImportError:
    HAS_ELASTICSEARCH = False
    ES_MAJOR_VERSION = 0

__virtualname__ = "elasticsearch"


def __virtual__():
    """
    Only load if elasticsearch librarielastic exist and ES version is 9+.
    """
    if not HAS_ELASTICSEARCH:
        return (
            False,
            "Cannot load module elasticsearch: elasticsearch librarielastic not found",
        )
    if ES_MAJOR_VERSION < 9:
        return (False, "Cannot load the module, elasticserach version is not 9+")

    return __virtualname__


def _get_instance(hosts=None, profile=None):
    """
    Return the elasticsearch instance
    """
    elastic = None
    proxies = None
    ca_certs = None
    verify_certs = True
    http_auth = None
    timeout = 10
    _profile = None

    if profile is None:
        profile = "elasticsearch"

    if isinstance(profile, str):
        _profile = __salt__["config.option"](profile)
    elif isinstance(profile, dict):
        _profile = profile
    if _profile:
        hosts = _profile.get("host", hosts)
        if not hosts:
            hosts = _profile.get("hosts", hosts)
        proxies = _profile.get("proxies", None)
        ca_certs = _profile.get("ca_certs", None)
        verify_certs = _profile.get("verify_certs", True)
        username = _profile.get("username", None)
        password = _profile.get("password", None)
        timeout = _profile.get("timeout", 10)

        if username and password:
            http_auth = (username, password)

    if hosts is None:
        hosts = ["http://127.0.0.1:9200"]
    if isinstance(hosts, str):
        _re = re.compile(r"(http[s]*://)(.*)")
        match = _re.match(hosts)
        if match:
            schema, hostport = match.groups()
            host, port = hostport.split(":")
            if port is None:
                port = "9200"
            hosts = [f"{schema}{host}:{port}"]
        else:
            hosts = [hosts]
    try:
        if proxies:
            elastic = elasticsearch.Elasticsearch(
                hosts,
                ca_certs=ca_certs,
                verify_certs=verify_certs,
                http_auth=http_auth,
                request_timeout=timeout,
                node_class=RequestsHttpNode,
            )
        else:
            elastic = elasticsearch.Elasticsearch(
                hosts,
                ca_certs=ca_certs,
                verify_certs=verify_certs,
                http_auth=http_auth,
                request_timeout=timeout,
            )

        # Try the connection
        elastic.info()
    except elasticsearch.exceptions.TransportError as err:
        raise CommandExecutionError(
            f"Could not connect to Elasticsearch host/ cluster {hosts} due to {err.errors}"
        ) from err
    return elastic


def ping(
    hosts=None,
    profile=None,
    allow_failure=False,
):
    """
    .. versionadded:: 3005.1

    Test connection to Elasticsearch instance. This method does not fail if not explicitly specified.

    allow_failure
        Throw exception if ping fails

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.ping allow_failure=True
        salt myminion elasticsearch.ping profile=elasticsearch-extra
    """
    try:
        _get_instance(hosts=hosts, profile=profile)
    except CommandExecutionError:
        if allow_failure:
            raise
        return False
    return True


def info(
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
):
    """
    .. versionadded:: 2017.7.0

    Return Elasticsearch information.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.info
        salt myminion elasticsearch.info profile=elasticsearch-extra
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        return elastic.info(
            error_trace=error_trace, filter_path=filter_path, human=human, pretty=pretty
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve server information, server returned errors {err.errors}"
        ) from err


def node_info(
    hosts=None,
    profile=None,
    node_id=None,
    metric=None,
    error_trace=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Return Elasticsearch node information.

    node_id
        Comma-separated list of node IDs or namelastic used to limit returned
        information.
    metric
        Limits the information returned to the specific metrics. Supports
        a comma-separated list, such as http,ingest.
    flat_settings
        If true, returns settings in flat format.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.
    timeout
        Period to wait for a response. If no response is received before
        the timeout expires, the request fails and returns an error.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.node_info flat_settings=True
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.nodes.info(
            node_id=node_id,
            metric=metric,
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve node information, server returned errors {err.errors}"
        ) from err


def cluster_health(
    index=None,
    level="cluster",
    local=False,
    hosts=None,
    profile=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
    wait_for_active_shards=None,
    wait_for_events=None,
    wait_for_no_initializing_shards=None,
    wait_for_no_relocating_shards=None,
    wait_for_nodes=None,
    wait_for_status=None,
):
    """
    # pylint: disable=line-too-long

    .. versionadded:: 3005.1

    Return Elasticsearch cluster health.

    index
        Comma-separated list of data streams, indices, and index aliases
        used to limit the request. Wildcard expressions (*) are supported. To target
        all data streams and indices in a cluster, omit this parameter or use _all
        or '*'.
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valuelastic can be 'all', 'closed', 'hidden', 'none', 'open'
    level
        Can be one of cluster, indices or shards. Controls the details
        level of the health information returned.
    local
        If true, the request retrievelastic information from the local node
        only. Defaults to false, which means information is retrieved from the master
        node.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.
    timeout
        Period to wait for a response. If no response is received before
        the timeout expires, the request fails and returns an error.
    wait_for_active_shards
        A number controlling to how many active shards
        to wait for, all to wait for all shards in the cluster to be active, or 0
        to not wait.
    wait_for_events
        Can be one of immediate, urgent, high, normal, low, languid.
        Wait until all currently queued events with the given priority are processed.
    wait_for_no_initializing_shards
        A boolean value which controls whether
        to wait (until the timeout provided) for the cluster to have no shard initializations.
        Defaults to false, which means it will not wait for initializing shards.
    wait_for_no_relocating_shards
        A boolean value which controls whether
        to wait (until the timeout provided) for the cluster to have no shard relocations.
        Defaults to false, which means it will not wait for relocating shards.
    wait_for_nodes
        The request waits until the specified number N of nodes
        is available. It also accepts >=N, <=N, >N and <N. Alternatively, it is possible
        to use ge(N), le(N), gt(N) and lt(N) notation.
    wait_for_status
        One of green, yellow or red. Will wait (until the timeout
        provided) until the status of the cluster changelastic to the one provided or
        better, i.e. green > yellow > red. By default, will not wait for any status.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.cluster_health
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.cluster.health(
            index=index,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            level=level,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
            wait_for_events=wait_for_events,
            wait_for_no_initializing_shards=wait_for_no_initializing_shards,
            wait_for_no_relocating_shards=wait_for_no_relocating_shards,
            wait_for_nodes=wait_for_nodes,
            wait_for_status=wait_for_status,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve health information, server returned errors {err.errors}"
        ) from err


def cluster_allocation_explain(
    hosts=None,
    profile=None,
    current_node=None,
    error_trace=None,
    filter_path=None,
    human=None,
    include_disk_info=None,
    include_yes_decisions=None,
    index=None,
    pretty=None,
    primary=None,
    shard=None,
):
    """
    .. versionadded:: 3005.1

    Return Elasticsearch cluster allocation explain

    current_node
        Specifielastic the node ID or the name of the node to only explain
        a shard that is currently located on the specified node.
    include_disk_info
        If true, returns information about disk usage and shard
        sizelastic.
    include_yes_decisions
        If true, returns YES decisions in explanation.
    index
        Specifielastic the name of the index that you would like an explanation
        for.
    primary
        If true, returns explanation for the primary shard for the given
        shard ID.
    shard
        Specifielastic the ID of the shard that you would like an explanation
        for.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.cluster_allocation_explain
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.cluster.allocation_explain(
            current_node=current_node,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            include_disk_info=include_disk_info,
            include_yes_decisions=include_yes_decisions,
            index=index,
            pretty=pretty,
            primary=primary,
            shard=shard,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve cluster allocation explanation, server returned errors {err.errors}"
        ) from err


def cluster_pending_tasks(
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    local=None,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Returns a list of any cluster-level changelastic (e.g. create index, update mapping,
    allocate or fail shard) which have not yet been executed.


    local
        Return local information, do not retrieve the state from master
        node (default: false)
    master_timeout
        Specify timeout for connection to master

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.cluster_pending_tasks
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.cluster.pending_tasks(
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve cluster allocation explanation, server returned errors {err.errors}"
        ) from err


def cluster_stats(
    node_id=None,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    flat_settings=None,
    human=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Return Elasticsearch cluster stats.

    node_id
        Comma-separated list of node filters used to limit returned information.
        Defaults to all nodes in the cluster.
    flat_settings
        Return settings in flat format (default: false)
    timeout
        Period to wait for each node to respond. If a node does not respond
        before its timeout expires, the response does not include its stats. However,
        timed out nodes are included in the response’s _nodes.failed property. Defaults
        to no timeout.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.cluster_stats
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.cluster.stats(
            node_id=node_id,
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            pretty=pretty,
            timeout=timeout,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve cluster stats, server returned errors {err.errors}"
        ) from err


def cluster_get_settings(
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    include_defaults=False,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Return Elasticsearch cluster settings.

    flat_settings
        Return settings in flat format (default: false)
    include_defaults
        Whether to return all default clusters setting.
    master_timeout
        Explicit operation timeout for connection to master node
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.cluster_get_settings
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.cluster.get_settings(
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            include_defaults=include_defaults,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve cluster settings, server returned errors {err.errors}"
        ) from err


def cluster_put_settings(
    body=None,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    master_timeout=None,
    persistent=None,
    pretty=None,
    timeout=None,
    transient=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3000

    Set Elasticsearch cluster settings.

    body
        The settings to be updated. Can be either 'transient' or 'persistent' (survives cluster restart)
        https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html

    flat_settings
        Return settings in flat format.

    CLI Example:

    .. code-block:: bash

      salt myminion elasticsearch.cluster_put_settings '{"persistent": {"indices.recovery.max_bytes_per_sec": "50mb"}}'
      salt myminion elasticsearch.cluster_put_settings '{"transient": {"indices.recovery.max_bytes_per_sec": "50mb"}}'
    """
    if body is None and persistent is None and transient is None:
        message = (
            "You must provide a body with settings or provide the persistent or transient data"
        )
        raise SaltInvocationError(message)
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        if body is not None:
            return elastic.cluster.put_settings(
                body=body,
                flat_settings=flat_settings,
                error_trace=error_trace,
                filter_path=filter_path,
                human=human,
                master_timeout=master_timeout,
                pretty=pretty,
                timeout=timeout,
            ).body
        else:
            return elastic.cluster.put_settings(
                flat_settings=flat_settings,
                error_trace=error_trace,
                filter_path=filter_path,
                human=human,
                master_timeout=master_timeout,
                pretty=pretty,
                timeout=timeout,
                transient=transient,
                persistent=persistent,
            ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot update cluster settings, server returned errors {err.errors}"
        ) from err


def alias_create(
    indices,
    alias,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_=None,
    filter_path=None,
    human=None,
    index_routing=None,
    is_write_index=None,
    master_timeout=None,
    pretty=None,
    routing=None,
    search_routing=None,
    timeout=None,
):
    """
    Create an alias for a specific index/indices

    indices
        A comma-separated list of index namelastic the alias should point to
        (supports wildcards); use _all to perform the operation on all indices.
    alias
        The name of the alias to be created or updated

    filter
        Filter definittion
    index_routing
        index_routing
    is_write_index
        is_write_index
    master_timeout
        Specify timeout for connection to master
    routing
        Routing definition
    search_routing
        search_routing
    timeout
        Explicit timestamp for the document

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.alias_create testindex_v1 testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        result = elastic.indices.put_alias(
            index=indices,
            name=alias,
            error_trace=error_trace,
            filter=filter_,
            filter_path=filter_path,
            human=human,
            index_routing=index_routing,
            is_write_index=is_write_index,
            master_timeout=master_timeout,
            pretty=pretty,
            routing=routing,
            search_routing=search_routing,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create alias {alias} in index {indices}, server returned errors {err.errors}"
        ) from err


def alias_delete(
    indices,
    aliases,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    Delete an alias of an index

    indices
        Single or multiple indices separated by comma, use _all to perform the operation on all indices.
    aliases
        Alias namelastic separated by comma

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.alias_delete testindex_v1 testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        result = elastic.indices.delete_alias(
            index=indices,
            name=aliases,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.exceptions.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete alias {aliases} in index {indices}, server returned errors {err.errors}"
        ) from err


def alias_exists(
    aliases,
    indices=None,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    local=None,
    pretty=None,
):
    """
    Return a boolean indicating whether given alias exists

    indices
        A comma-separated list of index namelastic to filter aliases
    aliases
        A comma-separated list of alias namelastic to return
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valid valuelastic are 'all', 'closed', 'hidden', 'none', 'open'
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    local
        Return local information, do not retrieve the state from master
        node (default: false)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.alias_exists None testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        return elastic.indices.exists_alias(
            name=aliases,
            index=indices,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            local=local,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return False
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot get alias {aliases} in index {indices}, server returned errors {err.errors}"
        ) from err


def alias_get(
    aliases,
    indices=None,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    local=None,
    pretty=None,
):
    """
    Check for the existence of an alias and if it exists, return it

    indices
        A comma-separated list of index namelastic to filter aliases
    aliases
        A comma-separated list of alias namelastic to return
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valid valuelastic are 'all', 'closed', 'hidden', 'none', 'open'
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    local
        Return local information, do not retrieve the state from master
        node (default: false)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.alias_get testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.get_alias(
            index=indices,
            name=aliases,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            local=local,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot get alias {aliases} in index {indices}, server returned errors {err.errors}"
        ) from err


def document_create(
    index,
    body=None,
    hosts=None,
    profile=None,
    source=None,
    document=None,
    id_=None,
    error_trace=None,
    filter_path=None,
    human=None,
    if_primary_term=None,
    if_seq_no=None,
    op_type=None,
    pipeline=None,
    pretty=None,
    refresh=None,
    require_alias=None,
    routing=None,
    timeout=None,
    version=None,
    version_type=None,
    wait_for_active_shards=None,
):
    """
    Create a document in a specified index

    index
        Index name where the document should reside
    body
        Document to store
    source
        URL of file specifying document to store. Cannot be used in combination with body.
    document
        Document to store. If body doesn't exist, this is the dictionary with the document contents
    if_primary_term
        only perform the index operation if the last operation
        that has changed the document has the specified primary term
    if_seq_no
        only perform the index operation if the last operation that
        has changed the document has the specified sequence number
    op_type
        Explicit operation type. Defaults to index for requests with
        an explicit document ID, and to createfor requests without an explicit
        document ID
    pipeline
        The pipeline id to preprocess incoming documents with
    refresh
        If true then refresh the affected shards to make this operation
        visible to search, if wait_for then wait for a refresh to make this operation
        visible to search, if false (the default) then do nothing with refreshelastic.
    require_alias
        When true, requirelastic destination to be an alias. Default
        is false
    routing
        Specific routing value
    timeout
        Explicit operation timeout
    version
        Explicit version number for concurrency control
    version_type
        Specific version type
    wait_for_active_shards
        Sets the number of shard copielastic that must be active
        before proceeding with the index operation. Defaults to 1, meaning the primary
        shard only. Set to all for all shard copies, otherwise set to any non-negative
        value less than or equal to the total number of copielastic for the shard (number
        of replicas + 1)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_create testindex doctype1 '{}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))
    try:
        if body is not None:
            return elastic.index(
                index=index,
                document=body,
                id=id_,
                error_trace=error_trace,
                filter_path=filter_path,
                human=human,
                if_primary_term=if_primary_term,
                if_seq_no=if_seq_no,
                op_type=op_type,
                pipeline=pipeline,
                pretty=pretty,
                refresh=refresh,
                require_alias=require_alias,
                routing=routing,
                timeout=timeout,
                version=version,
                version_type=version_type,
                wait_for_active_shards=wait_for_active_shards,
            ).body
        else:
            return elastic.index(
                index=index,
                document=document,
                id=id_,
                error_trace=error_trace,
                filter_path=filter_path,
                human=human,
                if_primary_term=if_primary_term,
                if_seq_no=if_seq_no,
                op_type=op_type,
                pipeline=pipeline,
                pretty=pretty,
                refresh=refresh,
                require_alias=require_alias,
                routing=routing,
                timeout=timeout,
                version=version,
                version_type=version_type,
                wait_for_active_shards=wait_for_active_shards,
            ).body

    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create document in index {index}, server returned errors {err.errors}"
        ) from err


def document_delete(
    index,
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    if_primary_term=None,
    if_seq_no=None,
    pretty=None,
    refresh=None,
    routing=None,
    timeout=None,
    version=None,
    version_type=None,
    wait_for_active_shards=None,
):
    """
    Delete a document from an index

    index
        The name of the index
    if_primary_term
        only perform the delete operation if the last operation
        that has changed the document has the specified primary term
    if_seq_no
        only perform the delete operation if the last operation that
        has changed the document has the specified sequence number
    refresh
        If true then refresh the affected shards to make this operation
        visible to search, if wait_for then wait for a refresh to make this operation
        visible to search, if false (the default) then do nothing with refreshelastic.
    routing
        Specific routing value
    timeout
        Explicit operation timeout
    version
        Explicit version number for concurrency control
    version_type
        Specific version type
    wait_for_active_shards
        Sets the number of shard copielastic that must be active
        before proceeding with the delete operation. Defaults to 1, meaning the primary
        shard only. Set to all for all shard copies, otherwise set to any non-negative
        value less than or equal to the total number of copielastic for the shard (number
        of replicas + 1)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_delete testindex doctype1 AUx-384m0Bug_8U80wQZ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.delete(
            index=index,
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            if_primary_term=if_primary_term,
            if_seq_no=if_seq_no,
            pretty=pretty,
            refresh=refresh,
            routing=routing,
            timeout=timeout,
            version=version,
            version_type=version_type,
            wait_for_active_shards=wait_for_active_shards,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete document {id} in index {index}, server returned errors {err.errors}"
        ) from err


def document_exists(
    index,
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    preference=None,
    pretty=None,
    realtime=None,
    refresh=None,
    routing=None,
    source=None,
    source_excludes=None,
    source_includes=None,
    stored_fields=None,
    version=None,
    version_type=None,
):
    """
    Return a boolean indicating whether given document exists

    index
        The name of the index
    preference
        Specify the node or shard the operation should be performed
        on (default: random)
    realtime
        Specify whether to perform the operation in realtime or search
        mode
    refresh
        Refresh the shard containing the document before performing the
        operation
    routing
        Specific routing value
    source
        True or false to return the _source field or not, or a list of
        fields to return
    source_excludes
        A list of fields to exclude from the returned _source
        field
    source_includes
        A list of fields to extract and return from the _source
        field
    stored_fields
        A comma-separated list of stored fields to return in the
        response
    version
        Explicit version number for concurrency control
    version_type
        Specific version type.  version_type must be one of
        ('external', 'external_gte', 'force', 'internal')

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_exists testindex AUx-384m0Bug_8U80wQZ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.exists(
            index=index,
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            preference=preference,
            pretty=pretty,
            realtime=realtime,
            refresh=refresh,
            routing=routing,
            source=source,
            source_excludes=source_excludes,
            source_includes=source_includes,
            stored_fields=stored_fields,
            version=version,
            version_type=version_type,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return False
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve document {id} from index {index}, server returned errors {err.errors}"
        ) from err


def document_get(
    index,
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    preference=None,
    pretty=None,
    realtime=None,
    refresh=None,
    routing=None,
    source=None,
    source_excludes=None,
    source_includes=None,
    stored_fields=None,
    version=None,
    version_type=None,
):
    """
    Check for the existence of a document and if it exists, return it

    index
        Index name where the document resides
    doc_type
        Type of the document, use _all to fetch the first document matching the ID across all types

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_get testindex AUx-384m0Bug_8U80wQZ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.get(
            index=index,
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            preference=preference,
            pretty=pretty,
            realtime=realtime,
            refresh=refresh,
            routing=routing,
            source=source,
            source_excludes=source_excludes,
            source_includes=source_includes,
            stored_fields=stored_fields,
            version=version,
            version_type=version_type,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve document {id} from index {index}, server returned errors {err.errors}"
        ) from err


def document_get_all(
    index,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    preference=None,
    pretty=None,
    realtime=None,
    refresh=None,
    routing=None,
    source=None,
    source_excludes=None,
    source_includes=None,
    stored_fields=None,
    version=None,
    version_type=None,
):
    """
    Get all documents in an index

    index
        Index name where the document resides

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_get_all testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.search(
            index=index,
            source_excludes=source_excludes,
            body={"size": 10000, "sort": [{"@timestamp": {"order": "desc"}}], "query": {"match_all": {}}},
        ).body["hits"]
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve documents from index {index}, server returned errors {err.errors}"
        ) from err


def document_update(
    index,
    id_,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    doc=None,
    script=None,
    upsert=None,
    doc_as_upsert=None,
    scripted_upsert=None,
    detect_noop=None,
    error_trace=None,
    filter_path=None,
    human=None,
    if_primary_term=None,
    if_seq_no=None,
    lang=None,
    pretty=None,
    refresh=None,
    require_alias=None,
    retry_on_conflict=None,
    routing=None,
    source_excludes=None,
    source_includes=None,
    timeout=None,
    wait_for_active_shards=None,
):
    r"""
    .. versionadded:: 1.3.0

    Update a document in a specified index

    index
        Index name where the document resides
    id\_
        Unique identifier for the document
    body
        The request body containing update definition. Cannot be used with source.
    source
        URL of file specifying update definition. Cannot be used in combination with body.
    doc
        A partial update to an existing document. If both doc and script are specified, doc is ignored.
    script
        Script to run to update the document. If both doc and script are specified, doc is ignored.
    upsert
        If the document does not exist, the contents of upsert are inserted as a new document.
        If the document exists, the script is run.
    doc_as_upsert
        If true, use the contents of doc as the value of upsert.
        Note: Using ingest pipelines with doc_as_upsert is not supported.
    scripted_upsert
        If true, run the script whether or not the document exists.
    detect_noop
        If true, the result in the response is set to noop if there are no changes to the document.
    if_primary_term
        Only perform the update operation if the last operation that has changed the document
        has the specified primary term.
    if_seq_no
        Only perform the update operation if the last operation that has changed the document
        has the specified sequence number.
    lang
        The script language (default: painless)
    refresh
        If true then refresh the affected shards to make this operation visible to search,
        if wait_for then wait for a refresh to make this operation visible to search,
        if false (the default) then do nothing with refreshes.
    require_alias
        When true, requires destination to be an alias. Default is false.
    retry_on_conflict
        Specify how many times should the operation be retried when a conflict occurs (default: 0)
    routing
        Specific routing value
    source_excludes
        A comma-separated list of source fields to exclude in the response
    source_includes
        A comma-separated list of source fields to include in the response
    timeout
        Explicit operation timeout
    wait_for_active_shards
        Sets the number of shard copies that must be active before proceeding with the update operation.
        Defaults to 1, meaning the primary shard only. Set to all for all shard copies, otherwise set
        to any non-negative value less than or equal to the total number of copies for the shard
        (number of replicas + 1)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.document_update testindex doc123 doc='{"field": "value"}'
        salt myminion elasticsearch.document_update testindex doc123 script='{"source": "ctx._source.counter += params.count", "params": {"count": 4}}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build update body from parameters
        update_body = {}
        if body:
            update_body = body
        else:
            if doc is not None:
                update_body["doc"] = doc
            if script is not None:
                update_body["script"] = script
            if upsert is not None:
                update_body["upsert"] = upsert
            if doc_as_upsert is not None:
                update_body["doc_as_upsert"] = doc_as_upsert
            if scripted_upsert is not None:
                update_body["scripted_upsert"] = scripted_upsert
        return elastic.update(
            index=index,
            id=id_,
            detect_noop=detect_noop,
            document=update_body if update_body else None,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            if_primary_term=if_primary_term,
            if_seq_no=if_seq_no,
            lang=lang,
            pretty=pretty,
            refresh=refresh,
            require_alias=require_alias,
            retry_on_conflict=retry_on_conflict,
            routing=routing,
            source_excludes=source_excludes,
            source_includes=source_includes,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot update document {id_} in index {index}, server returned errors {err.errors}"
        ) from err


# pylint: disable=too-many-arguments
def search(
    index=None,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    query=None,
    q=None,
    aggregations=None,
    aggs=None,
    size=None,
    from_=None,
    sort=None,
    search_after=None,
    scroll=None,
    pit=None,
    timeout=None,
    terminate_after=None,
    allow_no_indices=None,
    allow_partial_search_results=None,
    analyze_wildcard=None,
    analyzer=None,
    batched_reduce_size=None,
    ccs_minimize_roundtrips=None,
    collapse=None,
    default_operator=None,
    df=None,
    docvalue_fields=None,
    error_trace=None,
    expand_wildcards=None,
    explain=None,
    fields=None,
    filter_path=None,
    highlight=None,
    human=None,
    ignore_throttled=None,
    ignore_unavailable=None,
    indices_boost=None,
    knn=None,
    lenient=None,
    max_concurrent_shard_requests=None,
    min_score=None,
    post_filter=None,
    pre_filter_shard_size=None,
    preference=None,
    pretty=None,
    profile_=None,
    request_cache=None,
    rescore=None,
    rest_total_hits_as_int=None,
    retriever=None,
    routing=None,
    runtime_mappings=None,
    script_fields=None,
    search_type=None,
    seq_no_primary_term=None,
    source_excludes=None,
    source_includes=None,
    stats=None,
    stored_fields=None,
    suggest=None,
    suggest_field=None,
    suggest_mode=None,
    suggest_size=None,
    suggest_text=None,
    track_scores=None,
    track_total_hits=None,
    typed_keys=None,
    version=None,
):
    r"""
    .. versionadded:: 1.3.0

    Execute a search query and return matching documents

    index
        A comma-separated list of index names to search; use _all or empty string to perform
        the operation on all indices
    body
        The search definition using the Query DSL. Cannot be used with source.
    source
        URL of file specifying search definition. Cannot be used in combination with body.
    query
        Defines the search definition using the Query DSL
    q
        Query in the Lucene query string syntax. Overrides query parameter in body if both specified.
    aggregations / aggs
        Defines the aggregations that are run as part of the search request
    size
        Number of hits to return (default: 10). Cannot page through more than 10,000 hits
        using from and size; use search_after instead.
    from\_
        Starting document offset (default: 0). Must be non-negative.
    sort
        A comma-separated list of <field>:<direction> pairs
    search_after
        Used to retrieve the next page of hits using sort values from the previous page
    scroll
        Period to retain the search context for scrolling (e.g., '1m' for 1 minute)
    pit
        Limit search to a point in time (PIT). If provided, cannot specify index in request path.
    timeout
        The period to wait for search execution
    terminate_after
        The maximum number of documents to collect for each shard
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves into no concrete indices
    allow_partial_search_results
        Indicate if an error should be returned if there is a partial search failure or timeout
    analyze_wildcard
        Specify whether wildcard and prefix queries should be analyzed (default: false)
    analyzer
        The analyzer to use for the query string
    batched_reduce_size
        The number of shard results that should be reduced at once on the coordinating node
    collapse
        Collapse search results based on field values
    default_operator
        The default operator for query string query (AND or OR)
    df
        The field to use as default where no field prefix is given in the query string
    explain
        If true, returns detailed information about score computation
    fields
        A comma-separated list of fields to return as part of a hit
    filter_path
        Used to reduce the response returned by elasticsearch
    highlight
        Specifies the highlighter to use for retrieving highlighted snippets
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable
    knn
        Approximate kNN search to run
    max_concurrent_shard_requests
        The number of concurrent shard requests per node this search executes concurrently
    min_score
        Minimum _score for matching documents
    post_filter
        Post filter applied after aggregations
    preference
        Specify the node or shard the operation should be performed on (default: random)
    profile\_
        Set to true to return detailed timing information (NOTE: adds significant overhead)
    request_cache
        Specify if request cache should be used for this request
    routing
        A comma-separated list of specific routing values
    runtime_mappings
        Defines runtime fields which exist only as part of the query
    script_fields
        Retrieve a script evaluation for each hit
    search_type
        Search operation type (query_then_fetch or dfs_query_then_fetch)
    source_excludes
        A list of source fields to exclude
    source_includes
        A list of source fields to include
    stats
        Tag of the request for logging and statistical purposes
    stored_fields
        A comma-separated list of stored fields to return
    suggest
        Specify suggest fields
    track_scores
        Whether to calculate and return scores even if they are not used for sorting
    track_total_hits
        Indicates whether hits.total should be rendered as an integer or an object
    version
        Whether to return document version as part of a hit

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.search
        salt myminion elasticsearch.search testindex q='field:value'
        salt myminion elasticsearch.search testindex query='{"match": {"field": "value"}}'
        salt myminion elasticsearch.search testindex body='{"query": {"match_all": {}}, "size": 20}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build search body from parameters if body not provided
        search_body = {}
        if body:
            if isinstance(body, str):
                body = salt.utils.json.loads(body)
            search_body = body
        else:
            if query is not None:
                search_body["query"] = query
            if aggregations is not None:
                search_body["aggregations"] = aggregations
            elif aggs is not None:
                search_body["aggs"] = aggs
            if sort is not None:
                search_body["sort"] = sort
            if fields is not None:
                search_body["fields"] = fields
            if script_fields is not None:
                search_body["script_fields"] = script_fields
            if highlight is not None:
                search_body["highlight"] = highlight
            if collapse is not None:
                search_body["collapse"] = collapse
            if post_filter is not None:
                search_body["post_filter"] = post_filter
            if rescore is not None:
                search_body["rescore"] = rescore
            if runtime_mappings is not None:
                search_body["runtime_mappings"] = runtime_mappings
            if suggest is not None:
                search_body["suggest"] = suggest
            if pit is not None:
                search_body["pit"] = pit
            if knn is not None:
                search_body["knn"] = knn
            if retriever is not None:
                search_body["retriever"] = retriever

        return elastic.search(
            index=index,
            aggregations=search_body.get("aggregations"),
            aggs=search_body.get("aggs"),
            allow_no_indices=allow_no_indices,
            allow_partial_search_results=allow_partial_search_results,
            analyze_wildcard=analyze_wildcard,
            analyzer=analyzer,
            batched_reduce_size=batched_reduce_size,
            ccs_minimize_roundtrips=ccs_minimize_roundtrips,
            collapse=search_body.get("collapse"),
            default_operator=default_operator,
            df=df,
            docvalue_fields=docvalue_fields,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            explain=explain,
            fields=search_body.get("fields"),
            filter_path=filter_path,
            from_=from_,
            highlight=search_body.get("highlight"),
            human=human,
            ignore_throttled=ignore_throttled,
            ignore_unavailable=ignore_unavailable,
            indices_boost=indices_boost,
            knn=search_body.get("knn"),
            lenient=lenient,
            max_concurrent_shard_requests=max_concurrent_shard_requests,
            min_score=min_score,
            pit=search_body.get("pit"),
            post_filter=search_body.get("post_filter"),
            pre_filter_shard_size=pre_filter_shard_size,
            preference=preference,
            pretty=pretty,
            profile=profile_,
            q=q,
            query=search_body.get("query"),
            request_cache=request_cache,
            rescore=search_body.get("rescore"),
            rest_total_hits_as_int=rest_total_hits_as_int,
            retriever=search_body.get("retriever"),
            routing=routing,
            runtime_mappings=search_body.get("runtime_mappings"),
            script_fields=search_body.get("script_fields"),
            scroll=scroll,
            search_after=search_after,
            search_type=search_type,
            seq_no_primary_term=seq_no_primary_term,
            size=size,
            sort=search_body.get("sort"),
            source_excludes=source_excludes,
            source_includes=source_includes,
            stats=stats,
            stored_fields=stored_fields,
            suggest=search_body.get("suggest"),
            suggest_field=suggest_field,
            suggest_mode=suggest_mode,
            suggest_size=suggest_size,
            suggest_text=suggest_text,
            terminate_after=terminate_after,
            timeout=timeout,
            track_scores=track_scores,
            track_total_hits=track_total_hits,
            typed_keys=typed_keys,
            version=version,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute search on index {index}, server returned errors {err.errors}"
        ) from err


def bulk(
    operations=None,
    index=None,
    hosts=None,
    profile=None,
    source=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pipeline=None,
    pretty=None,
    refresh=None,
    require_alias=None,
    require_data_stream=None,
    routing=None,
    source_excludes=None,
    source_includes=None,
    timeout=None,
    wait_for_active_shards=None,
    list_executed_pipelines=None,
    include_source_on_error=None,
):
    """
    .. versionadded:: 1.3.0

    Perform multiple index, create, delete, and update operations in a single request

    operations
        The list of operations to perform. Each operation is a dict with action and optional metadata.

        Format::

            [
                {"index": {"_index": "test", "_id": "1"}},
                {"field": "value"},
                {"delete": {"_index": "test", "_id": "2"}},
                {"create": {"_index": "test", "_id": "3"}},
                {"field": "value"}
            ]
    index
        Default index for items which don't provide one
    source
        URL of file specifying bulk operations. Cannot be used in combination with operations.
    pipeline
        The pipeline id to preprocess incoming documents with. To turn off the default
        pipeline, set to _none.
    refresh
        If true then refresh the affected shards to make this operation visible to search,
        if wait_for then wait for a refresh to make this operation visible to search,
        if false (the default) then do nothing with refreshes.
    require_alias
        If true, the request's actions must target an index alias
    require_data_stream
        If true, the request's actions must target a data stream (existing or to be created)
    routing
        A custom value used to route operations to a specific shard
    source_excludes
        A comma-separated list of source fields to exclude from the response
    source_includes
        A comma-separated list of source fields to include in the response
    timeout
        The period each action waits for the following operations: automatic index creation,
        dynamic mapping updates, waiting for active shards. Default is 1m (one minute).
    wait_for_active_shards
        Sets the number of shard copies that must be active before proceeding with the operation.
        Defaults to 1, meaning the primary shard only. Set to all for all shard copies.
    list_executed_pipelines
        If true, the response will include the ingest pipelines that were run for each index or create
    include_source_on_error
        If true, include the document source in the error message in case of parsing errors

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.bulk '[{"index": {"_index": "test", "_id": "1"}}, {"field": "value1"}, {"index": {"_index": "test", "_id": "2"}}, {"field": "value2"}]'
        salt myminion elasticsearch.bulk operations='[...]' index=test pipeline=my-pipeline
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and operations:
        message = "Either operations or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        operations = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    if not operations:
        raise SaltInvocationError("operations parameter is required")

    try:
        return elastic.bulk(
            operations=operations,
            index=index,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            include_source_on_error=include_source_on_error,
            list_executed_pipelines=list_executed_pipelines,
            pipeline=pipeline,
            pretty=pretty,
            refresh=refresh,
            require_alias=require_alias,
            require_data_stream=require_data_stream,
            routing=routing,
            source_excludes=source_excludes,
            source_includes=source_includes,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute bulk operation, server returned errors {err.errors}"
        ) from err


def count(
    index=None,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    query=None,
    q=None,
    allow_no_indices=None,
    analyze_wildcard=None,
    analyzer=None,
    default_operator=None,
    df=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_throttled=None,
    ignore_unavailable=None,
    lenient=None,
    min_score=None,
    preference=None,
    pretty=None,
    routing=None,
    terminate_after=None,
):
    """
    .. versionadded:: 1.3.0

    Get the count of documents matching a query

    index
        A comma-separated list of data streams, indices, and aliases to search.
        Supports wildcards (*). Use _all or empty string to perform the operation on all indices.
    body
        Query definition using the Query DSL. Cannot be used with source.
    source
        URL of file specifying query definition. Cannot be used in combination with body.
    query
        Defines the search definition using the Query DSL
    q
        Query in the Lucene query string syntax
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves into no concrete indices
    analyze_wildcard
        Specify whether wildcard and prefix queries should be analyzed (default: false)
    analyzer
        The analyzer to use for the query string
    default_operator
        The default operator for query string query (AND or OR)
    df
        The field to use as default where no field prefix is given in the query string
    expand_wildcards
        Whether to expand wildcard expression to concrete indices that are open, closed or both
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable (missing or closed)
    lenient
        Specify whether format-based query failures should be ignored
    min_score
        Minimum _score for matching documents. Documents with a lower score are not included in results.
    preference
        Specify the node or shard the operation should be performed on (default: random)
    routing
        A comma-separated list of specific routing values
    terminate_after
        The maximum number of documents to collect for each shard

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.count
        salt myminion elasticsearch.count testindex
        salt myminion elasticsearch.count testindex q='status:active'
        salt myminion elasticsearch.count testindex query='{"match": {"field": "value"}}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build query body from parameters if body not provided
        query_body = {}
        if body:
            query_body = body
        elif query is not None:
            query_body["query"] = query

        return elastic.count(
            index=index,
            allow_no_indices=allow_no_indices,
            analyze_wildcard=analyze_wildcard,
            analyzer=analyzer,
            default_operator=default_operator,
            df=df,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_throttled=ignore_throttled,
            ignore_unavailable=ignore_unavailable,
            lenient=lenient,
            min_score=min_score,
            preference=preference,
            pretty=pretty,
            q=q,
            query=query_body.get("query") if query_body else None,
            routing=routing,
            terminate_after=terminate_after,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute count on index {index}, server returned errors {err.errors}"
        ) from err


def mget(
    index=None,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    docs=None,
    ids=None,
    error_trace=None,
    filter_path=None,
    human=None,
    preference=None,
    pretty=None,
    realtime=None,
    refresh=None,
    routing=None,
    source_excludes=None,
    source_includes=None,
    stored_fields=None,
):
    """
    .. versionadded:: 1.3.0

    Get multiple documents by ID

    index
        Name of the index to retrieve documents from
    body
        Document identifiers and optional parameters. Cannot be used with source.
    source
        URL of file specifying document identifiers. Cannot be used in combination with body.
    docs
        The documents to retrieve (required if index not in URI)
    ids
        The document IDs to retrieve (allowed when index specified in URI)
    preference
        Specify the node or shard the operation should be performed on (default: random)
    realtime
        Specify whether to perform the operation in realtime or search mode
    refresh
        If true, refresh the relevant shards before retrieval to make changes visible
    routing
        Specific routing value
    source_excludes
        A comma-separated list of source fields to exclude from the response
    source_includes
        A comma-separated list of source fields to include in the response
    stored_fields
        A comma-separated list of stored fields to return in the response

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.mget testindex ids='["1", "2", "3"]'
        salt myminion elasticsearch.mget docs='[{"_index": "test", "_id": "1"}, {"_index": "test2", "_id": "2"}]'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build mget body from parameters if body not provided
        mget_body = {}
        if body:
            mget_body = body
        else:
            if docs is not None:
                mget_body["docs"] = docs
            if ids is not None:
                mget_body["ids"] = ids

        return elastic.mget(
            index=index,
            docs=mget_body.get("docs") if not ids else None,
            ids=ids,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            preference=preference,
            pretty=pretty,
            realtime=realtime,
            refresh=refresh,
            routing=routing,
            source_excludes=source_excludes,
            source_includes=source_includes,
            stored_fields=stored_fields,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute mget, server returned errors {err.errors}"
        ) from err


# pylint: disable=too-many-arguments
def delete_by_query(
    index,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    query=None,
    q=None,
    allow_no_indices=None,
    analyze_wildcard=None,
    analyzer=None,
    conflicts=None,
    default_operator=None,
    df=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    from_=None,
    human=None,
    ignore_unavailable=None,
    lenient=None,
    max_docs=None,
    preference=None,
    pretty=None,
    refresh=None,
    request_cache=None,
    requests_per_second=None,
    routing=None,
    scroll=None,
    scroll_size=None,
    search_timeout=None,
    search_type=None,
    slices=None,
    sort=None,
    stats=None,
    terminate_after=None,
    timeout=None,
    version=None,
    wait_for_active_shards=None,
    wait_for_completion=None,
):
    """
    .. versionadded:: 1.3.0

    Delete documents matching a query

    index
        A comma-separated list of data streams, indices, and aliases to search.
        Supports wildcards (*).
    body
        Query definition using the Query DSL. Cannot be used with source.
    source
        URL of file specifying query definition. Cannot be used in combination with body.
    query
        Query in the request body to identify documents to delete
    q
        Query in the Lucene query string syntax
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves into no concrete indices
    analyzer
        The analyzer to use for the query string
    conflicts
        What to do when the operation encounters version conflicts (abort or proceed)
    default_operator
        The default operator for query string query (AND or OR)
    df
        The field to use as default where no field prefix is given in the query string
    expand_wildcards
        Whether to expand wildcard expression to concrete indices that are open, closed or both
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable
    max_docs
        Maximum number of documents to process (default: all documents)
    preference
        Specify the node or shard the operation should be performed on
    refresh
        If true, refresh the affected shards after performing the operation
    requests_per_second
        The throttle for this request in sub-requests per second (-1 means no throttle)
    routing
        A comma-separated list of specific routing values
    scroll
        Specify how long a consistent view of the index should be maintained for scrolled search
    scroll_size
        Size of the scroll request (default: 100)
    search_timeout
        Explicit timeout for each search request
    slices
        The number of slices this task should be divided into (default: 1, auto calculates)
    timeout
        Time each individual bulk request should wait for shards that are unavailable
    wait_for_active_shards
        Sets the number of shard copies that must be active before proceeding
    wait_for_completion
        If false, return task ID for async execution (default: true)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.delete_by_query testindex query='{"match": {"status": "old"}}'
        salt myminion elasticsearch.delete_by_query testindex q='status:old'
        salt myminion elasticsearch.delete_by_query testindex query='{"range": {"date": {"lt": "now-30d"}}}' slices=auto
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build query body from parameters if body not provided
        query_body = {}
        if body:
            query_body = body
        elif query is not None:
            query_body["query"] = query

        return elastic.delete_by_query(
            index=index,
            allow_no_indices=allow_no_indices,
            analyze_wildcard=analyze_wildcard,
            analyzer=analyzer,
            conflicts=conflicts,
            default_operator=default_operator,
            df=df,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            from_=from_,
            human=human,
            ignore_unavailable=ignore_unavailable,
            lenient=lenient,
            max_docs=max_docs,
            preference=preference,
            pretty=pretty,
            q=q,
            query=query_body.get("query") if query_body else None,
            refresh=refresh,
            request_cache=request_cache,
            requests_per_second=requests_per_second,
            routing=routing,
            scroll=scroll,
            scroll_size=scroll_size,
            search_timeout=search_timeout,
            search_type=search_type,
            slices=slices,
            sort=sort,
            stats=stats,
            terminate_after=terminate_after,
            timeout=timeout,
            version=version,
            wait_for_active_shards=wait_for_active_shards,
            wait_for_completion=wait_for_completion,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute delete_by_query on index {index}, server returned errors {err.errors}"
        ) from err


# pylint: disable=too-many-arguments
def update_by_query(
    index,
    hosts=None,
    profile=None,
    body=None,
    source=None,
    query=None,
    script=None,
    q=None,
    allow_no_indices=None,
    analyze_wildcard=None,
    analyzer=None,
    conflicts=None,
    default_operator=None,
    df=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    from_=None,
    human=None,
    ignore_unavailable=None,
    lenient=None,
    max_docs=None,
    pipeline=None,
    preference=None,
    pretty=None,
    refresh=None,
    request_cache=None,
    requests_per_second=None,
    routing=None,
    scroll=None,
    scroll_size=None,
    search_timeout=None,
    search_type=None,
    slices=None,
    sort=None,
    stats=None,
    terminate_after=None,
    timeout=None,
    version=None,
    wait_for_active_shards=None,
    wait_for_completion=None,
):
    """
    .. versionadded:: 1.3.0

    Update documents matching a query

    index
        A comma-separated list of data streams, indices, and aliases to search.
        Supports wildcards (*).
    body
        Request body containing query and script. Cannot be used with source.
    source
        URL of file specifying request body. Cannot be used in combination with body.
    query
        Query in the request body to identify documents to update
    script
        Script to run to update the document source or metadata
    q
        Query in the Lucene query string syntax
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves into no concrete indices
    analyzer
        The analyzer to use for the query string
    conflicts
        What to do when the operation encounters version conflicts (abort or proceed)
    default_operator
        The default operator for query string query (AND or OR)
    df
        The field to use as default where no field prefix is given in the query string
    expand_wildcards
        Whether to expand wildcard expression to concrete indices that are open, closed or both
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable
    max_docs
        Maximum number of documents to process (default: all documents)
    pipeline
        The pipeline ID to preprocess incoming documents with
    preference
        Specify the node or shard the operation should be performed on
    refresh
        If true, refresh the affected shards after performing the operation
    requests_per_second
        The throttle for this request in sub-requests per second (-1 means no throttle)
    routing
        A comma-separated list of specific routing values
    scroll
        Specify how long a consistent view of the index should be maintained for scrolled search
    scroll_size
        Size of the scroll request (default: 100)
    search_timeout
        Explicit timeout for each search request
    slices
        The number of slices this task should be divided into (default: 1, auto calculates)
    timeout
        Time each individual bulk request should wait for shards that are unavailable
    wait_for_active_shards
        Sets the number of shard copies that must be active before proceeding
    wait_for_completion
        If false, return task ID for async execution (default: true)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.update_by_query testindex script='{"source": "ctx._source.status = params.status", "params": {"status": "updated"}}'
        salt myminion elasticsearch.update_by_query testindex query='{"match": {"status": "old"}}' script='{"source": "ctx._source.status = \"new\""}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build request body from parameters if body not provided
        request_body = {}
        if body:
            request_body = body
        else:
            if query is not None:
                request_body["query"] = query
            if script is not None:
                request_body["script"] = script

        return elastic.update_by_query(
            index=index,
            allow_no_indices=allow_no_indices,
            analyze_wildcard=analyze_wildcard,
            analyzer=analyzer,
            conflicts=conflicts,
            default_operator=default_operator,
            df=df,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            from_=from_,
            human=human,
            ignore_unavailable=ignore_unavailable,
            lenient=lenient,
            max_docs=max_docs,
            pipeline=pipeline,
            preference=preference,
            pretty=pretty,
            q=q,
            query=request_body.get("query") if request_body else None,
            refresh=refresh,
            request_cache=request_cache,
            requests_per_second=requests_per_second,
            routing=routing,
            script=request_body.get("script") if request_body else None,
            scroll=scroll,
            scroll_size=scroll_size,
            search_timeout=search_timeout,
            search_type=search_type,
            slices=slices,
            sort=sort,
            stats=stats,
            terminate_after=terminate_after,
            timeout=timeout,
            version=version,
            wait_for_active_shards=wait_for_active_shards,
            wait_for_completion=wait_for_completion,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute update_by_query on index {index}, server returned errors {err.errors}"
        ) from err


# pylint: disable=too-many-arguments
def reindex(
    hosts=None,
    profile=None,
    body=None,
    source=None,
    dest=None,
    source_index=None,
    dest_index=None,
    conflicts=None,
    error_trace=None,
    filter_path=None,
    human=None,
    max_docs=None,
    pretty=None,
    refresh=None,
    requests_per_second=None,
    require_alias=None,
    script=None,
    scroll=None,
    slices=None,
    timeout=None,
    wait_for_active_shards=None,
    wait_for_completion=None,
):
    """
    .. versionadded:: 1.3.0

    Copy documents from a source to a destination

    body
        Request body containing source and destination. Cannot be used with source file.
    source
        URL of file specifying reindex request. Cannot be used in combination with body.
    dest
        The destination you are copying to (dict with index, op_type, etc.)
    source_index
        The source index you are copying from (dict with index, query, etc.)
    dest_index
        Destination index name (simplified alternative to dest parameter)
    source_index
        Source index name (simplified alternative to source parameter)
    conflicts
        What to do when reindex encounters version conflicts (abort or proceed)
    max_docs
        Maximum number of documents to reindex
    refresh
        If true, refresh the affected shards after performing the operation
    requests_per_second
        The throttle for this request in sub-requests per second (-1 means no throttle)
    require_alias
        If true, the destination must be an index alias
    script
        Script to run to update the document source or metadata
    scroll
        Specify how long a consistent view of the index should be maintained for scrolled search
    slices
        The number of slices this task should be divided into (default: 1, auto calculates)
    timeout
        Time each individual bulk request should wait for shards that are unavailable
    wait_for_active_shards
        Sets the number of shard copies that must be active before proceeding
    wait_for_completion
        If false, return task ID for async execution (default: true)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.reindex source_index=old_index dest_index=new_index
        salt myminion elasticsearch.reindex body='{"source": {"index": "old"}, "dest": {"index": "new"}}'
        salt myminion elasticsearch.reindex source_index=old dest_index=new script='{"source": "ctx._source.new_field = ctx._source.old_field"}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and body:
        message = "Either body or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        body = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    try:
        # Build request body from parameters if body not provided
        request_body = {}
        if body:
            request_body = body
        else:
            # Build source and dest from simplified parameters
            if source_index is not None:
                if isinstance(source_index, str):
                    request_body["source"] = {"index": source_index}
                else:
                    request_body["source"] = source_index
            elif dest is not None:
                request_body["source"] = dest

            if dest_index is not None:
                if isinstance(dest_index, str):
                    request_body["dest"] = {"index": dest_index}
                else:
                    request_body["dest"] = dest_index
            elif dest is not None:
                request_body["dest"] = dest

            if script is not None:
                request_body["script"] = script

        if not request_body.get("source") or not request_body.get("dest"):
            raise SaltInvocationError(
                "Both source and dest must be specified for reindex operation"
            )

        return elastic.reindex(
            dest=request_body.get("dest"),
            source=request_body.get("source"),
            conflicts=conflicts,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            max_docs=max_docs,
            pretty=pretty,
            refresh=refresh,
            requests_per_second=requests_per_second,
            require_alias=require_alias,
            script=request_body.get("script"),
            scroll=scroll,
            slices=slices,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
            wait_for_completion=wait_for_completion,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute reindex, server returned errors {err.errors}"
        ) from err


def msearch(
    searches,
    index=None,
    hosts=None,
    profile=None,
    source=None,
    allow_no_indices=None,
    ccs_minimize_roundtrips=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_throttled=None,
    ignore_unavailable=None,
    max_concurrent_searches=None,
    max_concurrent_shard_requests=None,
    pre_filter_shard_size=None,
    pretty=None,
    rest_total_hits_as_int=None,
    routing=None,
    search_type=None,
    typed_keys=None,
):
    """
    .. versionadded:: 1.3.0

    Run multiple search requests in a single API call

    searches
        List of search request definitions (sequence of dicts with header and body)
    index
        A comma-separated list of index names to use as default
    source
        URL of file specifying search requests. Cannot be used in combination with searches.
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves into no concrete indices
    ccs_minimize_roundtrips
        If true, network round-trips between the coordinating node and remote clusters are minimized
    expand_wildcards
        Whether to expand wildcard expression to concrete indices that are open, closed or both
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable
    max_concurrent_searches
        Maximum number of concurrent searches the multi search API can execute
    max_concurrent_shard_requests
        The number of concurrent shard requests per node this search executes concurrently
    pre_filter_shard_size
        Threshold that enforces a pre-filter round-trip to prefilter search shards
    rest_total_hits_as_int
        If true, hits.total are rendered as an integer in the response
    routing
        A comma-separated list of specific routing values
    search_type
        Search operation type (query_then_fetch or dfs_query_then_fetch)
    typed_keys
        Specify whether aggregation and suggester names should be prefixed by type

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.msearch '[{}, {"query": {"match_all": {}}}, {"index": "test2"}, {"query": {"match": {"field": "value"}}}]'
        salt myminion elasticsearch.msearch searches='[...]' index=default_index
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and searches:
        message = "Either searches or source should be specified but not both."
        raise SaltInvocationError(message)
    if source:
        searches = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))

    if not searches:
        raise SaltInvocationError("searches parameter is required")

    try:
        return elastic.msearch(
            searches=searches,
            index=index,
            allow_no_indices=allow_no_indices,
            ccs_minimize_roundtrips=ccs_minimize_roundtrips,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_throttled=ignore_throttled,
            ignore_unavailable=ignore_unavailable,
            max_concurrent_searches=max_concurrent_searches,
            max_concurrent_shard_requests=max_concurrent_shard_requests,
            pre_filter_shard_size=pre_filter_shard_size,
            pretty=pretty,
            rest_total_hits_as_int=rest_total_hits_as_int,
            routing=routing,
            search_type=search_type,
            typed_keys=typed_keys,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute msearch, server returned errors {err.errors}"
        ) from err


def scroll(
    scroll_id,
    hosts=None,
    profile=None,
    scroll=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
    rest_total_hits_as_int=None,
):
    """
    .. versionadded:: 1.3.0

    Retrieve the next batch of results for a scrolling search

    scroll_id
        The scroll ID for the search
    scroll
        Period to retain the search context for scrolling (e.g., '1m')
    rest_total_hits_as_int
        If true, hits.total are rendered as an integer in the response

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.scroll scroll_id='DXF1ZXJ5Q...' scroll='1m'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if not scroll_id:
        raise SaltInvocationError("scroll_id parameter is required")

    try:
        return elastic.scroll(
            scroll_id=scroll_id,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pretty=pretty,
            rest_total_hits_as_int=rest_total_hits_as_int,
            scroll=scroll,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot execute scroll, server returned errors {err.errors}"
        ) from err


def clear_scroll(
    scroll_id=None,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
):
    """
    .. versionadded:: 1.3.0

    Clear the search context for a scrolling search

    scroll_id
        The scroll ID or list of scroll IDs to clear. Use '_all' to clear all scroll IDs.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.clear_scroll scroll_id='DXF1ZXJ5Q...'
        salt myminion elasticsearch.clear_scroll scroll_id='_all'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.clear_scroll(
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pretty=pretty,
            scroll_id=scroll_id,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot clear scroll, server returned errors {err.errors}"
        ) from err


def open_point_in_time(
    index,
    keep_alive,
    hosts=None,
    profile=None,
    allow_partial_search_results=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    preference=None,
    pretty=None,
    routing=None,
):
    """
    .. versionadded:: 1.3.0

    Open a point in time for searching

    index
        A comma-separated list of index names to open point in time.
        Use _all or * or empty string to perform the operation on all indices.
    keep_alive
        Specific the time to live for the point in time (e.g., '1m', '1h')
    allow_partial_search_results
        If false, throws exception if request targets unavailable shards
    expand_wildcards
        Whether to expand wildcard expression to concrete indices that are open, closed or both
    ignore_unavailable
        Whether specified concrete indices should be ignored when unavailable
    preference
        Specify the node or shard the operation should be performed on
    routing
        A comma-separated list of specific routing values

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.open_point_in_time myindex keep_alive='1m'
        salt myminion elasticsearch.open_point_in_time myindex keep_alive='5m' ignore_unavailable=True
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if not keep_alive:
        raise SaltInvocationError("keep_alive parameter is required")

    try:
        return elastic.open_point_in_time(
            index=index,
            keep_alive=keep_alive,
            allow_partial_search_results=allow_partial_search_results,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            preference=preference,
            pretty=pretty,
            routing=routing,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot open point in time for index {index}, server returned errors {err.errors}"
        ) from err


def close_point_in_time(
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
):
    r"""
    .. versionadded:: 1.3.0

    Close a point in time

    id\_
        The ID of the point-in-time to close

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.close_point_in_time id_='46ToAwMDaWR5BXV1aWQy...'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if not id_:
        raise SaltInvocationError("id_ parameter is required")

    try:
        return elastic.close_point_in_time(
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot close point in time, server returned errors {err.errors}"
        ) from err


def index_create(
    index,
    hosts=None,
    profile=None,
    source=None,
    aliases=None,
    error_trace=None,
    filter_path=None,
    human=None,
    mappings=None,
    master_timeout=None,
    pretty=None,
    settings=None,
    timeout=None,
    wait_for_active_shards=None,
):
    """
    # pylint: disable=line-too-long
    Create an index

    index
        Index name
    source
        URL to file specifying index definition. Cannot be used in combination with body.
    index
        The name of the index
    aliases
        The list of aliases
    mappings
        Mapping for fields in the index. If specified, this mapping
        can include: - Field namelastic - Field data types - Mapping parameters
    master_timeout
        Specify timeout for connection to master
    settings
        Settings
    timeout
        Explicit operation timeout
    wait_for_active_shards
        Set the number of active shards to wait for before
        the operation returns. It also accepts ('all', 'index-setting')

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.index_create testindex
     salt myminion elasticsearch.index_create testindex2 \
        '{"settings" : {"index" : {"number_of_shards" : 3, "number_of_replicas" : 2}}}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    if source and (settings or mappings):
        message = "Either (settings or mappings) or source should be specified but not both."
        raise SaltInvocationError(message)

    src_map = None
    if source:
        src_map = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))
    try:
        if src_map is not None:
            settings = src_map.get("settings")
            mappings = src_map.get("mappings")

        result = elastic.indices.create(
            index=index,
            aliases=aliases,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            mappings=mappings,
            master_timeout=master_timeout,
            pretty=pretty,
            settings=settings,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
        ).body
        return result.get("acknowledged", False) and result.get("shards_acknowledged", True)
    except elasticsearch.TransportError as err:
        if "index_already_exists_exception" in err.errors:
            return True
        raise CommandExecutionError(
            f"Cannot create index {index}, server returned errors {err.errors}"
        ) from err


def index_delete(
    index,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    Delete an index

    index
        A comma-separated list of indices to delete; use _all or *
        string to delete all indices
    allow_no_indices
        Ignore if a wildcard expression resolvelastic to no concrete
        indices (default: false)
    expand_wildcards
        Whether wildcard expressions should get expanded to
        open, closed, or hidden indices.
        Accetps: ('all', 'closed', 'hidden', 'none', 'open')
    ignore_unavailable
        Ignore unavailable indexelastic (default: false)
    master_timeout
        Specify timeout for connection to master
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_delete testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.indices.delete(
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.exceptions.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete index {index}, server returned errors {err.errors}"
        ) from err


def index_exists(
    index,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    ignore_unavailable=False,
    include_defaults=False,
    local=False,
    pretty=None,
):
    """
    Return a boolean indicating whether given index exists

    index
        A comma-separated list of index names
    allow_no_indices
        Ignore if a wildcard expression resolvelastic to no concrete
        indices (default: false)
    expand_wildcards
        Whether wildcard expressions should get expanded to
        open or closed indices (default: open)
        Accepts values: ('all', 'closed', 'hidden', 'none', 'open')
    flat_settings
        Return settings in flat format (default: false)
    ignore_unavailable
        Ignore unavailable indexelastic (default: false)
    include_defaults
        Whether to return all default setting for each of the
        indices.
    local
        Return local information, do not retrieve the state from master
        node (default: false)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_exists testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.exists(
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            ignore_unavailable=ignore_unavailable,
            include_defaults=include_defaults,
            local=local,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return False
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve index {index}, server returned errors {err.errors}"
        ) from err


def index_get(
    index,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    features=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    ignore_unavailable=None,
    include_defaults=False,
    local=False,
    master_timeout=None,
    pretty=None,
):
    """
    Returns information about one or more indices.

    index
        Comma-separated list of data streams, indices, and index aliases
        used to limit the request. Wildcard expressions (*) are supported.
    allow_no_indices
        If false, the request returns an error if any wildcard
        expression, index alias, or _all value targets only missing or closed indices.
        This behavior applielastic even if the request targets other open indices. For
        example, a request targeting foo*,bar* returns an error if an index starts
        with foo but no index starts with bar.
    expand_wildcards
        Type of index that wildcard expressions can match. If
        the request can target data streams, this argument determinelastic whether wildcard
        expressions match hidden data streams. Supports comma-separated values, such
        as 'all', 'closed', 'hidden', 'none', 'open'
    features
        Return only information on specified index featurelastic.
        Support valuelastic such as 'aliases', 'mappings', 'settings'
    flat_settings
        If true, returns settings in flat format.
    ignore_unavailable
        If false, requests that target a missing index return
        an error.
    include_defaults
        If true, return all default settings in the response.
    local
        If true, the request retrievelastic information from the local node
        only. Defaults to false, which means information is retrieved from the master
        node.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_get testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.get(
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            features=features,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            ignore_unavailable=ignore_unavailable,
            include_defaults=include_defaults,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve index {index}, server returned errors {err.errors}"
        ) from err


def index_open(
    index,
    allow_no_indices=True,
    expand_wildcards="closed",
    ignore_unavailable=True,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
    wait_for_active_shards=None,
):
    """
    .. versionadded:: 3005.1

    Open specified index.

    index
        A comma separated list of indices to open
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valid choicelastic are ('all', 'closed', 'hidden', 'none', 'open')
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    master_timeout
        Specify timeout for connection to master
    timeout
        Explicit operation timeout
    wait_for_active_shards
        Sets the number of active shards to wait for before
        the operation returns. Valid choicelastic are an integer or 'all', 'index-setting' strings

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_open testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.indices.open(
            index=index,
            allow_no_indices=allow_no_indices,
            expand_wildcards=expand_wildcards,
            ignore_unavailable=ignore_unavailable,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
        ).body

        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot open index {index}, server returned errors {err.errors}"
        ) from err


def index_close(
    index,
    allow_no_indices=True,
    expand_wildcards="open",
    ignore_unavailable=True,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
    wait_for_active_shards=None,
):
    """
    .. versionadded:: 2017.7.0

    Close specified index.

    index
        A comma separated list of indices to close
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valid choicelastic are ('all', 'closed', 'hidden', 'none', 'open')
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    master_timeout
        Specify timeout for connection to master
    timeout
        Explicit operation timeout
    wait_for_active_shards
        Sets the number of active shards to wait for before
        the operation returns. Valid choicelastic are an integer or 'all', 'index-setting' strings

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_close testindex
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.indices.close(
            index=index,
            allow_no_indices=allow_no_indices,
            expand_wildcards=expand_wildcards,
            ignore_unavailable=ignore_unavailable,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
            wait_for_active_shards=wait_for_active_shards,
        ).body

        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot close index {index}, server returned errors {err.errors}"
        ) from err


def index_get_settings(
    hosts=None,
    profile=None,
    index=None,
    name=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    flat_settings=None,
    human=None,
    ignore_unavailable=None,
    include_defaults=None,
    local=None,
    master_timeout=None,
    pretty=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3000

    Check for the existence of an index and if it exists, return its settings
    https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-settings.html

    index
        (Optional, string) A comma-separated list of index names;
        use _all or empty string for all indices. Defaults to '_all'.
    name
        (Optional, string) The name of the settings that should be included
    allow_no_indices
        (Optional, boolean) Whether to ignore if a wildcard indices expression resolves into no concrete indices.
        (This includelastic _all string or when no indices have been specified)
    expand_wildcards
        (Optional, string) Whether to expand wildcard expression to concrete indices that are open, closed or both.
        Valid choicelastic are: open closed, none, all, hidden
    flat_settings
        (Optional, boolean) Return settings in flat format
    ignore_unavailable
        (Optional, boolean) Whether specified concrete indices should be ignored when unavailable (missing or closed)
    include_defaults
        (Optional, boolean) Whether to return all default setting for each of the indices.
    local
        (Optional, boolean) Return local information, do not retrieve the state from master node

    The defaults settings for the above parameters depend on the API version being used.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_get_settings index=testindex
    """

    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.get_settings(
            index=index,
            name=name,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            ignore_unavailable=ignore_unavailable,
            include_defaults=include_defaults,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve index settings, server returned errors {err.errors}"
        ) from err


def index_put_settings(
    hosts=None,
    profile=None,
    source=None,
    settings=None,
    index=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    flat_settings=None,
    human=None,
    ignore_unavailable=None,
    master_timeout=None,
    preserve_existing=None,
    pretty=None,
    timeout=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3000

    Update existing index settings
    https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html

    source
        URL to file specifying index definition. Cannot be used in combination with body.
    settings:
        The index settings to be updated
    index
        A comma-separated list of index names; use _all or empty string
        to perform the operation on all indices
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valuelastic can be 'all', 'closed', 'hidden', 'none', 'open'
    flat_settings
        Return settings in flat format (default: false)
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    master_timeout
        Specify timeout for connection to master
    preserve_existing
        Whether to update existing settings. If set to true
        existing settings on an index remain unchanged, the default is false
    timeout
        Explicit operation timeout

    The defaults settings for the above parameters depend on the API version being used.

    .. note::
        Elasticsearch time units can be found here:

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_put_settings index=testindex
          body='{"settings" : {"index" : {"number_of_replicas" : 2}}}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    if source and settings:
        message = "Either settings or source should be specified but not both."
        raise SaltInvocationError(message)

    src_map = {}
    if source:
        src_map = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))
        settings = src_map.get("settings", settings)
    try:
        result = elastic.indices.put_settings(
            settings=settings,
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout,
            preserve_existing=preserve_existing,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot update index settings, server returned errors {err.errors}"
        ) from err


def mapping_create(
    index,
    hosts=None,
    profile=None,
    source=None,
    allow_no_indices=None,
    date_detection=None,
    dynamic=None,
    dynamic_date_formats=None,
    dynamic_templates=None,
    error_trace=None,
    expand_wildcards=None,
    field_names=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    master_timeout=None,
    meta=None,
    numeric_detection=None,
    pretty=None,
    properties=None,
    routing=None,
    runtime=None,
    source_=None,
    timeout=None,
    write_index_only=None,
):
    """
    # pylint: disable=line-too-long
    Create a mapping in a given index

    index
        A comma-separated list of index namelastic the mapping should be added
        to (supports wildcards); use _all or omit to add the mapping on all indices.
    source
        URL to file specifying mapping definition. Cannot be used in combination with body.
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    date_detection
        Controls whether dynamic date detection is enabled.
    dynamic
        Controls whether new fields are added dynamically.
    dynamic_date_formats
        If date detection is enabled then new string fields
        are checked against 'dynamic_date_formats' and if the value matchelastic then
        a new date field is added instead of string.
    dynamic_templates
        Specify dynamic templatelastic for the mapping.
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
    field_names
        Control whether field namelastic are enabled for the index.
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    master_timeout
        Specify timeout for connection to master
    meta
        A mapping type can have custom meta data associated with it. These
        are not used at all by Elasticsearch, but can be used to store application-specific
        metadata.
    numeric_detection
        Automatically map strings into numeric data typelastic for
        all fields.
    properties
        Mapping for a field. For new fields, this mapping can include:
        - Field name - Field data type - Mapping parameters
    routing
        Enable making a routing value required on indexed documents.
    runtime
        Mapping of runtime fields for the index.
    `source_`
        Control whether the _source field is enabled on the index.
    timeout
        Explicit operation timeout
    write_index_only
        When true, applielastic mappings only to the write index
        of an alias or data stream

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.mapping_create testindex user \
       '{ "user" : { "properties" : { "message" : {"type" : "string", "store" : true } } } }'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    if source and properties:
        message = "Either properties or source should be specified but not both."
        raise SaltInvocationError(message)

    src_map = {}
    if source:
        src_map = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))
        properties = src_map.get("properties", properties)
    try:
        result = elastic.indices.put_mapping(
            index=index,
            allow_no_indices=allow_no_indices,
            date_detection=date_detection,
            dynamic=dynamic,
            dynamic_date_formats=dynamic_date_formats,
            dynamic_templates=dynamic_templates,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            field_names=field_names,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout,
            meta=meta,
            numeric_detection=numeric_detection,
            pretty=pretty,
            properties=properties,
            routing=routing,
            runtime=runtime,
            source=source_,
            timeout=timeout,
            write_index_only=write_index_only,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create mapping {index}, server returned errors {err.errors}"
        ) from err


def mapping_get(
    index,
    hosts=None,
    profile=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    local=None,
    master_timeout=None,
    pretty=None,
):
    """
    Retrieve mapping definition of index

    index
        A comma-separated list of index names
    allow_no_indices
        Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards
        Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
    ignore_unavailable
        Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    local
        Return local information, do not retrieve the state from master
        node (default: false)
    master_timeout
        Specify timeout for connection to master

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.mapping_get testindex user
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.get_mapping(
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve mapping {index}, server returned errors {err.errors}"
        ) from err


def index_template_create(
    name,
    hosts=None,
    profile=None,
    source=None,
    aliases=None,
    create=None,
    error_trace=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    index_patterns=None,
    mappings=None,
    master_timeout=None,
    order=None,
    pretty=None,
    settings=None,
    timeout=None,
    version=None,
):
    """
    # pylint: disable=line-too-long
    Create an index template

    name
        The name of the template
    source
        URL to file specifying template definition. Cannot be used in combination with settings or mappings.
    aliases
        Aliases for the index.
    create
        If true, this request cannot replace or update existing index
        templatelastic.
    error_trace
        error_trace
    filter_path
        filter_path
    flat_settings
        Return settings in flat format (default: false)
    index_patterns
        Array of wildcard expressions used to match the names
        of indices during creation.
    mappings
        Mapping for fields in the index.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.
    order
        Order in which Elasticsearch applielastic this template if index matches
        multiple templatelastic. Templatelastic with lower 'order' values are merged first.
        Templatelastic with higher 'order' values are merged later, overriding templates
        with lower valuelastic.
    settings
        Configuration options for the index.
    timeout
        timeout
    version
        ersion number used to manage index templatelastic externally. This
        number is not automatically generated by Elasticsearch.

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.index_template_create testindex_templ
       '{ "template": "logstash-*", "order": 1, "settings": { "number_of_shards": 1 } }'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    if source and (settings or mappings):
        message = "Either settings or source or mappings should be specified but not both."
        raise SaltInvocationError(message)
    src_map = {}
    if source:
        src_map = __salt__["cp.get_file_str"](source, saltenv=__opts__.get("saltenv", "base"))
        settings = src_map.get("settings", settings)
        mappings = src_map.get("mappings", mappings)
    try:
        result = elastic.indices.put_template(
            name=name,
            aliases=aliases,
            create=create,
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            index_patterns=index_patterns,
            mappings=mappings,
            master_timeout=master_timeout,
            order=order,
            pretty=pretty,
            settings=settings,
            timeout=timeout,
            version=version,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create template {name}, server returned errors {err.errors}"
        ) from err


def index_template_delete(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    Delete an index template (type) along with its data

    name
        The name of the template
    master_timeout
        Specify timeout for connection to master
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_template_delete testindex_templ user
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        result = elastic.indices.delete_template(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body

        return result.get("acknowledged", False)
    except elasticsearch.exceptions.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete template {name}, server returned errors {err.errors}"
        ) from err


def index_template_exists(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
):
    """
    Return a boolean indicating whether given index template exists

    name
        Comma-separated list of index template namelastic used to limit the request.
        Wildcard (*) expressions are supported.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_template_exists testindex_templ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        return elastic.indices.exists_index_template(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve template {name}, server returned errors {err.errors}"
        ) from err


def template_exists(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    flat_settings=False,
    human=None,
    local=False,
    master_timeout=None,
    pretty=None,
):
    """
    Return a boolean indicating whether given index template exists

    name
        Comma-separated list of index template namelastic used to limit the request.
        Wildcard (*) expressions are supported.
    flat_settings
        Return settings in flat format (default: false)
    local
        Return local information, do not retrieve the state from master
        node (default: false)
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_template_exists testindex_templ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        return elastic.indices.exists_template(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve template {name}, server returned errors {err.errors}"
        ) from err


def index_template_get(
    name=None,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    flat_settings=None,
    human=None,
    local=None,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Retrieve template definition of index or index/type

    name
        The comma separated namelastic of the index templates
    flat_settings
        Return settings in flat format (default: false)
    local
        Return local information, do not retrieve the state from master
        node (default: false)
    master_timeout
        Explicit operation timeout for connection to master node

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.index_template_get testindex_templ
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.get_template(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            flat_settings=flat_settings,
            human=human,
            local=local,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.exceptions.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot retrieve template {name}, server returned errors {err.errors}"
        ) from err


def geo_ip_stats(
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Returns statistical information about geoip databases

    https://www.elastic.co/guide/en/elasticsearch/reference/current/geoip-stats-api.html

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.geo_ip_stats
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.ingest.geo_ip_stats(
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot get geo_ip_stats, server returned errors {err.errors}"
        ) from err


def processor_grok(
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Returns a list of built-in patterns

    https://www.elastic.co/guide/en/elasticsearch/reference/current/grok-processor.html#grok-processor-rest-get

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.processor_grok
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.ingest.processor_grok(
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot get built-in patterns, server returned errors {err.errors}"
        ) from err


def pipeline_get(
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    summary=None,
):
    """
    .. versionadded:: 3005.1

    Retrieve Ingest pipeline definition. Available since Elasticsearch 5.0.

    `id_`
        Comma separated list of pipeline ids. Wildcards supported
    master_timeout
        Explicit operation timeout for connection to master node
    summary
        Return pipelinelastic without their definitions (default: false)

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.pipeline_get mypipeline
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.ingest.get_pipeline(
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            summary=summary,
        ).body
    except elasticsearch.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create pipeline {id}, server returned errors {err.errors}"
        ) from err
    except AttributeError as err:
        raise CommandExecutionError("Method is applicable only for Elasticsearch 5.0+") from err


def pipeline_delete(
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Delete Ingest pipeline. Available since Elasticsearch 5.0.

    `id_`
        Pipeline ID
    master_timeout
        Explicit operation timeout for connection to master node
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.pipeline_delete mypipeline
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        ret = elastic.ingest.delete_pipeline(
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return ret.get("acknowledged", False)
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete pipeline {id}, server returned errors {err.errors}"
        ) from err
    except AttributeError as err:
        raise CommandExecutionError("Method is applicable only for Elasticsearch 5.0+") from err


def pipeline_create(
    id_,
    hosts=None,
    profile=None,
    description=None,
    error_trace=None,
    filter_path=None,
    human=None,
    if_version=None,
    master_timeout=None,
    meta=None,
    on_failure=None,
    pretty=None,
    processors=None,
    timeout=None,
    version=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    Create Ingest pipeline by supplied definition. Available since Elasticsearch 5.0.

    `id_`
        Pipeline id
    description
        Description of the ingest pipeline.
    if_version
        Required version for optimistic concurrency control for pipeline updates
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.
    meta
        Optional metadata about the ingest pipeline. May have any contents.
        This map is not automatically generated by Elasticsearch.
    on_failure
        Processors to run immediately after a processor failure. Each
        processor supports a processor-level on_failure value. If a processor without
        an on_failure value fails, Elasticsearch uselastic this pipeline-level parameter
        as a fallback. The processors in this parameter run sequentially in the order
        specified. Elasticsearch will not attempt to run the pipeline's remaining
        processors.
    processors
        Processors used to perform transformations on documents before
        indexing. Processors run sequentially in the order specified.
    timeout
        Period to wait for a response. If no response is received before
        the timeout expires, the request fails and returns an error.
    version
        Version number used by external systems to track ingest pipelinelastic.
        This parameter is intended for external systems only. Elasticsearch does
        not use or validate pipeline version numbers.

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.pipeline_create mypipeline
       '{"description": "my custom pipeline", "processors": [{"set" : {"field": "collector_timestamp_millis",
       "value": "{{_ingest.timestamp}}"}}]}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        out = elastic.ingest.put_pipeline(
            id=id_,
            description=description,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            if_version=if_version,
            master_timeout=master_timeout,
            meta=meta,
            on_failure=on_failure,
            pretty=pretty,
            processors=processors,
            timeout=timeout,
            version=version,
        ).body
        return out.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create pipeline {id}, server returned errors {err.errors}"
        ) from err
    except AttributeError as err:
        raise CommandExecutionError("Method is applicable only for Elasticsearch 5.0+") from err


def pipeline_simulate(
    id_=None,
    hosts=None,
    profile=None,
    docs=None,
    error_trace=None,
    filter_path=None,
    human=None,
    pipeline=None,
    pretty=None,
    verbose=False,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    Simulate existing Ingest pipeline on provided data. Available since Elasticsearch 5.0.

    `id_`
        Pipeline id
    docs:
        Documents
    verbose
        Specify if the output should be more verbose

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.pipeline_simulate mypipeline
       '{"docs":[{"_index":"index","_type":"type","_id":"id","_source":{"foo":"bar"}},
       {"_index":"index","_type":"type","_id":"id","_source":{"foo":"rab"}}]}' verbose=True
    """
    elastic = _get_instance(hosts=hosts, profile=profile)
    try:
        result = elastic.ingest.simulate(
            id=id_,
            docs=docs,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            pipeline=pipeline,
            pretty=pretty,
            verbose=verbose,
        ).body
        return result

    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot simulate pipeline {id}, server returned errors {err.errors}"
        ) from err
    except AttributeError as err:
        raise CommandExecutionError("Method is applicable only for Elasticsearch 5.0+") from err


def script_get(
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Obtain existing script definition.

    `id_`
        Script ID
    master_timeout
        Specify timeout for connection to master

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.script_template_get mytemplate
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.get_script(
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain search template {id}, server returned errors {err.errors}"
        ) from err


def script_create(
    id_,
    script=None,
    hosts=None,
    profile=None,
    context=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    Create cript by supplied script definition

    .. versionadded:: 3005.1

    script
        Script definition

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.script_create mytemplate
        '{"template":{"query":{"match":{"title":"{{query_string}}"}}}}'

    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.put_script(
            id=id_,
            script=script,
            context=context,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create search template {id}, server returned errors {err.errors}"
        ) from err


def script_delete(
    id_,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Delete existing script.

    `id_`
        Script ID
    master_timeout
        Specify timeout for connection to master
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.script_delete id=id
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.delete_script(
            id=id_,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete search template {id}, server returned errors {err.errors}"
        ) from err


def repository_get(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    local=False,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Get existing repository details.

    name
                comma-separated list of repository names
    local
                Return local information, do not retrieve the state from master node (default: false)
    master_timeout
                Explicit operation timeout for connection to master node

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.repository_get testrepo
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.snapshot.get_repository(
            name=name,
            local=local,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain repository {name}, server returned errors {err.errors}"
        ) from err


def repository_cleanup(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Removelastic stale data from repository

    name
        comma-separated list of repository names
    local
        Return local information, do not retrieve the state from master node (default: false)
    master_timeout
        Explicit operation timeout for connection to master node
    timeout
        Period to wait for a response.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.repository_get testrepo
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.snapshot.cleanup_repository(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain repository {name}, server returned errors {err.errors}"
        ) from err


def repository_create(
    name,
    hosts=None,
    profile=None,
    type_=None,
    settings=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    repository=None,
    timeout=None,
    verify=None,
    body=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    Create repository for storing snapshots. Note that shared repository paths have to be specified in path.repo
    Elasticsearch configuration option.

    name
            A repository name
    hosts
        List of hosts to connect
    profile
        Security profile to use
    settings:
            Repository settings definition
    `type_`:
            Repository type
    master_timeout
            Explicit operation timeout for connection to master node
    repository
            Repository
    timeout
            Explicit operation timeout
    verify
            Whether to verify the repository after creation
    body
        Repository definition as in
        https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
        The use of body is deprecated and it will be disabled in a coming release

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.repository_create testrepo
       '{"type":"fs","settings":{"location":"/tmp/test","compress":true}}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        if body is not None:
            result = elastic.snapshot.create_repository(name=name, type=type_, settings=body).body
        else:
            result = elastic.snapshot.create_repository(
                name=name,
                type=type_,
                settings=settings,
                error_trace=error_trace,
                filter_path=filter_path,
                human=human,
                master_timeout=master_timeout,
                pretty=pretty,
                repository=repository,
                timeout=timeout,
                verify=verify,
            ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create repository {name}, server returned errors {err.errors}"
        ) from err


def repository_delete(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Delete existing repository.

        name
                Name of the snapshot repository to unregister. Wildcard (*) patterns are supported.
        master_timeout
                Explicit operation timeout for connection to master node
        timeout
                Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.repository_delete testrepo
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.snapshot.delete_repository(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete repository {name}, server returned errors {err.errors}"
        ) from err


def repository_verify(
    name,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Obtain list of cluster nodes which successfully verified this repository.

    name
        Repository name
    master_timeout
        Explicit operation timeout for connection to master node
    timeout
        Explicit operation timeout

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.repository_verify testrepo
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.snapshot.verify_repository(
            name=name,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
    except elasticsearch.NotFoundError:
        return None
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot verify repository {name}, server returned errors {err.errors}"
        ) from err


def snapshot_status(
    hosts=None,
    profile=None,
    repository=None,
    snapshot=None,
    error_trace=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Obtain status of all currently running snapshots.

    repository
        Particular repository to look for snapshots
    snapshot
        A comma-separated list of snapshot names
    ignore_unavailable
        Whether to ignore unavailable snapshots, defaults
        to false which means a SnapshotMissingException is thrown
    master_timeout
        Explicit operation timeout for connection to master node

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.snapshot_status ignore_unavailable=True
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.snapshot.status(
            repository=repository,
            snapshot=snapshot,
            ignore_unavailable=ignore_unavailable,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain snapshot status, server returned errors {err.errors}"
        ) from err


def snapshot_clone(
    hosts=None,
    profile=None,
    repository=None,
    indices=None,
    target_snapshot=None,
    snapshot=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
    timeout=None,
):
    """
    .. versionadded:: 3005.1

    Clonelastic indices from one snapshot into another snapshot in the same repository.

    repository
        Particular repository to look for snapshots
    indices
        List of indices to snapshot
    snapshot
        The name of the snapshot to clone from
    target_snapshot
        The name of the cloned snapshot to create
    master_timeout
        Explicit operation timeout for connection to master node

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.snapshot_status ignore_unavailable=True
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.snapshot.clone(
            repository=repository,
            indices=indices,
            snapshot=snapshot,
            target_snapshot=target_snapshot,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
            timeout=timeout,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain snapshot status, server returned errors {err.errors}"
        ) from err


def snapshot_get(
    repository,
    snapshot,
    hosts=None,
    profile=None,
    after=None,
    error_trace=None,
    filter_path=None,
    from_sort_value=None,
    human=None,
    ignore_unavailable=False,
    include_repository=None,
    index_details=None,
    index_names=None,
    master_timeout=None,
    offset=None,
    order=None,
    pretty=None,
    size=None,
    slm_policy_filter=None,
    sort=None,
    verbose=None,
):
    """
    .. versionadded:: 3005.1

    Obtain snapshot residing in specified repository.

        repository
            Comma-separated list of snapshot repository namelastic used to
            limit the request. Wildcard (*) expressions are supported.
        snapshot
            Comma-separated list of snapshot namelastic to retrieve. Also accepts
            wildcards (*). - To get information about all snapshots in a registered repository,
            use a wildcard (*) or _all. - To get information about any snapshots that
            are currently running, use _current.
        after
            Offset identifier to start pagination from as returned by the next
            field in the response body.
        from_sort_value
            Value of the current sort column at which to start retrieval.
            Can either be a string snapshot- or repository name when sorting by snapshot
            or repository name, a millisecond time value or a number when sorting by
            index- or shard count.
        ignore_unavailable
            If false, the request returns an error for any snapshots
            that are unavailable.
        include_repository
            If true, returns the repository name in each snapshot.
        index_details
            If true, returns additional information about each index
            in the snapshot comprising the number of shards in the index, the total size
            of the index in bytes, and the maximum number of segments per shard in the
            index. Defaults to false, meaning that this information is omitted.
        index_names
            If true, returns the name of each index in each snapshot.
        master_timeout
            Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        offset
            Numeric offset to start pagination from based on the snapshots
            matching this request. Using a non-zero value for this parameter is mutually
            exclusive with using the after parameter. Defaults to 0.
        order
            Sort order. Valid valuelastic are asc for ascending and desc for descending
            order. Defaults to asc, meaning ascending order.
        size
            Maximum number of snapshots to return. Defaults to 0 which means
            return all that match the request without limit.
        slm_policy_filter
            Filter snapshots by a comma-separated list of SLM policy
            namelastic that snapshots belong to. Also accepts wildcards (*) and combinations
            of wildcards followed by exclude patterns starting with -. To include snapshots
            not created by an SLM policy you can use the special pattern _none that will
            match all snapshots without an SLM policy.
        sort
            Allows setting a sort order for the result. Defaults to start_time,
            i.e. sorting by snapshot start time stamp.
        verbose
            If true, returns additional information about each snapshot such
            as the version of Elasticsearch which took the snapshot, the start and end
            timelastic of the snapshot, and the number of shards snapshotted.

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.snapshot_get testrepo testsnapshot
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.snapshot.get(
            repository=repository,
            snapshot=snapshot,
            ignore_unavailable=ignore_unavailable,
            after=after,
            error_trace=error_trace,
            filter_path=filter_path,
            from_sort_value=from_sort_value,
            human=human,
            include_repository=include_repository,
            index_details=index_details,
            index_names=index_names,
            master_timeout=master_timeout,
            offset=offset,
            order=order,
            pretty=pretty,
            size=size,
            slm_policy_filter=slm_policy_filter,
            sort=sort,
            verbose=verbose,
        ).body
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot obtain details of snapshot {snapshot} in repository {repository}, "
            f"server returned errors {err.errors}"
        ) from err


def snapshot_create(
    repository,
    snapshot,
    hosts=None,
    profile=None,
    error_trace=None,
    feature_states=None,
    filter_path=None,
    human=None,
    ignore_unavailable=None,
    include_global_state=None,
    indices=None,
    master_timeout=None,
    metadata=None,
    partial=None,
    pretty=None,
    wait_for_completion=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    Create snapshot in specified repository by supplied definition.

    repository
        Repository name
    snapshot
        Snapshot name
    feature_states
        Feature statelastic to include in the snapshot. Each feature
        state includelastic one or more system indices containing related data. You can
        view a list of eligible featurelastic using the get features API. If include_global_state
        is true, all current feature statelastic are included by default. If include_global_state
        is false, no feature statelastic are included by default.
    ignore_unavailable
        If true, the request ignorelastic data streams and indices
        in indices that are missing or closed. If false, the request returns
        an error for any data stream or index that is missing or closed.
    include_global_state
        If true, the current cluster state is included
        in the snapshot. The cluster state includelastic persistent cluster settings,
        composable index templates, legacy index templates, ingest pipelines, and
        ILM policielastic. It also includelastic data stored in system indices, such as Watches
        and task records (configurable via feature_states).
    indices
        Data streams and indices to include in the snapshot. Supports
        multi-target syntax. Includelastic all data streams and indices by default.
    master_timeout
        Period to wait for a connection to the master node. If
        no response is received before the timeout expires, the request fails and
        returns an error.
    metadata
        Optional metadata for the snapshot. May have any contents. Must
        be less than 1024 bytelastic. This map is not automatically generated by Elasticsearch.
    partial
        If true, allows restoring a partial snapshot of indices with
        unavailable shards. Only shards that were successfully included in the snapshot
        will be restored. All missing shards will be recreated as empty. If false,
        the entire restore operation will fail if one or more indices included in
        the snapshot do not have all primary shards available.
    wait_for_completion
        If true, the request returns a response when the
        snapshot is complete. If false, the request returns a response when the
        snapshot initializelastic.

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.snapshot_create testrepo testsnapshot
       '{"indices":"index_1,index_2","ignore_unavailable":true,"include_global_state":false}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        response = elastic.snapshot.create(
            repository=repository,
            snapshot=snapshot,
            error_trace=error_trace,
            feature_states=feature_states,
            filter_path=filter_path,
            human=human,
            ignore_unavailable=ignore_unavailable,
            include_global_state=include_global_state,
            indices=indices,
            master_timeout=master_timeout,
            metadata=metadata,
            partial=partial,
            pretty=pretty,
            wait_for_completion=wait_for_completion,
        ).body
        return response.get("accepted", False)
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot create snapshot {snapshot} in repository {repository}, server returned errors {err.errors}"
        ) from err


def snapshot_restore(
    repository,
    snapshot,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    ignore_index_settings=None,
    ignore_unavailable=None,
    include_aliases=None,
    include_global_state=None,
    index_settings=None,
    indices=None,
    master_timeout=None,
    partial=None,
    pretty=None,
    rename_pattern=None,
    rename_replacement=None,
    wait_for_completion=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    Restore existing snapshot in specified repository by supplied definition.

    repository
        Repository name
    snapshot
        Snapshot name
    ignore_index_settings
        ignore_index_settings
    ignore_unavailable
        ignore_unavailable
    include_aliases
        include_aliases
    include_global_state
        include_global_state
    index_settings
        index_settings
    indices
        A list of indices to restore
    master_timeout
        Explicit operation timeout for connection to master node
    partial
        partial
    rename_pattern
        rename_pattern
    rename_replacement
        rename_replacement
    wait_for_completion
        Should this request wait until the operation has completed before returning

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.snapshot_restore testrepo testsnapshot
       '{"indices":"index_1,index_2","ignore_unavailable":true,"include_global_state":true}'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        response = elastic.snapshot.restore(
            repository=repository,
            snapshot=snapshot,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            ignore_index_settings=ignore_index_settings,
            ignore_unavailable=ignore_unavailable,
            include_aliases=include_aliases,
            include_global_state=include_global_state,
            index_settings=index_settings,
            indices=indices,
            master_timeout=master_timeout,
            partial=partial,
            pretty=pretty,
            rename_pattern=rename_pattern,
            rename_replacement=rename_replacement,
            wait_for_completion=wait_for_completion,
        ).body
        return response.get("accepted", False)
    except elasticsearch.ApiError as err:
        raise CommandExecutionError(
            f"Cannot restore snapshot {snapshot} in repository {repository}, server returned errors {err.errors}"
        ) from err


def snapshot_delete(
    repository,
    snapshot,
    hosts=None,
    profile=None,
    error_trace=None,
    filter_path=None,
    human=None,
    master_timeout=None,
    pretty=None,
):
    """
    .. versionadded:: 3005.1

    Delete snapshot from specified repository.

    repository
        Repository name
    snapshot
        Snapshot name
    master_timeout
        Explicit operation timeout for connection to master node

    CLI Example:

    .. code-block:: bash

        salt myminion elasticsearch.snapshot_delete testrepo testsnapshot
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        result = elastic.snapshot.delete(
            repository=repository,
            snapshot=snapshot,
            error_trace=error_trace,
            filter_path=filter_path,
            human=human,
            master_timeout=master_timeout,
            pretty=pretty,
        ).body
        return result.get("acknowledged", False)
    except elasticsearch.NotFoundError:
        return True
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(
            f"Cannot delete snapshot {snapshot} from repository {repository}, server returned errors {err.errors}"
        ) from err


def flush(
    hosts=None,
    profile=None,
    index=None,
    allow_no_indices=None,
    error_trace=None,
    expand_wildcards=None,
    filter_path=None,
    force=None,
    human=None,
    ignore_unavailable=None,
    pretty=None,
    wait_if_ongoing=None,
):
    """
    # pylint: disable=line-too-long
    .. versionadded:: 3005.1

    index: A comma-separated list of index names; use _all or empty string
        for all indices
    allow_no_indices: Whether to ignore if a wildcard indices expression resolves
        into no concrete indices. (This includelastic _all string or when no indices
        have been specified)
    expand_wildcards: Whether to expand wildcard expression to concrete indices
        that are open, closed or both.
        Valid valuelastic are::

            all - Expand to open and closed indices.
            open - Expand only to open indices.
            closed - Expand only to closed indices.
            none - Wildcard expressions are not accepted.

    force: Whether a flush should be forced even if it is not necessarily
        needed ie. if no changelastic will be committed to the index. This is useful if
        transaction log IDs should be incremented even if no uncommitted changes
        are present. (This setting can be considered as internal)
    ignore_unavailable: Whether specified concrete indices should be ignored
        when unavailable (missing or closed)
    wait_if_ongoing: If set to true the flush operation will block until the
        flush can be executed if another flush operation is already executing. The
        default is true. If set to false the flush will be skipped iff if another
        flush operation is already running.


    The defaults settings for the above parameters depend on the API being used.

    CLI Example:

    .. code-block:: bash

     salt myminion elasticsearch.flush index='index1,index2' ignore_unavailable=True
       allow_no_indices=True expand_wildcards='all'
    """
    elastic = _get_instance(hosts=hosts, profile=profile)

    try:
        return elastic.indices.flush(
            index=index,
            allow_no_indices=allow_no_indices,
            error_trace=error_trace,
            expand_wildcards=expand_wildcards,
            filter_path=filter_path,
            force=force,
            human=human,
            ignore_unavailable=ignore_unavailable,
            pretty=pretty,
            wait_if_ongoing=wait_if_ongoing,
        ).body
    except elasticsearch.TransportError as err:
        raise CommandExecutionError(f"Cannot flush, server returned errors {err.errors}") from err
