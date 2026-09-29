"""
Return data to an elasticsearch server for indexing.

Copied from original returner and modified to support elasticsearch 8.x

Original maintainers: Jurnell Cockhren <jurnell.cockhren@sophicware.com>, Arnold Bechtoldt <mail@arnoldbechtoldt.com>

:maintainer: Cesar Sanchez <cesan3@gmail.com>

To enable this returner the elasticsearch python client must be installed
on the desired minions (all or some subset).

Please see documentation of :mod:`elasticsearch execution module <salt.modules.elasticsearch>`
for a valid connection configuration.

.. warning::

        The index that you wish to store documents will be created by Elasticsearch automatically if
        doesn't exist yet. It is highly recommended to create predefined index templates with appropriate mapping(s)
        that will be used by Elasticsearch upon index creation. Otherwise you will have problems as described in #20826.

To use the returner per salt call:

.. code-block:: bash

    salt '*' test.ping --return elasticsearch

In order to have the returner apply to all minions:

.. code-block:: yaml

    ext_job_cache: elasticsearch

Minion configuration:
    debug_returner_payload': False
        Output the payload being posted to the log file in debug mode

    doc_type: 'default'
        Document type to use for normal return messages

    functions_blacklist
        Optional list of functions that should not be returned to elasticsearch

    index_date: False
        Use a dated index (e.g. <index>-2016.11.29)

    master_event_index: 'salt-master-event-cache'
        Index to use when returning master events

    master_event_doc_type: 'default'
        Document type to use for master events

    master_job_cache_index: 'salt-master-job-cache'
        Index to use for master job cache

    master_job_cache_doc_type: 'default'
        Document type to use for master job cache

    number_of_shards: 1
        Number of shards to use for the indexes

    number_of_replicas: 0
        Number of replicas to use for the indexes

    NOTE: The following options are valid for 'state.apply', 'state.sls' and 'state.highstate' functions only.

    states_count: False
        Count the number of states which succeeded or failed and return it in top-level item called 'counts'.
        States reporting None (i.e. changes would be made but it ran in test mode) are counted as successes.
    states_order_output: False
        Prefix the state UID (e.g. file_|-yum_configured_|-/etc/yum.conf_|-managed) with a zero-padded version
        of the '__run_num__' value to allow for easier sorting. Also store the state function (i.e. file.managed)
        into a new key '_func'. Change the index to be '<index>-ordered' (e.g. salt-state_apply-ordered).
    states_single_index: False
        Store results for state.apply, state.sls and state.highstate in the salt-state_apply index
        (or -ordered/-<date>) indexes if enabled

.. code-block:: yaml

    elasticsearch:
        hosts:
          - "10.10.10.10:9200"
          - "10.10.10.11:9200"
          - "10.10.10.12:9200"
        index_date: True
        number_of_shards: 5
        number_of_replicas: 1
        debug_returner_payload: True
        states_count: True
        states_order_output: True
        states_single_index: True
        functions_blacklist:
          - test.ping
          - saltutil.find_job
"""

import datetime
import logging
import uuid

from datetime import timedelta
from datetime import tzinfo
from salt.exceptions import CommandExecutionError

import salt.returners
import salt.utils.jid
import salt.utils.json


JOB_QUERY="""
{
    "query": {
        "match": {
            "jid": JID
        }
    },
    "size": 10000
}
"""
MINION_QUERY="""
{
  "size": 0,
  "aggs": {
    "unique_field_values": {
      "terms": {
        "field": "load.id.keyword",
        "size": 10000
      }
    }
  }
}
"""
FUNCTION_QUERY="""
{
  "size": 1,
  "sort": [
    {
      "jid.keyword": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "match_all": {}
  }
}
"""

RETURN_MAPPING={
    "@timestamp": {
        "type": "date"
    },
    "counts": {
        "type": "object"
    },
    "data": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "fun": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Function.keyword": {
        "type": "alias",
        "path": "fun.keyword"
    },
    "fun_args": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Arguments.keyword": {
        "type": "alias",
        "path": "fun_args.keyword"
    },
    "jid": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "minion": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Minions.keyword": {
        "type": "alias",
        "path": "minion.keyword"
    },
    "retcode": {
            "type": "long"
    },
    "success": {
        "type": "boolean"
    }
}

MASTER_MAPPING={
    "@timestamp": {
        "type": "date"
    },
    "Arguments": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Function": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Minions": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Schedule": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "StartTime": {
        "type": "date"
    },
    "Target": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "Target-type": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "User": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "jid": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    }
}

class UTC(tzinfo):
    def utcoffset(self, _dt):
        return timedelta(0)

    def tzname(self, _dt):
        return "UTC"

    def dst(self, _dt):
        return timedelta(0)

def _load_modules_ssh(__salt__):
    loaded = False
    import salt.modules.config
    for f in ["elasticsearch.index_exists", 
                "elasticsearch.index_create", 
                "elasticsearch.alias_create", 
                "elasticsearch.alias_exists",
                "elasticsearch.alias_delete",
                "elasticsearch.document_create", 
                "elasticsearch.document_exists", 
                "elasticsearch.document_get", 
                "elasticsearch.document_get_all",
                "elasticsearch.document_update",
                "elasticsearch.search"]:
        if f not in __salt__:
            if not loaded:
                import saltext.elasticsearch.modules.elasticsearch8_mod
                loaded = True
                saltext.elasticsearch.modules.elasticsearch8_mod.__salt__ = __salt__
            __salt__[f] = eval(f"saltext.elasticsearch.modules.elasticsearch8_mod.{f.split('.')[1]}")
    __salt__["config.option"] = salt.modules.config.option 


try:
    try:
        import elasticsearch8 as elasticsearch
    except ImportError:
        import elasticsearch

    HAS_ELASTICSEARCH = True
    ES_MAJOR_VERSION = elasticsearch.__version__[0]
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)
    logging.getLogger("elastic_transport.transport").setLevel(logging.CRITICAL)
except ImportError:
    HAS_ELASTICSEARCH = False
    ES_MAJOR_VERSION = 0

__virtualname__ = "elasticsearch"

log = logging.getLogger(__name__)

STATE_FUNCTIONS = {
    "state.apply": "state_apply",
    "state.highstate": "state_apply",
    "state.sls": "state_apply",
}


def __virtual__():
    if not HAS_ELASTICSEARCH:
        return (
            False,
            "Cannot load module elasticsearch: elasticsearch librarielastic not found",
        )
    if ES_MAJOR_VERSION < 8:
        return (False, "Cannot load the module, elasticserach version is not 8+")

    return __virtualname__


def _get_options(ret=None):
    """
    Get the returner options from salt.
    """

    defaults = {
        "debug_returner_payload": False,
        "doc_type": "default",
        "functions_blacklist": [],
        "index_date": False,
        "master_event_index": "salt-master-event-cache",
        "master_event_doc_type": "default",
        "master_job_cache_index": "salt-master-job-cache",
        "master_job_cache_doc_type": "default",
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "states_order_output": False,
        "states_count": False,
        "states_single_index": False,
        "failover": False,
        "dev": False,
    }

    attrs = {
        "debug_returner_payload": "debug_returner_payload",
        "doc_type": "doc_type",
        "functions_blacklist": "functions_blacklist",
        "index_date": "index_date",
        "master_event_index": "master_event_index",
        "master_event_doc_type": "master_event_doc_type",
        "master_job_cache_index": "master_job_cache_index",
        "master_job_cache_doc_type": "master_job_cache_doc_type",
        "number_of_shards": "number_of_shards",
        "number_of_replicas": "number_of_replicas",
        "states_count": "states_count",
        "states_order_output": "states_order_output",
        "states_single_index": "states_single_index",
        "failover": "failover",
        "dev": "dev",
    }
    try:
        _options = salt.returners.get_returner_options(
            __virtualname__,
           ret,
           attrs,
           __salt__=__salt__,
           __opts__=__opts__,
           __grains__=grains,
           defaults=defaults,
        )
    except Exception as err:
        log.debug(f"Exception getting options {err}")
        _options = defaults
        try:
            _options.update(__opts__['elasticsearch'])
        except Exception:
            log.debug(f"__opts__['elasticsearch'] not defined, setting options to default'")
 
    if _options["dev"]:
        _options["master_job_cache_index"] = f"dev-{_options['master_job_cache_index']}"

    return _options


def _get_index_name(state, dev=False):
    log.trace(f"running _get_index_name for {state}")
    log.trace(f"get_index_name dev {dev}")
    index = f"salt-{state.replace('.', '_')}"
    if dev:
        index = f"dev-salt-{state.replace('.', '_')}"
    return index


def _ensure_index(index):
    log.debug(f"running _ensure_index for {index}")
    _load_modules_ssh(__salt__)
    index_exists = __salt__["elasticsearch.index_exists"](index=f"{index}-v2")
    if not index_exists:
        options = _get_options()

        index_definition = {
            "number_of_shards": options["number_of_shards"],
            "number_of_replicas": options["number_of_replicas"],
            "index.mapping.ignore_malformed": True
        }
        if 'master' not in index:
            mapping={"properties": RETURN_MAPPING}
        else:
            mapping={"properties": MASTER_MAPPING}

        try:
            __salt__["elasticsearch.index_create"](index=f"{index}-v2", settings=index_definition, mappings=mapping)
        except Exception:
            pass
        try:
            if  __salt__["elasticsearch.alias_exists"](indices=f"{index}-v1", aliases=index):
                __salt__["elasticsearch.alias_delete"](indices=f"{index}-v1", aliases=index)
            __salt__["elasticsearch.alias_create"](indices=f"{index}-v2", alias=index)
        except Exception:
            raise


def _convert_keys(data):
    if isinstance(data, dict):
        new_data = {}
        for k, sub_data in data.items():
            if isinstance(k, (bytes, bytearray)):
                k = k.decode("utf-8", errors="replace")
            if "." in k:
                new_data["_orig_key"] = k
                k = k.replace(".", "_")
            new_data[k] = _convert_keys(sub_data)
    elif isinstance(data, list):
        new_data = []
        for item in data:
            new_data.append(_convert_keys(item))
    elif isinstance(data, (bytes, bytearray)):
        return data.decode("utf-8", errors="replace")
    else:
        return data

    return new_data


def returner(ret):
    log.debug(f"running returner")
    log.trace(f"for {ret}")
    """
    Process the return from Salt
    """

    job_fun = ret["fun"]
    job_fun_args = salt.utils.json.dumps(ret["fun_args"]) if "fun_args" in ret else []
    job_id = ret["jid"]
    job_retcode = ret.get("retcode", 1)
    job_success = bool(not job_retcode)

    options = _get_options(ret)
    _load_modules_ssh(__salt__)

    if job_fun in options["functions_blacklist"]:
        log.info(
            "Won't push new data to Elasticsearch, job with jid=%s and "
            "function=%s which is in the user-defined list of ignored "
            "functions",
            job_id,
            job_fun,
        )
        return
    if ret.get("data", None) is None and ret.get("return") is None:
        log.info(
            "Won't push new data to Elasticsearch, job with jid=%s was not successful",
            job_id,
        )
        return

    # Build the index name
    if options["states_single_index"] and job_fun in STATE_FUNCTIONS:
        index = f"salt-{STATE_FUNCTIONS[job_fun]}"
    else:
        index = _get_index_name(job_fun, options["dev"])
    if options["index_date"]:
        index = "{}-{}".format(index, datetime.date.today().strftime("%Y.%m.%d"))

    counts = {}

    # Do some special processing for state returns
    if job_fun in STATE_FUNCTIONS:
        # Init the state counts
        if options["states_count"]:
            counts = {
                "succeeded": 0,
                "failed": 0,
            }

        # Prepend each state execution key in ret['return'] with a zero-padded
        # version of the '__run_num__' field allowing the states to be ordered
        # more easily. Change the index to be
        # index to be '<index>-ordered' so as not to clash with the unsorted
        # index data format
        if options["states_order_output"] and isinstance(ret["return"], dict):
            index = f"{index}-ordered"
            max_chars = len(str(len(ret["return"])))

            for uid, data in ret["return"].items():
                # Skip keys we've already prefixed
                if uid.startswith(tuple("0123456789")):
                    continue

                # Store the function being called as it's a useful key to search
                decoded_uid = uid.split("_|-")
                ret["return"][uid]["_func"] = f"{decoded_uid[0]}.{decoded_uid[-1]}"

                # Prefix the key with the run order so it can be sorted
                new_uid = "{}_|-{}".format(
                    str(data["__run_num__"]).zfill(max_chars),
                    uid,
                )

                ret["return"][new_uid] = ret["return"].pop(uid)

        # Catch a state output that has failed and where the error message is
        # not in a dict as expected. This prevents elasticsearch from
        # complaining about a mapping error
        elif not isinstance(ret["return"], dict):
            ret["return"] = {"return": ret["return"]}

        # Need to count state successes and failures
        if options["states_count"]:
            for state_data in ret["return"].values():
                if state_data["result"] is False:
                    counts["failed"] += 1
                else:
                    counts["succeeded"] += 1

    # Ensure the index exists
    try:
        _ensure_index(index)
    except Exception as err:
        error_string = str(err)
        if 'resource_already_exists_exception' in error_string or 'invalid_alias_name_exception' in error_string:
            log.debug(f"returner index {index} creation failed because it already exists")
        else:
            raise

    # Build the payload
    data_payload = salt.utils.json.dumps(_convert_keys(ret["return"]))
    utc = UTC()
    data = {
        "@timestamp": datetime.datetime.now(utc).isoformat(),
        "success": job_success,
        "retcode": job_retcode,
        "minion": ret["id"],
        "fun": job_fun,
        "fun_args": job_fun_args,
        "jid": job_id,
        "counts": counts,
        "data": data_payload,
    }

    if options["debug_returner_payload"]:
        log.debug("elasicsearch payload: %s", data)

    # Post the payload
    ret = __salt__["elasticsearch.document_create"](
        index=index, document=salt.utils.json.dumps(data)
    )


def event_return(events):
    log.debug(f"running event_return for {events}")
    """
    Return events to Elasticsearch

    Requires that the `event_return` configuration be set in master config.
    """
    options = _get_options()

    index = options["master_event_index"]

    if options["index_date"]:
        index = "{}-{}".format(index, datetime.date.today().strftime("%Y.%m.%d"))

    _ensure_index(index)

    for event in events:
        data = {"tag": event.get("tag", ""), "data": event.get("data", "")}

    __salt__["elasticsearch.document_create"](
        index=index,
        id_=uuid.uuid4(),
        document=salt.utils.json.dumps(data),
    )


def prep_jid(nocache=False, passed_jid=None):  # pylint: disable=unused-argument
    """
    Do any work necessary to prepare a JID, including sending a custom id
    """
    log.debug(f"running prepare_jid for {passed_jid}")
    return passed_jid if passed_jid is not None else salt.utils.jid.gen_jid(__opts__)



def _make_cache_doc(load, jid):
    log.debug(f"_make_cache_doc starting")
    load_args = None
    for arg in ['arg', 'fun_args', 'fun_arg']:
        if arg in load:
            log.debug(f"prep_jid {arg} found")
            load_args = salt.utils.json.dumps(load[arg])
    doc = {
        "Function": load['fun'],
        "User": load['user'] if 'user' in load else 'salt',
        "Schedule": load['schedule'] if 'schedule' in load else None,
        "Target-type": load['tgt_type'] if 'tgt_type' in load else None,
        "Target":  load['tgt'],
        "jid": jid,
        "Arguments": load_args,
        "StartTime": load['_stamp'],
        "@timestamp": load['_stamp']
    }

    return doc



def _add_minion_to_doc(mid):
    return {
        "script": {
            "source": "if (ctx._source.containsKey('Minions')) { ctx._source.Minions.add(params.minion) } else { ctx._source.Minions = [params.minion] }",
            "lang": "painless",
            "params": {
                "minion": mid
            }
        },
        "upsert": {
            "Minion": [mid]
        }
    }


# pylint: disable=unused-argument
def save_load(jid, load, minions=None):
    """
    Save the load to the specified jid id

    .. versionadded:: 2015.8.1
    """
    log.debug(f"save_load starting for {jid}")
    log.trace(f"save_load {load}")
    options = _get_options()
    add_to_cache = False

    index = options["master_job_cache_index"]
    retries = options["retry_on_conflicts"] if "retry_on_conflicts" in options else 5
    _ensure_index(index)
    job_in_cache = __salt__["elasticsearch.document_exists"](index=index, id_=jid)

    utc = UTC()
    if "_stamp" not in load:
        load["_stamp"] = datetime.datetime.now(utc).isoformat()

    if 'cmd' in load:
        if load['cmd'] == 'publish':
            add_to_cache = True
        elif load['cmd'] == "_return":
            mid = load['id']
            if job_in_cache:
                log.debug(f"save_load adding {mid} for {load['cmd']}")
                try:
                    __salt__["elasticsearch.document_update"](index=index, id_=jid, body=_add_minion_to_doc(mid), retry_on_conflict=retries)
                except Exception as e:
                    log.error(f"non-fatal error {e} adding {mid} to {load['cmd']} for {jid}")
            else:
                add_to_cache = True
        else:
            log.debug(f"save_load cmd in load but it is neither _return nor publish {jid}: {cmd}")
            log.trace(f"save_load load {load}")
    else:
        log.debug(f"save_load cmd not in load, attempting to load job to cache")
        if 'jid' in load:
            jid = load['jid']
            if not job_in_cache:
                add_to_cache = True
            else:
                log.debug(f"save_load skipping {jid} for {index} because it already exists")

        else:
            log.debug(f"save_load no jid to upload")
            log.trace(f"save_load load {load}")

    if add_to_cache:
        doc = _make_cache_doc(load, jid)
        log.debug(f"save_load uploading doc to index {index}")
        log.trace(f"save_load doc {doc}")
        __salt__["elasticsearch.document_create"](
            index=index, id_=jid, document=salt.utils.json.dumps(doc)
        )



def get_load(jid):
    """
    Return the load data that marks a specified jid

    .. versionadded:: 2015.8.1
    """
    log.debug(f"get_load running for {jid}")
    options = _get_options()

    index = options["master_job_cache_index"]

    data = __salt__["elasticsearch.document_get"](index=index, id_=jid)
    log.debug(f"get_load found results")
    log.trace(f"get_load data {data}")
    if isinstance(data, str):
        return salt.utils.json.loads(data)
    elif isinstance(data, dict):
        log.debug(f"get_load mapping data")
        data["_source"]["tgt"] = data['_source']["Target"]
        data["_source"]["tgt_type"] = data['_source']["Target-type"]
        data["_source"]["fun"] = data['_source']["Function"]
        data["_source"]["user"] = data['_source']["User"]
        data["_source"]["arg"] = salt.utils.json.loads(data['_source']["Arguments"])
        log.trace(f"data {data}")
        return data["_source"]
    else:
        log.debug(f"get_load got data that is neither a string nor a dict")
    return {}

def get_jid(jid):
    """
    Return the load data that marks a specified jid

    .. versionadded:: 2015.8.1
    """
    log.debug(f"get_jid starting for {jid}")
    options = _get_options()
    ret = {}
    index = options["master_job_cache_index"]
    data = __salt__["elasticsearch.document_get"](index=index, id_=jid, source_excludes=["_index", "_primary_term", "version", "found", "_seq_no"])
    if not data:
        raise CommandExecutionError(f"Job {jid} does not exist")
    log.debug(f"get_jid fetched data")
    log.trace(f"get_jid data {data}")
    fun = data['_source']['Function']
    job_index =  _get_index_name(fun, options["dev"])

    query = JOB_QUERY.replace("JID", jid)
    log.debug(f"get_jid index: {job_index} built job query") 
    log.trace(f"get_jid query {query}")

    try:
        job_data = __salt__["elasticsearch.search"](index=job_index, body=query, size=10000, source_excludes=[ "@timestamp", "counts", "fun", "jid", "fun_args", "fun_kwargs"])
        log.debug(f"get_jid fetched job data")
        log.trace(f"get_jid job data: {job_data}")
        if isinstance(data, str):
            return salt.utils.json.loads(data)
        elif isinstance(data, dict):
            log.debug(f"get_jid fetched {len(job_data['hits']['hits'])} results")
            for j in job_data['hits']['hits']:
                mid = j['_source'].pop('minion')
                ret[mid] = j['_source']
                try:
                    ret[mid] = salt.utils.json.loads(ret[mid]['data'])
                except:
                    log.debug(f"get_jid error deserializing data")
                    log.trace(f"get_jid data {ret[mid]['data']}")
    except Exception as err:
        log.debug(f"get_jid error running search {err}")
       
    return ret

def get_jids():
    """
    Return the load data that marks a specified jid

    .. versionadded:: 2015.8.1
    """
    log.debug(f"running get_jids")
    options = _get_options()
    jobs = {}
    index = options["master_job_cache_index"]

    data = __salt__["elasticsearch.document_get_all"](index=index, source_excludes=["_index", "_score", "max_score", "total", "hits._index", "@timestamp"])
    if isinstance(data, str):
        log.debug(f"get_jids found string data")
        data =  salt.utils.json.loads(data)
    for job in data['hits']:
       try:
           jid = job['_source'].pop('jid')
           if 'Minions' in job['_source']:
               del job['_source']['Minions']
           if 'Arguments' in job['_source']:
               job["_source"]["Arguments"] = salt.utils.json.loads(job["_source"]["Arguments"])
           if 'KeywordArgs' in job['_source']:
               job["_source"]["KeywordArgs"] = salt.utils.json.loads(job["_source"]["KeywordArgs"])
           jobs[jid] = job["_source"]
       except Exception as err:
          log.debug(f"failed to fetch job data: {job} error: {err}")

    return jobs

def get_fun(fun):
    log.debug(f"running get_fun for {fun}")
    options = _get_options()
    index =  _get_index_name(fun, options["dev"])
    query = FUNCTION_QUERY
    data = __salt__["elasticsearch.search"](index=index, body=query, size=10000)
    return data.body['hits']['hits']

def get_minions():
    log.debug("running get_minions")
    options = _get_options()
    index =  _get_index_name(fun, options["dev"])
    query = MINION_QUERY
    data = __salt__["elasticsearch.search"](index=index, body=query, size=10000)
    return [m['key'] for m in  data.body['aggregations']['unique_field_values']['buckets']]

