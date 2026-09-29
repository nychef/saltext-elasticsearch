"""
Test the elasticsearch master returner functions (master job cache).

These cover ``save_load``, ``get_load``, ``get_jid``, ``get_jids``,
``get_fun``, ``get_minions``, ``_make_cache_doc`` and ``_add_minion_to_doc``
from the custom returner.
"""

import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import salt.utils.json

import saltext.elasticsearch.returners.elasticsearch8_mod as elasticsearch_return


@pytest.fixture
def configure_loader_modules():
    return {elasticsearch_return: {}}


@pytest.fixture
def default_options():
    """Options returned by _get_options(), including the ones the master
    job-cache functions read."""
    return {
        "debug_returner_payload": False,
        "doc_type": "default",
        "functions_blacklist": [],
        "index_date": False,
        "dev": False,
        "failover": False,
        "master_event_index": "salt-master-event-cache",
        "master_event_doc_type": "default",
        "master_job_cache_index": "salt-master-job-cache",
        "master_job_cache_doc_type": "default",
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "states_order_output": False,
        "states_count": False,
        "states_single_index": False,
    }


@pytest.fixture
def mock_salt():
    """Dict of mocked __salt__ functions used by the master returner."""
    return {
        "elasticsearch.index_exists": MagicMock(return_value=True),
        "elasticsearch.index_create": MagicMock(return_value=True),
        "elasticsearch.alias_create": MagicMock(return_value=True),
        "elasticsearch.alias_exists": MagicMock(return_value=False),
        "elasticsearch.alias_delete": MagicMock(return_value=True),
        "elasticsearch.document_create": MagicMock(return_value=True),
        "elasticsearch.document_exists": MagicMock(return_value=False),
        "elasticsearch.document_get": MagicMock(return_value=None),
        "elasticsearch.document_get_all": MagicMock(return_value={"hits": []}),
        "elasticsearch.document_update": MagicMock(return_value=True),
        "elasticsearch.search": MagicMock(return_value={"hits": {"hits": []}}),
        "config.option": MagicMock(return_value={}),
    }


@pytest.fixture
def patched_options(default_options):
    """Patch salt.returners.get_returner_options to return the fixture options."""
    with patch("salt.returners.get_returner_options", return_value=default_options):
        yield default_options


@pytest.fixture
def load():
    """A typical publish load as sent by the master."""
    return {
        "cmd": "publish",
        "fun": "test.ping",
        "jid": "20260924120000000000",
        "id": "minion1",
        "user": "norbert",
        "tgt": "*",
        "tgt_type": "glob",
        "arg": ["timeout=5"],
        "_stamp": "2026-09-24T12:00:00Z",
    }


# ---------------------------------------------------------------------------
# _make_cache_doc()
# ---------------------------------------------------------------------------


def test__make_cache_doc_full(load, patched_options):
    """
    The cache doc carries the function, user, target, jid, JSON-encoded
    arguments and both timestamps.
    """
    doc = elasticsearch_return._make_cache_doc(load, load["jid"])
    assert doc["Function"] == "test.ping"
    assert doc["User"] == "norbert"
    assert doc["Schedule"] is None
    assert doc["Target-type"] == "glob"
    assert doc["Target"] == "*"
    assert doc["jid"] == "20260924120000000000"
    assert salt.utils.json.loads(doc["Arguments"]) == ["timeout=5"]
    assert doc["StartTime"] == "2026-09-24T12:00:00Z"
    assert doc["@timestamp"] == "2026-09-24T12:00:00Z"


def test__make_cache_doc_minimal(patched_options):
    """
    Loads without user/schedule/tgt_type fall back to the documented defaults.
    """
    load = {"fun": "test.ping", "tgt": "minion1", "_stamp": "2026-09-24T12:00:00Z"}
    doc = elasticsearch_return._make_cache_doc(load, "20260924120000000001")
    assert doc["User"] == "salt"
    assert doc["Schedule"] is None
    assert doc["Target-type"] is None
    assert doc["Target"] == "minion1"


def test__make_cache_doc_prefers_fun_args(patched_options):
    """
    fun_args wins over arg when both are present in the load.
    """
    load = {"fun": "test.ping", "tgt": "*", "arg": ["a"], "fun_args": ["b"],
            "_stamp": "2026-09-24T12:00:00Z"}
    doc = elasticsearch_return._make_cache_doc(load, "20260924120000000002")
    assert salt.utils.json.loads(doc["Arguments"]) == ["b"]


# ---------------------------------------------------------------------------
# _add_minion_to_doc()
# ---------------------------------------------------------------------------


def test__add_minion_to_doc():
    """
    The upsert doc appends the minion id via a painless script.
    """
    doc = elasticsearch_return._add_minion_to_doc("minion1")
    assert doc["script"]["lang"] == "painless"
    assert doc["script"]["params"]["minion"] == "minion1"
    assert doc["upsert"] == {"Minion": ["minion1"]}
    assert "ctx._source.Minions" in doc["script"]["source"]


# ---------------------------------------------------------------------------
# save_load()
# ---------------------------------------------------------------------------


def test_save_load_publish_creates_cache_doc(mock_salt, patched_options, load):
    """
    A publish load is written to the master job cache index with id_=jid.
    """
    elasticsearch_return.save_load(load["jid"], load)
    mock_salt["elasticsearch.document_create"].assert_called_once()
    kwargs = mock_salt["elasticsearch.document_create"].call_args[1]
    assert kwargs["index"] == "salt-master-job-cache"
    assert kwargs["id_"] == "20260924120000000000"
    doc = salt.utils.json.loads(kwargs["document"])
    assert doc["Function"] == "test.ping"
    assert doc["Target"] == "*"


def test_save_load_return_updates_minions(mock_salt, patched_options, load):
    """
    A _return load for an existing cache doc updates it with the minion id.
    """
    ret_load = dict(load)
    ret_load["cmd"] = "_return"
    mock_salt["elasticsearch.document_exists"].return_value = True

    elasticsearch_return.save_load(ret_load["jid"], ret_load)
    mock_salt["elasticsearch.document_update"].assert_called_once()
    kwargs = mock_salt["elasticsearch.document_update"].call_args[1]
    assert kwargs["index"] == "salt-master-job-cache"
    assert kwargs["id_"] == "20260924120000000000"
    assert kwargs["body"]["script"]["params"]["minion"] == "minion1"
    mock_salt["elasticsearch.document_create"].assert_not_called()


def test_save_load_return_missing_creates_cache_doc(mock_salt, patched_options, load):
    """
    A _return load for a missing cache doc falls back to creating it.
    """
    ret_load = dict(load)
    ret_load["cmd"] = "_return"
    mock_salt["elasticsearch.document_exists"].return_value = False

    elasticsearch_return.save_load(ret_load["jid"], ret_load)
    mock_salt["elasticsearch.document_create"].assert_called_once()
    mock_salt["elasticsearch.document_update"].assert_not_called()


def test_save_load_other_cmd_is_noop(mock_salt, patched_options, load):
    """
    Loads with an unknown cmd are logged and skipped without ES calls.
    """
    other = dict(load)
    other["cmd"] = "sync"
    elasticsearch_return.save_load(other["jid"], other)
    mock_salt["elasticsearch.document_create"].assert_not_called()
    mock_salt["elasticsearch.document_update"].assert_not_called()


def test_save_load_no_cmd_new_jid(mock_salt, patched_options, load):
    """
    Loads without cmd and a fresh jid are added to the cache.
    """
    plain = {"fun": "test.ping", "jid": load["jid"], "tgt": "*", "tgt_type": "glob",
             "_stamp": "2026-09-24T12:00:00Z"}
    elasticsearch_return.save_load(plain["jid"], plain)
    mock_salt["elasticsearch.document_create"].assert_called_once()


def test_save_load_no_cmd_existing_jid(mock_salt, patched_options, load):
    """
    Loads without cmd whose jid is already cached are skipped.
    """
    plain = {"fun": "test.ping", "jid": load["jid"], "tgt": "*", "tgt_type": "glob",
             "_stamp": "2026-09-24T12:00:00Z"}
    mock_salt["elasticsearch.document_exists"].return_value = True
    elasticsearch_return.save_load(plain["jid"], plain)
    mock_salt["elasticsearch.document_create"].assert_not_called()


def test_save_load_no_jid_noop(mock_salt, patched_options):
    """
    Loads without cmd and without a jid are skipped entirely.
    """
    elasticsearch_return.save_load(None, {"fun": "test.ping", "_stamp": "x"})
    mock_salt["elasticsearch.document_create"].assert_not_called()


# ---------------------------------------------------------------------------
# get_load()
# ---------------------------------------------------------------------------


def test_get_load_string_response(mock_salt, patched_options, load):
    """
    A string response from document_get is parsed as JSON. The string path
    returns the cache doc as-is (no field-name mapping).
    """
    cache_doc = elasticsearch_return._make_cache_doc(load, load["jid"])
    mock_salt["elasticsearch.document_get"].return_value = salt.utils.json.dumps(cache_doc)

    ret = elasticsearch_return.get_load(load["jid"])
    assert ret["Function"] == "test.ping"
    assert ret["jid"] == "20260924120000000000"


def test_get_load_dict_response(mock_salt, patched_options, load):
    """
    A dict response from document_get is unmapped from the cache field names.
    """
    cache_doc = elasticsearch_return._make_cache_doc(load, load["jid"])
    mock_salt["elasticsearch.document_get"].return_value = {"_source": dict(cache_doc)}

    ret = elasticsearch_return.get_load(load["jid"])
    assert ret["fun"] == "test.ping"
    assert ret["tgt"] == "*"
    assert ret["tgt_type"] == "glob"
    assert ret["user"] == "norbert"
    assert ret["arg"] == ["timeout=5"]


def test_get_load_missing(mock_salt, patched_options, load):
    """
    A missing document returns an empty dict.
    """
    mock_salt["elasticsearch.document_get"].return_value = None
    assert elasticsearch_return.get_load("missing-jid") == {}


# ---------------------------------------------------------------------------
# get_jid()
# ---------------------------------------------------------------------------


def _cache_doc_stub(load):
    return {
        "_source": elasticsearch_return._make_cache_doc(load, load["jid"]),
    }


def test_get_jid_returns_minion_returns(mock_salt, patched_options, load):
    """
    get_jid reads the cache doc for the function, searches the job index and
    returns {minion: return}, deserializing the data field.
    """
    mock_salt["elasticsearch.document_get"].return_value = _cache_doc_stub(load)
    mock_salt["elasticsearch.search"].return_value = {
        "hits": {"hits": [
            {"_source": {"minion": "minion1", "data": salt.utils.json.dumps({"return": True})}},
            {"_source": {"minion": "minion2", "data": salt.utils.json.dumps({"return": "ret"})}},
        ]}}

    ret = elasticsearch_return.get_jid(load["jid"])
    assert ret == {"minion1": {"return": True}, "minion2": {"return": "ret"}}
    # the job index derives from the cached function name
    search_kwargs = mock_salt["elasticsearch.search"].call_args[1]
    assert search_kwargs["index"] == "salt-test_ping"


def test_get_jid_missing_raises(mock_salt, patched_options, load):
    """
    A jid missing from the master cache raises CommandExecutionError.
    """
    from salt.exceptions import CommandExecutionError

    mock_salt["elasticsearch.document_get"].return_value = None
    with pytest.raises(CommandExecutionError):
        elasticsearch_return.get_jid("missing-jid")


def test_get_jid_search_failure_returns_empty(mock_salt, patched_options, load):
    """
    A search failure is tolerated and yields an empty dict.
    """
    mock_salt["elasticsearch.document_get"].return_value = _cache_doc_stub(load)
    mock_salt["elasticsearch.search"].side_effect = Exception("es down")
    assert elasticsearch_return.get_jid(load["jid"]) == {}


# ---------------------------------------------------------------------------
# get_jids()
# ---------------------------------------------------------------------------


def test_get_jids_lists_cached_jobs(mock_salt, patched_options, load):
    """
    get_jids returns {jid: source} with Minions stripped and Arguments decoded.
    """
    cache_doc = elasticsearch_return._make_cache_doc(load, load["jid"])
    cache_doc["Minions"] = ["minion1"]
    mock_salt["elasticsearch.document_get_all"].return_value = {
        "hits": [{"_source": cache_doc}]}

    jobs = elasticsearch_return.get_jids()
    assert list(jobs) == ["20260924120000000000"]
    assert jobs["20260924120000000000"]["Function"] == "test.ping"
    assert "Minions" not in jobs["20260924120000000000"]
    assert jobs["20260924120000000000"]["Arguments"] == ["timeout=5"]


def test_get_jids_empty(mock_salt, patched_options):
    """
    An empty cache index yields no jobs.
    """
    assert elasticsearch_return.get_jids() == {}


# ---------------------------------------------------------------------------
# get_fun() / get_minions()
# ---------------------------------------------------------------------------


def test_get_fun_returns_hits(mock_salt, patched_options):
    """
    get_fun searches the salt-<fun> index and returns the raw hits.
    """
    hits = [{"_id": "a", "_source": {}}, {"_id": "b", "_source": {}}]
    mock_salt["elasticsearch.search"].return_value = {"hits": {"hits": hits}}

    ret = elasticsearch_return.get_fun("test.ping")
    assert ret == hits
    kwargs = mock_salt["elasticsearch.search"].call_args[1]
    assert kwargs["index"] == "salt-test_ping"


def test_get_minions_returns_keys(mock_salt, patched_options):
    """
    get_minions extracts the aggregation bucket keys.
    """
    mock_salt["elasticsearch.search"].return_value = {
        "aggregations": {"unique_field_values": {"buckets": [
            {"key": "minion1", "doc_count": 3},
            {"key": "minion2", "doc_count": 1},
        ]}}}

    ret = elasticsearch_return.get_minions()
    assert ret == ["minion1", "minion2"]
    kwargs = mock_salt["elasticsearch.search"].call_args[1]
    assert kwargs["index"] == "salt-master-job-cache"
