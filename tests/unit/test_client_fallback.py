"""
Test the elasticsearch8/elasticsearch client import fallback in the ES8
execution module and returner.

The ES8 files try the pinned ``elasticsearch8`` package first and fall back to
the standard ``elasticsearch`` package when it is not installed. These tests
exercise both paths (and the refusal path) by loading fresh module instances
with the client packages stubbed in sys.modules.
"""

import importlib.util
import sys
import types

import pytest


@pytest.fixture
def es8_paths():
    """File paths of the ES8 execution module and returner."""
    paths = {}
    for kind, module_name in (
        ("module", "saltext.elasticsearch.modules.elasticsearch8_mod"),
        ("returner", "saltext.elasticsearch.returners.elasticsearch8_mod"),
    ):
        spec = importlib.util.find_spec(module_name)
        paths[kind] = spec.origin
    return paths


def _load_fresh(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _client_stub(version):
    stub = types.ModuleType("elasticsearch")
    stub.__version__ = version
    return stub


@pytest.mark.parametrize("target", ["module", "returner"])
def test_uses_elasticsearch8_when_installed(es8_paths, target, monkeypatch):
    """
    When the elasticsearch8 package is installed it is used directly and the
    elasticsearch fallback is not taken.
    """
    monkeypatch.setitem(sys.modules, "elastic_transport", types.ModuleType("elastic_transport"))
    monkeypatch.setattr(sys.modules["elastic_transport"], "RequestsHttpNode", object, raising=False)
    stub = _client_stub((8, 14, 0))
    monkeypatch.setitem(sys.modules, "elasticsearch8", stub)
    monkeypatch.setitem(sys.modules, "elasticsearch", None)

    mod = _load_fresh(es8_paths[target], f"es8_fallback_{target}_preferred")
    assert mod.HAS_ELASTICSEARCH is True
    assert mod.ES_MAJOR_VERSION == 8
    assert mod.elasticsearch is stub
    assert mod.__virtual__() == "elasticsearch"


@pytest.mark.parametrize("target", ["module", "returner"])
def test_falls_back_to_elasticsearch_8(es8_paths, target, monkeypatch):
    """
    When elasticsearch8 is not installed the standard elasticsearch package
    (8.x) is used instead.
    """
    monkeypatch.setitem(sys.modules, "elastic_transport", types.ModuleType("elastic_transport"))
    monkeypatch.setattr(sys.modules["elastic_transport"], "RequestsHttpNode", object, raising=False)
    stub = _client_stub((8, 14, 0))
    monkeypatch.setitem(sys.modules, "elasticsearch8", None)
    monkeypatch.setitem(sys.modules, "elasticsearch", stub)

    mod = _load_fresh(es8_paths[target], f"es8_fallback_{target}_es8")
    assert mod.HAS_ELASTICSEARCH is True
    assert mod.ES_MAJOR_VERSION == 8
    assert mod.elasticsearch is stub
    assert mod.__virtual__() == "elasticsearch"


@pytest.mark.parametrize("target", ["module", "returner"])
def test_falls_back_to_elasticsearch_9(es8_paths, target, monkeypatch):
    """
    The fallback also works with a standard elasticsearch 9.x client; the
    ES8 files load under the >= 8 version gate.
    """
    monkeypatch.setitem(sys.modules, "elastic_transport", types.ModuleType("elastic_transport"))
    monkeypatch.setattr(sys.modules["elastic_transport"], "RequestsHttpNode", object, raising=False)
    stub = _client_stub((9, 0, 0))
    monkeypatch.setitem(sys.modules, "elasticsearch8", None)
    monkeypatch.setitem(sys.modules, "elasticsearch", stub)

    mod = _load_fresh(es8_paths[target], f"es8_fallback_{target}_es9")
    assert mod.HAS_ELASTICSEARCH is True
    assert mod.ES_MAJOR_VERSION == 9
    assert mod.elasticsearch is stub
    assert mod.__virtual__() == "elasticsearch"


@pytest.mark.parametrize("target", ["module", "returner"])
def test_refuses_when_neither_client_installed(es8_paths, target, monkeypatch):
    """
    When neither client package is installed the module reports
    HAS_ELASTICSEARCH False and __virtual__ refuses to load.
    """
    monkeypatch.setitem(sys.modules, "elastic_transport", types.ModuleType("elastic_transport"))
    monkeypatch.setattr(sys.modules["elastic_transport"], "RequestsHttpNode", object, raising=False)
    monkeypatch.setitem(sys.modules, "elasticsearch8", None)
    monkeypatch.setitem(sys.modules, "elasticsearch", None)

    mod = _load_fresh(es8_paths[target], f"es8_fallback_{target}_none")
    assert mod.HAS_ELASTICSEARCH is False
    assert mod.ES_MAJOR_VERSION == 0
    ret = mod.__virtual__()
    assert ret[0] is False
