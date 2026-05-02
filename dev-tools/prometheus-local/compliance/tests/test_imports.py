#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

"""Catch import-time regressions: cycles, missing exports, broken aliases.
Cheap to run, hard to fake by mistake."""


def test_every_module_imports():
    import comparator  # noqa: F401
    import config  # noqa: F401
    import corpus  # noqa: F401
    import main  # noqa: F401
    import proc  # noqa: F401
    import tsgen  # noqa: F401
    import util  # noqa: F401


def test_default_config_wires_concrete_procs_to_their_roles():
    # The Config.__post_init__ wiring is the single swap point for backends:
    # if a future refactor accidentally points control at ESProc or vice
    # versa, this fails.
    from config import Config
    from proc import ESProc, PrometheusProc

    cfg = Config.default()
    assert isinstance(cfg.control_proc, PrometheusProc)
    assert isinstance(cfg.test_proc, ESProc)


def test_concrete_procs_satisfy_protocol_shape():
    # Structural check - the Proc protocol declares these methods, so any
    # impl that passes a `Proc` annotation must have all of them. If a future
    # refactor accidentally renames one (as has happened before), this fails.
    from proc import ESProc, PrometheusProc

    for impl in (PrometheusProc, ESProc):
        for name in (
            "setup",
            "teardown",
            "check_health",
            "write",
            "query_range",
            "__aenter__",
            "__aexit__",
        ):
            assert callable(getattr(impl, name)), f"{impl.__name__} missing {name}"
