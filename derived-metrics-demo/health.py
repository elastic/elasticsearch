#!/usr/bin/env python3
"""Renders the derived metrics breaker and threadpool from a _nodes/stats response on stdin.

These are what the feature costs the node, as opposed to what it produces. Both are bounded
deliberately — the breaker at a fraction of the heap, the pool with a queue that sheds rather than
grows — so a non-zero 'tripped' or 'rejected' is the feature doing its job, not a fault.
"""

import json
import sys


def main():
    nodes = json.load(sys.stdin).get("nodes", {})
    if not nodes:
        print("    no derived metrics stats reported")
        return
    for name, node in nodes.items():
        breaker = node.get("breakers", {}).get("derived_metrics", {})
        pool = node.get("thread_pool", {}).get("derived_metrics", {})
        used = breaker.get("estimated_size", "-")
        limit = breaker.get("limit_size", "-")
        tripped = breaker.get("tripped", 0)
        print(f"    breaker      {used} of {limit}, tripped {tripped}")
        print(
            "    thread pool  "
            f"active {pool.get('active', 0)}  "
            f"queue {pool.get('queue', 0)}  "
            f"completed {pool.get('completed', 0)}  "
            f"rejected {pool.get('rejected', 0)}"
        )


if __name__ == "__main__":
    main()
