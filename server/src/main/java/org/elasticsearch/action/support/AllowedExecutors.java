/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Map;
import java.util.Set;

/**
 * A digraph of the executors that are allowed to block while waiting for a future to be completed on a different threadpool. To avoid
 * deadlocks there must be no cycles in this digraph.
 */
class AllowedExecutors {
    static class AllowedExecutorsEntry implements Map.Entry<String, Set<String>> {
        private final String key;
        private final Set<String> value;

        AllowedExecutorsEntry(String key, Set<String> value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Set<String> getValue() {
            return value;
        }

        @Override
        public Set<String> setValue(Set<String> value) {
            assert false : "unexpected";
            return null;
        }
    }

    /**
     * @return a digraph of the executors which are permitted to block each other in production.
     */
    static Map<String, Set<String>> getProductionAllowedExecutors() {
        return getAllowedExecutors(
            newEntry("ccr" /* does not block */),
            newEntry("clusterApplierService#updateTask" /* does not block */),
            newEntry("get" /* does not block */),
            newEntry("masterService#updateTask" /* does not block */),
            newEntry("searchable_snapshots_cache_fetch_async" /* does not block */),
            newEntry("system_read" /* does not block */),
            newEntry("system_write" /* does not block */),
            newEntry("transport_worker" /* does not block */),
            newEntry("write" /* does not block */),
            newEntry(
                "file-watcher",
                /* blocks on futures completed by: */
                "masterService#updateTask"
            ),
            newEntry(
                "management",
                /* blocks on futures completed by: */
                "system_read"
            ),
            newEntry(
                "cluster_coordination",
                /* blocks on futures completed by: */
                "transport_worker"
            ),
            newEntry(
                "watcher",
                /* blocks on futures completed by: */
                "get",
                "system_read"
            ),
            newEntry(
                "watcher-lifecycle",
                /* blocks on futures completed by: */
                "system_read",
                "get"
            ),
            newEntry(
                "search",
                /* blocks on futures completed by: */
                "searchable_snapshots_cache_fetch_async"
            ),
            newEntry(
                "search_worker",
                /* blocks on futures completed by: */
                "searchable_snapshots_cache_fetch_async"
            ),
            newEntry(
                "searchable_snapshots_cache_prewarming",
                /* blocks on futures completed by: */
                "searchable_snapshots_cache_fetch_async"
            ),
            newEntry(
                "ml_job_comms",
                /* blocks on futures completed by: */
                "clusterApplierService#updateTask",
                "masterService#updateTask",
                "search",
                "transport_worker",
                "write"
            ),
            newEntry(
                "ml_utility",
                /* blocks on futures completed by: */
                "clusterApplierService#updateTask",
                "management",
                "masterService#updateTask",
                "system_read",
                "system_write",
                "transport_worker",
                "write"
            ),
            newEntry(
                "snapshot_meta",
                /* blocks on futures completed by: */
                "ccr"
            ),
            newEntry(
                "generic",
                /* blocks on futures completed by: */
                "ccr",
                "management",
                "masterService#updateTask",
                "searchable_snapshots_cache_fetch_async",
                "system_read",
                "transport_worker"
            ),
            newEntry(
                "snapshot",
                /* blocks on futures completed by: */
                "generic",
                "searchable_snapshots_cache_fetch_async",
                "system_read",
                "transport_worker"
            )
        );
    }

    /**
     * @return a digraph of the executors which are permitted to block each other according to the given entries.
     * @throws IllegalStateException if the input entries are not topologically sorted.
     */
    static Map<String, Set<String>> getAllowedExecutors(AllowedExecutorsEntry... entries) {
        final var keys = Sets.newHashSetWithExpectedSize(entries.length);
        for (final var entry : entries) {
            if (keys.contains(entry.key)) {
                throw new IllegalStateException(Strings.format("duplicate key %s", entry.getKey()));
            }
            if (keys.containsAll(entry.getValue()) == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "input is not topologically sorted: threadpool [%s] may only block on a subset of %s, saw %s",
                        entry.getKey(),
                        keys,
                        entry.getValue()
                    )
                );
            }
            keys.add(entry.getKey());
        }
        return Map.ofEntries(entries);
    }

    static AllowedExecutorsEntry newEntry(String waitingThread, String... completingThreads) {
        return new AllowedExecutorsEntry(waitingThread, Set.of(completingThreads));
    }
}
