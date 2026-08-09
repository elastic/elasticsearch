/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-node count of the search requests this coordinator currently has outstanding, covering both
 * requests handed to the transport layer and the probe slots {@link ArsReservations} claims at
 * routing-decision time.
 * <p>
 * The counts feed adaptive replica selection: the C3 formula uses them to compensate for work
 * already sent to a peer, the probe cap uses them to bound how much traffic reaches a node that has
 * no stats yet, and they are reported as {@code adaptive_selection.outgoing_searches} in node stats.
 * <p>
 * Entries are removed once they reach zero so that ids of nodes that have gone away are not
 * retained forever.
 */
final class PendingSearchRequests {

    private final Map<String, Long> counts = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    void increment(String nodeId) {
        counts.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
    }

    void decrement(String nodeId) {
        assert assertCountValid(nodeId);
        counts.computeIfPresent(nodeId, (id, conns) -> conns == 1 ? null : conns - 1);
    }

    /**
     * A point-in-time copy of the counts. Callers that need a view which stays stable across a
     * series of decisions, such as the per-search snapshot used to spread the shards of a single
     * search, must use this rather than {@link #liveView()}.
     */
    Map<String, Long> snapshot() {
        return new HashMap<>(counts);
    }

    /**
     * A read-only live view of the counts, reflecting concurrent activity as it happens. Used by the
     * ARS probe cap, which has to observe what other searches are doing right now.
     */
    Map<String, Long> liveView() {
        return Collections.unmodifiableMap(counts);
    }

    private boolean assertCountValid(String nodeId) {
        var conns = counts.get(nodeId);
        // null is possible if a concurrent decrement already removed the entry
        assert conns == null || conns >= 1 : "number of connections for " + nodeId + " should be >= 1 but was " + conns;
        // Always return true, there is additional asserting here, the boolean is just so this
        // can be skipped when assertions are not enabled
        return true;
    }
}
