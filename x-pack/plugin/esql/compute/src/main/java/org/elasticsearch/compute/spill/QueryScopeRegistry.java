/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.function.Consumer;

/**
 * The scopes of the queries running on this node, by query identity. Acquisition is atomic per key and returns a lease
 * bound to the exact entry it found or created: a scope that has started closing is never handed out, the entry is
 * replaced by a fresh scope instead, and the closing one finishes outside the map.
 */
public final class QueryScopeRegistry {

    /**
     * Creates the scope for a query, or returns null when spilling is off for it, in which case the query runs on the
     * pre-feature paths with the node's block factory and no scope at all.
     */
    @FunctionalInterface
    public interface ScopeFactory {
        @Nullable
        QueryScope create(String queryId, Consumer<QueryScope> onClosing);
    }

    private final Map<String, QueryScope> scopes = ConcurrentCollections.newConcurrentMap();
    private final ScopeFactory factory;

    public QueryScopeRegistry(ScopeFactory factory) {
        this.factory = factory;
    }

    /**
     * A lease on the query's scope, creating the scope if there is none, or null when spilling is off for the query. A
     * null is not remembered: the next acquire for the query asks the factory again, which reads the settings of that
     * moment.
     */
    @Nullable
    public QueryScope.Lease acquire(String queryId) {
        while (true) {
            QueryScope existing = scopes.get(queryId);
            if (existing != null) {
                QueryScope.Lease lease = existing.tryAcquire();
                if (lease != null) {
                    return lease;
                }
                // closing: drop it if it is still the entry, then start over
                scopes.remove(queryId, existing);
                continue;
            }
            QueryScope created = factory.create(queryId, scope -> scopes.remove(queryId, scope));
            if (created == null) {
                return null;
            }
            QueryScope.Lease lease = created.tryAcquire();
            assert lease != null : "a fresh scope hands out its first lease";
            if (scopes.putIfAbsent(queryId, created) == null) {
                return lease;
            }
            // lost the race: release the only lease, which closes the unused scope, and retry on the winner
            lease.close();
        }
    }

    /**
     * A lease on the query's scope only if it exists and is not closing, for a holder that must not create one: an
     * observer such as a test or a stats endpoint. The exchange layer and the computes acquire instead, since the
     * first of them to arrive on a node is the one that creates the scope.
     */
    @Nullable
    public QueryScope.Lease resolve(String queryId) {
        QueryScope existing = scopes.get(queryId);
        return existing == null ? null : existing.tryAcquire();
    }

    public int size() {
        return scopes.size();
    }
}
