/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.compute.spill.QueryScopeRegistry;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

/**
 * The node's spill figures for the ES|QL stats endpoint: what the store holds and may hold, what it could not delete,
 * and how many queries have a scope open on this node. Observational only, read when asked; nothing admits or refuses
 * a spill on their account, the store's own counters do that as a write is admitted.
 */
public final class SpillStats {
    /** The counter names, under {@code spill} in the endpoint's nested map. */
    public static final String BYTES_IN_USE = "spill.bytes_in_use";
    public static final String BYTES_LIMIT = "spill.bytes_limit";
    public static final String CLEANUP_DEBT_BYTES = "spill.cleanup_debt_bytes";
    public static final String SCOPES = "spill.scopes";

    @Nullable
    private final SpillStore store;
    private final QueryScopeRegistry scopes;

    /**
     * @param store the node's store, or null when none is installed or the feature is off: no figure is reported then
     * @param scopes the registry of the node's query scopes
     */
    public SpillStats(@Nullable SpillStore store, QueryScopeRegistry scopes) {
        this.store = store;
        this.scopes = scopes;
    }

    /**
     * Adds the figures to the node's counters; without a store nothing is added, so a stats consumer sees no
     * {@code spill} entry where spilling cannot happen.
     */
    public void populate(Counters counters) {
        if (store == null) {
            return;
        }
        counters.inc(BYTES_IN_USE, store.bytesInUse());
        counters.inc(BYTES_LIMIT, store.bytesLimit());
        counters.inc(CLEANUP_DEBT_BYTES, store.cleanupDebtBytes());
        counters.inc(SCOPES, scopes.size());
    }
}
