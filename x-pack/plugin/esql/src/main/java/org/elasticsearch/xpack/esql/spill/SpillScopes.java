/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.QueryScopeRegistry;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.Executor;

/**
 * The registry's scope factory: a scope from the settings snapshot of the moment and the store, or null when spilling is
 * off or there is no store, in which case the query runs on its pre-feature paths with the node's block factory. The
 * snapshot is read once per scope, here, when the first holder on this node creates it: turning the switch off never
 * touches a scope that exists, so a query that spills keeps spilling. A null is not remembered by the registry, so
 * turning the switch on may give a scope to a later compute of a query that started without one; that is harmless,
 * since every holder of a lease is independent and does without one, as the nodes of a query may disagree anyway while
 * a settings update reaches them.
 */
public final class SpillScopes {

    private SpillScopes() {}

    /**
     * @param codecExecutor where the query's run writers serialize, the plugin's codec pool
     */
    public static QueryScopeRegistry.ScopeFactory factory(
        SpillSettings.Holder settings,
        @Nullable SpillStore store,
        BlockFactory nodeFactory,
        Executor codecExecutor
    ) {
        return (queryId, onClosing) -> {
            SpillSettings current = settings.get();
            if (current.enabled() == false || store == null) {
                return null;
            }
            QueryScope scope = QueryScope.create(
                queryId,
                nodeFactory,
                current.budgetBytes(nodeFactory.breaker()),
                SpillSettings.MIN_SPILL_BYTES,
                store,
                codecExecutor,
                onClosing
            );
            scope.setResultBudgetBytes(current.resultBudgetBytes(nodeFactory.breaker()));
            return scope;
        };
    }
}
