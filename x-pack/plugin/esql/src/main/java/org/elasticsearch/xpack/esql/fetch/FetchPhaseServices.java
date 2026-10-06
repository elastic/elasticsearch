/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.xpack.esql.fetch.lifetime.FetchContextService;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

/**
 * The entry point to the runtime of the fetch phase. ES|QL code outside this package gets the operators and the reader
 * contexts of the fetch phase from it, so the runtime can change without touching that code.
 */
public interface FetchPhaseServices {
    /**
     * Builds the operators of the fetch phase.
     */
    FetchOperatorProvider operatorProvider();

    /**
     * The reader contexts of the fetch phase on this node: the ones it keeps open as a data node, and the leases of the
     * queries it coordinates.
     */
    FetchContextService contextService();

    /**
     * The runtime of this node: a {@link FetchOperator} for each {@link org.elasticsearch.xpack.esql.plan.physical.FetchExec},
     * and the reader contexts of {@code contextService}.
     */
    static FetchPhaseServices create(FetchContextService contextService) {
        return new FetchPhaseServices() {
            @Override
            public FetchOperatorProvider operatorProvider() {
                return FetchOperator.PROVIDER;
            }

            @Override
            public FetchContextService contextService() {
                return contextService;
            }
        };
    }
}
