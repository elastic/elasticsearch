/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

/**
 * The entry point to the runtime of the fetch phase. ES|QL code outside this package gets the operators and the reader
 * contexts of the fetch phase from it, so the runtime can change without touching that code.
 */
public interface FetchPhaseServices {
    /**
     * The fetch operators of the query that {@code scope} describes, on its coordinator.
     */
    FetchOperatorProvider operatorProvider(QueryFetchScope scope);

    /**
     * The reader contexts this node keeps open for the fetch phase as a data node.
     */
    FetchContextService contextService();

    /**
     * The leases of the fetch contexts of the queries this node coordinates.
     */
    FetchContextLeases leases();

    /**
     * The runtime of this node: a {@link FetchOperator} for each {@link FetchExec} that sends its requests through
     * {@code fetchService}, the reader contexts of {@code contextService} and the leases of {@code leases}.
     */
    static FetchPhaseServices create(
        FetchContextService contextService,
        FetchContextLeases leases,
        FetchService fetchService,
        ClusterService clusterService
    ) {
        return new FetchPhaseServices() {
            @Override
            public FetchOperatorProvider operatorProvider(QueryFetchScope scope) {
                return new FetchOperatorProvider() {
                    @Override
                    public Operator.OperatorFactory fetchOperator(FetchExec exec, int docChannel) {
                        return FetchOperator.Factory.forExec(
                            exec,
                            docChannel,
                            new QueryFetchClient(
                                fetchService::sendFetch,
                                clusterService.threadPool().getThreadContext(),
                                leases.nodes(),
                                scope,
                                exec.originalIndices()
                            ),
                            scope.fetchFailures(),
                            scope.stats()
                        );
                    }

                    @Override
                    public boolean receivesDetachedDocuments() {
                        return true;
                    }
                };
            }

            @Override
            public FetchContextService contextService() {
                return contextService;
            }

            @Override
            public FetchContextLeases leases() {
                return leases;
            }
        };
    }
}
