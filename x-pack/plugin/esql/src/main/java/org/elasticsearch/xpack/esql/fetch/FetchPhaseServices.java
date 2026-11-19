/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

/**
 * The entry point to the runtime of the fetch phase on a coordinator. ES|QL code outside this package gets the operators
 * and the context trackers of the fetch phase from it, so the runtime can change without touching that code.
 */
public interface FetchPhaseServices {
    /**
     * The fetch operators of the query that {@code scope} describes, on its coordinator.
     */
    FetchOperatorProvider operatorProvider(QueryFetchScope scope);

    /**
     * A new tracker of the fetch contexts of one execution, which frees them as the user who calls this.
     */
    FetchContextTracker newContextTracker();

    /**
     * The runtime of this node: a {@link FetchOperator} for each {@link FetchExec} that sends its requests through
     * {@code fetchService} to the nodes of the local cluster. Its requests carry the index expressions of the
     * {@code FetchExec} with the indices options of the query's data node requests.
     */
    static FetchPhaseServices create(
        FetchService fetchService,
        SearchTransportService searchTransportService,
        ClusterService clusterService
    ) {
        return new FetchPhaseServices() {
            @Override
            public FetchOperatorProvider operatorProvider(QueryFetchScope scope) {
                return new FetchOperatorProvider() {
                    @Override
                    public Operator.OperatorFactory fetchOperator(FetchExec exec, int docChannel) {
                        OriginalIndices indices = new OriginalIndices(
                            exec.originalIndices().toArray(String[]::new),
                            SearchRequest.DEFAULT_INDICES_OPTIONS
                        );
                        return FetchOperator.Factory.forExec(
                            exec,
                            docChannel,
                            new QueryFetchClient(
                                fetchService::sendFetch,
                                clusterService.threadPool().getThreadContext(),
                                () -> clusterService.state().nodes(),
                                scope,
                                indices
                            )
                        );
                    }

                    @Override
                    public boolean receivesDetachedDocuments() {
                        return true;
                    }
                };
            }

            @Override
            public FetchContextTracker newContextTracker() {
                return new FetchContextTracker(searchTransportService, clusterService);
            }
        };
    }
}
