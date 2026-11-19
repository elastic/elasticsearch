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
 * Builds what the coordinator of a query needs for the fetch phase: a tracker of the fetch contexts of each execution, and
 * a {@link FetchOperator} for each {@link FetchExec}. The client of the operator lives in this package, so code outside it
 * gets the operators from here.
 */
public final class FetchPhaseServices {
    private final FetchService fetchService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;

    public FetchPhaseServices(FetchService fetchService, SearchTransportService searchTransportService, ClusterService clusterService) {
        this.fetchService = fetchService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
    }

    /**
     * The fetch operators of the query that {@code scope} describes, on its coordinator. Each sends its requests through
     * {@link FetchService} to the nodes of the local cluster, with the index expressions of its {@link FetchExec} and the
     * indices options of the query's data node requests.
     */
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

    /**
     * A new tracker of the fetch contexts of one execution, which frees them as the user who calls this.
     */
    public FetchContextTracker newContextTracker() {
        return new FetchContextTracker(searchTransportService, clusterService);
    }
}
