/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.function.Supplier;

/**
 * @param fetchOperators the fetch operators of the plan. Only the coordinator of a query that plans the fetch phase has
 *                       them.
 */
record ComputeContext(
    String sessionId,
    String description,
    String clusterAlias,
    EsqlFlags flags,
    IndexedByShardId<ComputeSearchContext> searchContexts,
    Configuration configuration,
    FoldContext foldCtx,
    Supplier<ExchangeSource> exchangeSourceSupplier,
    Supplier<ExchangeSink> exchangeSinkSupplier,
    boolean retainSearchContexts,
    boolean singleNodeOptimizations,
    FetchOperatorProvider fetchOperators
) {
    /**
     * For computes whose plans never fetch.
     */
    ComputeContext(
        String sessionId,
        String description,
        String clusterAlias,
        EsqlFlags flags,
        IndexedByShardId<ComputeSearchContext> searchContexts,
        Configuration configuration,
        FoldContext foldCtx,
        Supplier<ExchangeSource> exchangeSourceSupplier,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        boolean retainSearchContexts,
        boolean singleNodeOptimizations
    ) {
        this(
            sessionId,
            description,
            clusterAlias,
            flags,
            searchContexts,
            configuration,
            foldCtx,
            exchangeSourceSupplier,
            exchangeSinkSupplier,
            retainSearchContexts,
            singleNodeOptimizations,
            FetchOperatorProvider.UNSUPPORTED
        );
    }

    IndexedByShardId<? extends SearchExecutionContext> searchExecutionContexts() {
        return searchContexts.map(s -> s.searchContext().getSearchExecutionContext());
    }
}
