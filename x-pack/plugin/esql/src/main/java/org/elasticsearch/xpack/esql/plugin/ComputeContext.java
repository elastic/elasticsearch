/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.SharedSortBound;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.function.Supplier;

/**
 * @param queryId the task of the query's coordinator in this cluster
 */
record ComputeContext(
    String sessionId,
    TaskId queryId,
    String description,
    String clusterAlias,
    EsqlFlags flags,
    IndexedByShardId<ComputeSearchContext> searchContexts,
    Configuration configuration,
    FoldContext foldCtx,
    Supplier<ExchangeSource> exchangeSourceSupplier,
    Supplier<ExchangeSink> exchangeSinkSupplier,
    boolean singleNodeOptimizations,
    @Nullable SharedSortBound sortBound
) {
    ComputeContext(
        String sessionId,
        TaskId queryId,
        String description,
        String clusterAlias,
        EsqlFlags flags,
        IndexedByShardId<ComputeSearchContext> searchContexts,
        Configuration configuration,
        FoldContext foldCtx,
        Supplier<ExchangeSource> exchangeSourceSupplier,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        boolean singleNodeOptimizations
    ) {
        this(
            sessionId,
            queryId,
            description,
            clusterAlias,
            flags,
            searchContexts,
            configuration,
            foldCtx,
            exchangeSourceSupplier,
            exchangeSinkSupplier,
            singleNodeOptimizations,
            null
        );
    }
}
