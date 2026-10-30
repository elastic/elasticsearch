/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.HashJoinTables;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.function.Supplier;

/**
 * @param hashJoinTables keeps hash join tables for the other computes of the same request, or {@code null} if this
 *                       compute is the only one that joins against its data
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
    boolean singleNodeOptimizations,
    @Nullable HashJoinTables hashJoinTables
) {
    IndexedByShardId<? extends SearchExecutionContext> searchExecutionContexts() {
        return searchContexts.map(s -> s.searchContext().getSearchExecutionContext());
    }
}
