/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

record ComputeContext(
    String sessionId,
    String clusterAlias,
    List<SearchContext> searchContexts,
    Configuration configuration,
    FoldContext foldCtx,
    ExchangeSourceHandler exchangeSource,
    ExchangeSinkHandler exchangeSink
) {
    List<SearchExecutionContext> searchExecutionContexts() {
        return searchContexts.stream().map(SearchContext::getSearchExecutionContext).toList();
    }
}
