/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the
 * {@link QueryWarnings} bridge.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    private QueryWarnings queryWarnings;

    EsqlSearchExecutionContext(SearchExecutionContext base) {
        super(base);
    }

    @Override
    public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
        return new ReinitializingSourceProvider(super::createSourceProvider);
    }

    /**
     * Attach the {@link QueryWarnings} bridge; called by
     * {@link EsPhysicalOperationProviders} after both the shard
     * contexts and the bridge are known, before any query is built.
     */
    public void setQueryWarnings(QueryWarnings warnings) {
        this.queryWarnings = warnings;
    }

    /**
     * Return the {@link QueryWarnings} bridge, or {@code null} if none has been attached yet.
     */
    public QueryWarnings queryWarnings() {
        return queryWarnings;
    }
}
