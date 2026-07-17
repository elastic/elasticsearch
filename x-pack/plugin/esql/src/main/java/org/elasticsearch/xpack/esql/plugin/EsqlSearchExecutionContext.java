/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the
 * {@link QueryWarnings} bridge.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    @Nullable
    private final QueryWarnings queryWarnings;

    /**
     * @param queryWarnings the bridge, or {@code null} for contexts that never build warnings
     *                      like remote-fetch detached contexts
     */
    public EsqlSearchExecutionContext(SearchExecutionContext base, @Nullable QueryWarnings queryWarnings) {
        super(base);
        this.queryWarnings = queryWarnings;
    }

    @Override
    public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
        return new ReinitializingSourceProvider(super::createSourceProvider);
    }

    /**
     * Return the {@link QueryWarnings} bridge, or {@code null} if none was supplied at construction.
     */
    @Nullable
    public QueryWarnings queryWarnings() {
        return queryWarnings;
    }
}
