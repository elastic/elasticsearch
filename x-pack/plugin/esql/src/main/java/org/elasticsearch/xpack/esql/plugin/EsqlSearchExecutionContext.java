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

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the
 * {@link QueryWarnings} bridge.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    private final QueryWarnings queryWarnings;

    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings) {
        super(base);
        this.queryWarnings = queryWarnings;
    }

    @Override
    public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
        return new ReinitializingSourceProvider(super::createSourceProvider);
    }

    /**
     * Return the {@link QueryWarnings} bridge for this context.
     */
    public QueryWarnings queryWarnings() {
        return queryWarnings;
    }
}
