/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the
 * {@link QueryWarnings} bridge.
 * <p>
 * When a Lucene-pushable {@code @timestamp} / {@code event.ingested} range is rewritten,
 * {@link #setTimeRangeFilterFromMillis} also records {@code time_range_filter_from} on
 * {@link ThreadContext} so blob-cache read/miss gauges can be attributed the same way as
 * search Query/DFS/Fetch phases. Callers should stash the thread context around planning
 * and driver submit so worker threads inherit the transient via {@code preserveContext}.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    private final QueryWarnings queryWarnings;
    private final ThreadContext threadContext;

    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings, ThreadContext threadContext) {
        super(base);
        this.queryWarnings = queryWarnings;
        this.threadContext = threadContext;
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

    @Override
    public void setTimeRangeFilterFromMillis(String fieldName, long timeRangeFilterFromMillis, DateFieldMapper.Resolution resolution) {
        super.setTimeRangeFilterFromMillis(fieldName, timeRangeFilterFromMillis, resolution);
        putTimeRangeFilterFrom();
    }

    @Override
    public void setTimeRangeFilterFromMillis(long timeRangeFilterFromMillis) {
        super.setTimeRangeFilterFromMillis(timeRangeFilterFromMillis);
        putTimeRangeFilterFrom();
    }

    private void putTimeRangeFilterFrom() {
        SearchRequestAttributesExtractor.putTimeRangeFilterFrom(threadContext, getTimeRangeFilterFromMillis(), nowInMillis.getAsLong());
    }
}
