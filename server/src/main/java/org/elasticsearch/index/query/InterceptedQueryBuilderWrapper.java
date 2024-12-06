/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Wrapper for instances of {@link AbstractQueryBuilder} that have been intercepted using the {@link QueryRewriteInterceptor} to
 * break out of the rewrite phase. These instances are unwrapped on serialization.
 * @param <T>
 */
class InterceptedQueryBuilderWrapper<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<T> {

    protected final T queryBuilder;

    InterceptedQueryBuilderWrapper(T queryBuilder) {
        super();
        this.queryBuilder = queryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        queryBuilder.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        queryBuilder.toXContent(builder, params);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return queryBuilder.doToQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // Ensure that we only perform interception once in a query's rewrite phase
        QueryRewriteContext interceptedContext = queryRewriteContext.getInterceptedQueryRewriteContext();
        var rewritten = queryBuilder.rewrite(interceptedContext);
        if (rewritten == queryBuilder) {
            return this;
        }
        return rewritten;
    }

    @Override
    protected boolean doEquals(T other) {
        // Handle the edge case where we need to unwrap the incoming query builder
        if (other instanceof InterceptedQueryBuilderWrapper) {
            @SuppressWarnings("unchecked")
            InterceptedQueryBuilderWrapper<T> wrapper = (InterceptedQueryBuilderWrapper<T>) other;
            return queryBuilder.doEquals(wrapper.queryBuilder);
        } else {
            return queryBuilder.doEquals(other);
        }
    }

    @Override
    protected int doHashCode() {
        return queryBuilder.doHashCode();
    }

    @Override
    public String getWriteableName() {
        return queryBuilder.getWriteableName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return queryBuilder.getMinimalSupportedVersion();
    }
}
