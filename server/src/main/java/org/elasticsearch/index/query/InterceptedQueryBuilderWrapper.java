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
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Wrapper for instances of {@link QueryBuilder} that have been intercepted using the {@link QueryRewriteInterceptor} to
 * break out of the rewrite phase. These instances are unwrapped on serialization.
 */
class InterceptedQueryBuilderWrapper implements QueryBuilder {

    protected final QueryBuilder queryBuilder;

    InterceptedQueryBuilderWrapper(QueryBuilder queryBuilder) {
        super();
        this.queryBuilder = queryBuilder;
    }

    @Override
    public QueryBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryRewriteInterceptor queryRewriteInterceptor = queryRewriteContext.getQueryRewriteInterceptor();
        try {
            queryRewriteContext.setQueryRewriteInterceptor(null);
            QueryBuilder rewritten = queryBuilder.rewrite(queryRewriteContext);
            return rewritten != queryBuilder ? new InterceptedQueryBuilderWrapper(rewritten) : this;
        } finally {
            queryRewriteContext.setQueryRewriteInterceptor(queryRewriteInterceptor);
        }
    }

    @Override
    public String getWriteableName() {
        return queryBuilder.getWriteableName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return queryBuilder.getMinimalSupportedVersion();
    }

    @Override
    public Query toQuery(SearchExecutionContext context) throws IOException {
        return queryBuilder.toQuery(context);
    }

    @Override
    public QueryBuilder queryName(String queryName) {
        queryBuilder.queryName(queryName);
        return this;
    }

    @Override
    public String queryName() {
        return queryBuilder.queryName();
    }

    @Override
    public float boost() {
        return queryBuilder.boost();
    }

    @Override
    public QueryBuilder boost(float boost) {
        queryBuilder.boost(boost);
        return this;
    }

    @Override
    public String getName() {
        return queryBuilder.getName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queryBuilder.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return queryBuilder.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof InterceptedQueryBuilderWrapper == false) return false;
        return Objects.equals(queryBuilder, ((InterceptedQueryBuilderWrapper) o).queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryBuilder);
    }
}
