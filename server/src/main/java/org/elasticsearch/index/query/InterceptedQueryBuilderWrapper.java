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

    protected final QueryBuilder original;
    protected final QueryBuilder rewritten;

    InterceptedQueryBuilderWrapper(QueryBuilder rewritten, QueryBuilder original) {
        super();
        this.original = original;
        this.rewritten = rewritten;
    }

    public QueryBuilder getOriginal() {
        return original;
    }

    @Override
    public QueryBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryRewriteInterceptor queryRewriteInterceptor = queryRewriteContext.getQueryRewriteInterceptor();
        try {
            queryRewriteContext.setQueryRewriteInterceptor(null);
            QueryBuilder rewritten = this.rewritten.rewrite(queryRewriteContext);
            return rewritten != this.rewritten ? new InterceptedQueryBuilderWrapper(rewritten, original) : this;
        } finally {
            queryRewriteContext.setQueryRewriteInterceptor(queryRewriteInterceptor);
        }
    }

    @Override
    public String getWriteableName() {
        return rewritten.getWriteableName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return rewritten.getMinimalSupportedVersion();
    }

    @Override
    public Query toQuery(SearchExecutionContext context) throws IOException {
        return rewritten.toQuery(context);
    }

    @Override
    public QueryBuilder queryName(String queryName) {
        rewritten.queryName(queryName);
        return this;
    }

    @Override
    public String queryName() {
        return rewritten.queryName();
    }

    @Override
    public float boost() {
        return rewritten.boost();
    }

    @Override
    public QueryBuilder boost(float boost) {
        rewritten.boost(boost);
        return this;
    }

    @Override
    public String getName() {
        return rewritten.getName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        rewritten.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return rewritten.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InterceptedQueryBuilderWrapper that = (InterceptedQueryBuilderWrapper) o;
        return Objects.equals(original, that.original) && Objects.equals(rewritten, that.rewritten);
    }

    @Override
    public int hashCode() {
        return Objects.hash(original, rewritten);
    }
}
