/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Builds the wrapped query at the root nested scope, regardless of the nested scope it is used in. Unwrapped on serialization.
 */
class RootScopedQueryBuilder implements QueryBuilder {

    private final QueryBuilder query;

    RootScopedQueryBuilder(QueryBuilder query) {
        this.query = Objects.requireNonNull(query);
    }

    QueryBuilder query() {
        return query;
    }

    @Override
    public Query toQuery(SearchExecutionContext context) throws IOException {
        try (Releasable ignored = context.nestedScope().unwindTo(null)) {
            return query.toQuery(context);
        }
    }

    @Override
    public Query toQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        try (Releasable ignored = context.nestedScope().unwindTo(null)) {
            return query.toQuery(context, visitor);
        }
    }

    @Override
    public QueryBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = query.rewrite(queryRewriteContext);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        return rewritten != query ? new RootScopedQueryBuilder(rewritten) : this;
    }

    @Override
    public String getWriteableName() {
        return query.getWriteableName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return query.getMinimalSupportedVersion();
    }

    @Override
    public QueryBuilder queryName(String queryName) {
        query.queryName(queryName);
        return this;
    }

    @Override
    public String queryName() {
        return query.queryName();
    }

    @Override
    public float boost() {
        return query.boost();
    }

    @Override
    public QueryBuilder boost(float boost) {
        query.boost(boost);
        return this;
    }

    @Override
    public String getName() {
        return query.getName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        query.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return query.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RootScopedQueryBuilder that = (RootScopedQueryBuilder) o;
        return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(query);
    }
}
