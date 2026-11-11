/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TransientQueryBuilder;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Builds the wrapped query at the root nested scope, regardless of the nested scope it is used in.
 */
final class RootScopedQueryBuilder extends TransientQueryBuilder<RootScopedQueryBuilder> {

    static final String NAME = "root_scoped";

    private final QueryBuilder query;

    RootScopedQueryBuilder(QueryBuilder query) {
        this.query = Objects.requireNonNull(query);
    }

    QueryBuilder query() {
        return query;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        try (Releasable ignored = context.nestedScope().unwindTo(null)) {
            return query.toQuery(context, visitor);
        }
    }

    @Override
    protected QueryBuilder doTransientRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder rewritten = query.rewrite(context);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        return rewritten != query ? new RootScopedQueryBuilder(rewritten) : this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RootScopedQueryBuilder other) {
        return query.equals(other.query);
    }

    @Override
    protected int doHashCode() {
        return query.hashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
