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
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TransientQueryBuilder;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A filter of a knn query, joined to the nested level of the vector field like the knn query joins its filters
 * (see {@link NestedHelper#joinToNestedLevel}). Used by the inner hits rewrite of knn queries.
 */
final class NestedLevelJoinQueryBuilder extends TransientQueryBuilder<NestedLevelJoinQueryBuilder> {

    static final String NAME = "nested_level_join";

    private final QueryBuilder filter;
    private final String vectorField;

    NestedLevelJoinQueryBuilder(QueryBuilder filter, String vectorField) {
        this.filter = Objects.requireNonNull(filter);
        this.vectorField = Objects.requireNonNull(vectorField);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        Query query = filter.toQuery(context);
        String vectorPath = context.nestedLookup().getNestedParent(vectorField);
        if (vectorPath != null) {
            NestedObjectMapper parentLevel = context.nestedScope().parentLevel();
            query = NestedHelper.joinToNestedLevel(query, vectorPath, parentLevel == null ? null : parentLevel.fullPath(), context);
        }
        // the join restructures the filter, so the clauses are counted on the joined query
        query.visit(visitor);
        return query;
    }

    @Override
    protected QueryBuilder doTransientRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder rewritten = filter.rewrite(context);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        return rewritten != filter ? new NestedLevelJoinQueryBuilder(rewritten, vectorField) : this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", vectorField);
        builder.field("filter", filter);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(NestedLevelJoinQueryBuilder other) {
        return filter.equals(other.filter) && vectorField.equals(other.vectorField);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filter, vectorField);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
