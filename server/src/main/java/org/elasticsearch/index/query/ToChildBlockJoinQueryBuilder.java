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
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Returns the documents of the current nested level whose ancestor matches the provided query, the ancestor being at the
 * level of the fields the query reads (see {@link NestedHelper#joinToNestedLevel}). Used internally by the inner hits of knn
 * queries and not exposed to users.
 */
public class ToChildBlockJoinQueryBuilder extends LeafQueryBuilder<ToChildBlockJoinQueryBuilder> {
    public static final String NAME = "to_child_block_join";

    private static final TransportVersion TO_CHILD_BLOCK_JOIN_QUERY = TransportVersion.fromName("to_child_block_join_query");

    private final QueryBuilder parentQueryBuilder;

    public ToChildBlockJoinQueryBuilder(QueryBuilder parentQueryBuilder) {
        this.parentQueryBuilder = parentQueryBuilder;
    }

    public ToChildBlockJoinQueryBuilder(StreamInput in) throws IOException {
        super(in);
        parentQueryBuilder = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(parentQueryBuilder);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        parentQueryBuilder.toXContent(builder, params);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = parentQueryBuilder.rewrite(queryRewriteContext);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        if (rewritten != parentQueryBuilder) {
            return new ToChildBlockJoinQueryBuilder(rewritten);
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        NestedObjectMapper currentLevel = context.nestedScope().getObjectMapper();
        if (currentLevel == null) {
            // we are NOT in a nested context, coming from the top level knn search
            Query parentFilter = Queries.newNonNestedFilter(context.indexVersionCreated());
            Query parentQuery = parentQueryBuilder.toQuery(context);
            // ensure that parentQuery only applies to parent docs by adding parentFilter
            return new ToChildBlockJoinQuery(Queries.filtered(parentQuery, parentFilter), context.bitsetFilter(parentFilter));
        }
        NestedObjectMapper parentLevel = context.nestedScope().parentLevel();
        String parentPath = parentLevel == null ? null : parentLevel.fullPath();
        return NestedHelper.joinToNestedLevel(parentQueryBuilder.toQuery(context), currentLevel.fullPath(), parentPath, context);
    }

    @Override
    protected boolean doEquals(ToChildBlockJoinQueryBuilder other) {
        return Objects.equals(parentQueryBuilder, other.parentQueryBuilder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(parentQueryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TO_CHILD_BLOCK_JOIN_QUERY;
    }
}
