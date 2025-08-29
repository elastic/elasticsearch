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
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A query returns child documents whose parent matches the provided query.
 * This query is used only for internal purposes and is not exposed to a user.
 */
public class ToChildBlockJoinQueryBuilder extends AbstractQueryBuilder<ToChildBlockJoinQueryBuilder> {
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
        final Query parentFilter;
        NestedObjectMapper originalObjectMapper = context.nestedScope().getObjectMapper();
        if (originalObjectMapper != null) {
            try {
                // we are in a nested context, to get the parent filter we need to go up one level
                context.nestedScope().previousLevel();
                NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
                parentFilter = objectMapper == null
                    ? Queries.newNonNestedFilter(context.indexVersionCreated())
                    : objectMapper.nestedTypeFilter();
            } finally {
                context.nestedScope().nextLevel(originalObjectMapper);
            }
        } else {
            // we are NOT in a nested context, coming from the top level knn search
            parentFilter = Queries.newNonNestedFilter(context.indexVersionCreated());
        }
        final BitSetProducer parentBitSet = context.bitsetFilter(parentFilter);
        Query parentQuery = parentQueryBuilder.toQuery(context);
        // ensure that parentQuery only applies to parent docs by adding parentFilter
        return new ToChildBlockJoinQuery(Queries.filtered(parentQuery, parentFilter), parentBitSet);
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
