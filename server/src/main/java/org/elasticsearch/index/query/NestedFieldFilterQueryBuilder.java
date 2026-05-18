/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
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
 * An internal query builder used exclusively by kNN vector search to apply filters in nested
 * inner_hits contexts. It correctly handles filters that contain {@code must_not} clauses
 * targeting either parent or nested fields.
 *
 * <p>During inner_hits rewriting, the search context (field mappings, nested lookup) is not yet
 * available. This builder defers the decision of how to wrap the filter until {@link #doToQuery},
 * where the {@link SearchExecutionContext} provides the information needed to distinguish
 * parent-level from child-level clauses via {@link NestedHelper#decomposeFilter}.
 *
 * <p>This replaces the previous approach of using a syntactic heuristic at rewrite time, which
 * could not distinguish {@code must_not} on parent fields from {@code must_not} on nested fields.
 *
 * <p>This query is used only for internal kNN filter processing and is not exposed to users.
 * It is never serialized across the wire.
 */
public class NestedFieldFilterQueryBuilder extends LeafQueryBuilder<NestedFieldFilterQueryBuilder> {
    public static final String NAME = "nested_field_filter";

    private static final TransportVersion NESTED_FIELD_FILTER_QUERY = TransportVersion.fromName("nested_field_filter_query");

    private final QueryBuilder filterQueryBuilder;

    public NestedFieldFilterQueryBuilder(QueryBuilder filterQueryBuilder) {
        this.filterQueryBuilder = Objects.requireNonNull(filterQueryBuilder);
    }

    public NestedFieldFilterQueryBuilder(StreamInput in) throws IOException {
        super(in);
        filterQueryBuilder = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filterQueryBuilder);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("filter");
        filterQueryBuilder.toXContent(builder, params);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = filterQueryBuilder.rewrite(queryRewriteContext);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        if (rewritten != filterQueryBuilder) {
            return new NestedFieldFilterQueryBuilder(rewritten);
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

        String nestedPath = originalObjectMapper != null ? originalObjectMapper.fullPath() : null;
        Query filterQuery = filterQueryBuilder.toQuery(context);

        if (nestedPath != null) {
            NestedHelper.DecomposedFilter decomposed = NestedHelper.decomposeFilter(filterQuery, nestedPath, context);
            if (decomposed != null && decomposed.hasBothLevels()) {
                // Mixed filter: wrap parent clauses with ToChildBlockJoinQuery, apply child clauses directly
                BitSetProducer parentBitSet = context.bitsetFilter(parentFilter);
                Query parentQuery = NestedHelper.toBooleanQuery(decomposed.parentClauses());
                parentQuery = new ToChildBlockJoinQuery(Queries.filtered(parentQuery, parentFilter), parentBitSet);
                Query childQuery = NestedHelper.toBooleanQuery(decomposed.childClauses());
                return new BooleanQuery.Builder().add(parentQuery, BooleanClause.Occur.FILTER)
                    .add(childQuery, BooleanClause.Occur.FILTER)
                    .build();
            } else if (decomposed != null && decomposed.isChildOnly()) {
                // Pure child-level filter (e.g. must_not on nested field with synthetic MatchAllDocsQuery)
                return NestedHelper.toBooleanQuery(decomposed.childClauses());
            }
        }

        // No decomposition needed: filter targets a single level. Use the should/should pattern
        // so the filter matches whether it targets parent docs (via join) or nested docs (directly).
        BitSetProducer parentBitSet = context.bitsetFilter(parentFilter);
        Query joinedFilter = new ToChildBlockJoinQuery(Queries.filtered(filterQuery, parentFilter), parentBitSet);
        return new BooleanQuery.Builder().add(filterQuery, BooleanClause.Occur.SHOULD)
            .add(joinedFilter, BooleanClause.Occur.SHOULD)
            .build();
    }

    @Override
    protected boolean doEquals(NestedFieldFilterQueryBuilder other) {
        return Objects.equals(filterQueryBuilder, other.filterQueryBuilder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filterQueryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return NESTED_FIELD_FILTER_QUERY;
    }
}
