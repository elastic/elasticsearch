/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A query that matches the provided docs with their scores, but within a nested context.
 * This query is used when executing a nested kNN search during the search query phase, in particular, it will score all
 * children that matched the initial kNN query during the DFS phase. This is useful for gathering inner hits.
 */
public class NestedKnnScoreDocQueryBuilder extends AbstractQueryBuilder<NestedKnnScoreDocQueryBuilder> {
    public static final String NAME = "nested_knn_score_doc";
    private final QueryBuilder kNNQuery;
    private final String field;
    private final float[] query;

    /**
     * Creates a query builder.
     *
     * @param kNNQuery the kNN query that was executed during the DFS phase
     * @param query    the query vector
     * @param field    the field that was used for the kNN query
     */
    public NestedKnnScoreDocQueryBuilder(QueryBuilder kNNQuery, float[] query, String field) {
        this.kNNQuery = kNNQuery;
        this.query = query;
        this.field = field;
    }

    public NestedKnnScoreDocQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.kNNQuery = in.readNamedWriteable(QueryBuilder.class);
        this.query = in.readFloatArray();
        this.field = in.readString();
    }

    QueryBuilder getKNNQuery() {
        return kNNQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(kNNQuery);
        out.writeFloatArray(query);
        out.writeString(field);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("knn_query");
        kNNQuery.toXContent(builder, params);
        builder.field("query", query);
        builder.field("field", field);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        String parentPath = context.nestedLookup().getNestedParent(field);
        if (parentPath == null) {
            throw new IllegalArgumentException("field [" + field + "] is not a nested field");
        }
        final MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + field + "] does not exist in the mapping");
        }
        if (fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }
        final DenseVectorFieldMapper.DenseVectorFieldType vectorFieldType = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;
        final Query kNNQuery = this.kNNQuery.toQuery(context);
        final BitSetProducer parentFilter;
        NestedObjectMapper originalObjectMapper = context.nestedScope().getObjectMapper();
        if (originalObjectMapper != null) {
            try {
                // we are in a nested context, to get the parent filter we need to go up one level
                context.nestedScope().previousLevel();
                NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
                parentFilter = objectMapper == null
                    ? context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()))
                    : context.bitsetFilter(objectMapper.nestedTypeFilter());
            } finally {
                context.nestedScope().nextLevel(originalObjectMapper);
            }
        } else {
            // we are NOT in a nested context, coming from the top level knn search
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
        }
        return new ESDiversifyingChildrenKnnVectorQuery(kNNQuery, vectorFieldType.createExactKnnQuery(query), parentFilter);
    }

    @Override
    protected boolean doEquals(NestedKnnScoreDocQueryBuilder other) {
        return kNNQuery.equals(other.kNNQuery) && field.equals(other.field) && Arrays.equals(query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(kNNQuery, field, Arrays.hashCode(query));
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = kNNQuery.rewrite(queryRewriteContext);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        if (rewritten != kNNQuery) {
            return new NestedKnnScoreDocQueryBuilder(rewritten, query, field);
        }
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.NESTED_KNN_MORE_INNER_HITS;
    }
}
