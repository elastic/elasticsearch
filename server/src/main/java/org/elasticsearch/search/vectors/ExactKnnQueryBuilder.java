/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Exact knn query builder. Will iterate and score all documents that have the provided knn field in the index.
 * Useful in inner hits scoring scenarios.
 */
public class ExactKnnQueryBuilder extends AbstractQueryBuilder<ExactKnnQueryBuilder> {
    public static final String NAME = "exact_knn";
    private final String field;
    private final VectorData query;

    /**
     * Creates a query builder.
     *
     * @param query    the query vector
     * @param field    the field that was used for the kNN query
     */
    public ExactKnnQueryBuilder(float[] query, String field) {
        this(VectorData.fromFloats(query), field);
    }

    /**
     * Creates a query builder.
     *
     * @param query    the query vector
     * @param field    the field that was used for the kNN query
     */
    public ExactKnnQueryBuilder(VectorData query, String field) {
        this.query = query;
        this.field = field;
    }

    public ExactKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.KNN_EXPLICIT_BYTE_QUERY_VECTOR_PARSING)) {
            this.query = in.readOptionalWriteable(VectorData::new);
        } else {
            this.query = VectorData.fromFloats(in.readFloatArray());
        }
        this.field = in.readString();
    }

    String getField() {
        return field;
    }

    VectorData getQuery() {
        return query;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.KNN_EXPLICIT_BYTE_QUERY_VECTOR_PARSING)) {
            out.writeOptionalWriteable(query);
        } else {
            out.writeFloatArray(query.asFloatVector());
        }
        out.writeString(field);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query);
        builder.field("field", field);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
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
        return vectorFieldType.createExactKnnQuery(query);
    }

    @Override
    protected boolean doEquals(ExactKnnQueryBuilder other) {
        return field.equals(other.field) && Objects.equals(query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Objects.hashCode(query));
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.NESTED_KNN_MORE_INNER_HITS;
    }
}
