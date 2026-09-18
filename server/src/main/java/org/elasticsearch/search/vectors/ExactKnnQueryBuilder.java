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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.LeafQueryBuilder;
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
public class ExactKnnQueryBuilder extends LeafQueryBuilder<ExactKnnQueryBuilder> {
    public static final String NAME = "exact_knn";

    static final TransportVersion EXACT_KNN_OVERSAMPLE = TransportVersion.fromName("exact_knn_oversample");

    private final String field;
    private final VectorData query;
    private final Float vectorSimilarity;
    private final Float oversample;

    /**
     * Creates a query builder.
     *
     * @param query the query vector
     * @param field the field that was used for the kNN query
     */
    public ExactKnnQueryBuilder(VectorData query, String field, Float vectorSimilarity) {
        this(query, field, vectorSimilarity, null);
    }

    /**
     * Creates a query builder.
     *
     * @param query      the query vector
     * @param field      the field that was used for the kNN query
     * @param oversample the {@code rescore_vector.oversample} the originating query specified, or {@code null} to use the
     *                   field's configured value. Selects the scoring fidelity; see
     *                   {@link DenseVectorFieldMapper.DenseVectorFieldType#createIndexedExactKnnQuery(VectorData, Float, Float)}.
     */
    public ExactKnnQueryBuilder(VectorData query, String field, Float vectorSimilarity, Float oversample) {
        this.query = query;
        this.field = field;
        this.vectorSimilarity = vectorSimilarity;
        this.oversample = oversample;
    }

    public ExactKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.query = in.readOptionalWriteable(VectorData::new);
        this.field = in.readString();
        this.vectorSimilarity = in.readOptionalFloat();
        this.oversample = in.getTransportVersion().supports(EXACT_KNN_OVERSAMPLE) ? in.readOptionalFloat() : null;
    }

    String getField() {
        return field;
    }

    VectorData getQuery() {
        return query;
    }

    Float vectorSimilarity() {
        return vectorSimilarity;
    }

    Float oversample() {
        return oversample;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(query);
        out.writeString(field);
        out.writeOptionalFloat(vectorSimilarity);
        if (out.getTransportVersion().supports(EXACT_KNN_OVERSAMPLE)) {
            out.writeOptionalFloat(oversample);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query);
        builder.field("field", field);
        if (vectorSimilarity != null) {
            builder.field("similarity", vectorSimilarity);
        }
        if (oversample != null) {
            builder.field("oversample", oversample);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            return Queries.NO_DOCS_INSTANCE;
        }
        if (fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }
        final DenseVectorFieldMapper.DenseVectorFieldType vectorFieldType = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;
        return vectorFieldType.createIndexedExactKnnQuery(query, vectorSimilarity, oversample);
    }

    @Override
    protected boolean doEquals(ExactKnnQueryBuilder other) {
        return field.equals(other.field)
            && Objects.equals(query, other.query)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity)
            && Objects.equals(oversample, other.oversample);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Objects.hashCode(query), vectorSimilarity, oversample);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }
}
