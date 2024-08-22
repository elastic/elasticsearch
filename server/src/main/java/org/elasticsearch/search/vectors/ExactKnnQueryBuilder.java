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
    private final Float vectorSimilarity;

    /**
     * Creates a query builder.
     *
     * @param query    the query vector
     * @param field    the field that was used for the kNN query
     */
    public ExactKnnQueryBuilder(VectorData query, String field, Float vectorSimilarity) {
        this.query = query;
        this.field = field;
        this.vectorSimilarity = vectorSimilarity;
    }

    public ExactKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.query = in.readOptionalWriteable(VectorData::new);
        } else {
            this.query = VectorData.fromFloats(in.readFloatArray());
        }
        this.field = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS)
            || in.getTransportVersion().isPatchFrom(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
        }
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalWriteable(query);
        } else {
            out.writeFloatArray(query.asFloatVector());
        }
        out.writeString(field);
        if (out.getTransportVersion().onOrAfter(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS)
            || out.getTransportVersion().isPatchFrom(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15)) {
            out.writeOptionalFloat(vectorSimilarity);
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
        return vectorFieldType.createExactKnnQuery(query, vectorSimilarity);
    }

    @Override
    protected boolean doEquals(ExactKnnQueryBuilder other) {
        return field.equals(other.field) && Objects.equals(query, other.query) && Objects.equals(vectorSimilarity, other.vectorSimilarity);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Objects.hashCode(query), vectorSimilarity);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }
}
