/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Transport-only exact MaxSim query used as the baseline for {@code _rank_vectors_eval}.
 * It intentionally has no public XContent representation.
 */
public final class ExactRankVectorsQueryBuilder extends LeafQueryBuilder<ExactRankVectorsQueryBuilder> {

    public static final String NAME = "_exact_rank_vectors";

    private final String field;
    private final float[][] queryVectors;

    public ExactRankVectorsQueryBuilder(String field, float[][] queryVectors) {
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
    }

    public ExactRankVectorsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        queryVectors = in.readArray(StreamInput::readFloatArray, float[][]::new);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        MappedFieldType mappedFieldType = context.getFieldType(field);
        if (mappedFieldType == null) {
            return Queries.NO_DOCS_INSTANCE;
        }
        if (mappedFieldType instanceof RankVectorsFieldMapper.RankVectorsFieldType == false) {
            throw new IllegalArgumentException("field [" + field + "] is not a [rank_vectors] field");
        }
        RankVectorsFieldMapper.RankVectorsFieldType fieldType = (RankVectorsFieldMapper.RankVectorsFieldType) mappedFieldType;
        return fieldType.exactMaxSimQuery(queryVectors);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeArray(StreamOutput::writeFloatArray, queryVectors);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", field);
        builder.startArray("query_vector");
        for (float[] vector : queryVectors) {
            builder.startArray();
            for (float value : vector) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return RankVectorsQueryBuilder.RANK_VECTORS_QUERY;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        return this;
    }

    @Override
    protected boolean doEquals(ExactRankVectorsQueryBuilder other) {
        return field.equals(other.field) && Arrays.deepEquals(queryVectors, other.queryVectors);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.deepHashCode(queryVectors));
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
