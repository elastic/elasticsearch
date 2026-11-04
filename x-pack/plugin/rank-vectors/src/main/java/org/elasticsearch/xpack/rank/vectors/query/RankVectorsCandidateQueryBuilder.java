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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Transport-only query used by {@code _rank_vectors_eval} to inspect raw per-shard
 * candidates. It has no XContent representation or parser.
 */
public final class RankVectorsCandidateQueryBuilder extends LeafQueryBuilder<RankVectorsCandidateQueryBuilder> {

    public static final String NAME = "_rank_vectors_muvera_candidates";
    private final String field;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;
    @Nullable
    private final Float queryOversample;

    public RankVectorsCandidateQueryBuilder(String field, float[][] queryVectors, int k, int numCandidates) {
        this(field, queryVectors, k, numCandidates, null);
    }

    public RankVectorsCandidateQueryBuilder(
        String field,
        float[][] queryVectors,
        int k,
        int numCandidates,
        @Nullable Float queryOversample
    ) {
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.k = k;
        this.numCandidates = numCandidates;
        this.queryOversample = queryOversample;
    }

    public RankVectorsCandidateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        queryVectors = in.readArray(StreamInput::readFloatArray, float[][]::new);
        k = in.readVInt();
        numCandidates = in.readVInt();
        queryOversample = in.getTransportVersion().supports(RankVectorsQueryBuilder.RANK_VECTORS_QUERY) ? in.readOptionalFloat() : null;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType mappedFieldType = context.getFieldType(field);
        if (mappedFieldType instanceof RankVectorsFieldMapper.RankVectorsFieldType == false) {
            throw new IllegalArgumentException("field [" + field + "] is not a [rank_vectors] field");
        }
        RankVectorsFieldMapper.RankVectorsFieldType rankVectorsFieldType = (RankVectorsFieldMapper.RankVectorsFieldType) mappedFieldType;
        Query aliasFilter = null;
        if (context.getAliasFilter() != null) {
            try (Releasable ignored = context.nestedScope().unwindTo(null)) {
                aliasFilter = context.getAliasFilter().toQuery(context);
            }
        }
        return rankVectorsFieldType.muveraCandidateQuery(queryVectors, k, numCandidates, queryOversample, aliasFilter);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeArray(StreamOutput::writeFloatArray, queryVectors);
        out.writeVInt(k);
        out.writeVInt(numCandidates);
        if (out.getTransportVersion().supports(RankVectorsQueryBuilder.RANK_VECTORS_QUERY)) {
            out.writeOptionalFloat(queryOversample);
        }
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
        builder.field("k", k);
        builder.field("num_candidates", numCandidates);
        if (queryOversample != null) {
            builder.startObject(RankVectorsQueryBuilder.RESCORE_VECTOR_FIELD.getPreferredName());
            builder.field("oversample", queryOversample);
            builder.endObject();
        }
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
    protected boolean doEquals(RankVectorsCandidateQueryBuilder other) {
        return field.equals(other.field)
            && Arrays.deepEquals(queryVectors, other.queryVectors)
            && k == other.k
            && numCandidates == other.numCandidates
            && Objects.equals(queryOversample, other.queryOversample);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.deepHashCode(queryVectors), k, numCandidates, queryOversample);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
