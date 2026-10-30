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
    private final int numCandidates;

    public RankVectorsCandidateQueryBuilder(String field, float[][] queryVectors, int numCandidates) {
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.numCandidates = numCandidates;
    }

    public RankVectorsCandidateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        queryVectors = in.readArray(StreamInput::readFloatArray, float[][]::new);
        numCandidates = in.readVInt();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        MappedFieldType mappedFieldType = context.getFieldType(field);
        if (mappedFieldType instanceof RankVectorsFieldMapper.RankVectorsFieldType == false) {
            throw new IllegalArgumentException("field [" + field + "] is not a [rank_vectors] field");
        }
        RankVectorsFieldMapper.RankVectorsFieldType rankVectorsFieldType = (RankVectorsFieldMapper.RankVectorsFieldType) mappedFieldType;
        return rankVectorsFieldType.muveraCandidateQuery(queryVectors, numCandidates);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeArray(StreamOutput::writeFloatArray, queryVectors);
        out.writeVInt(numCandidates);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) {
        throw new UnsupportedOperationException("internal MUVERA candidate queries do not support XContent");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // The public query and this private companion are introduced together.
        return RankVectorsQueryBuilder.RANK_VECTORS_QUERY;
    }

    @Override
    protected boolean doEquals(RankVectorsCandidateQueryBuilder other) {
        return field.equals(other.field) && Arrays.deepEquals(queryVectors, other.queryVectors) && numCandidates == other.numCandidates;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.deepHashCode(queryVectors), numCandidates);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
