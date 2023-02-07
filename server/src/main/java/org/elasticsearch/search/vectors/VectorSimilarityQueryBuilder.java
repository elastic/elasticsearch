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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class VectorSimilarityQueryBuilder extends AbstractQueryBuilder<VectorSimilarityQueryBuilder> {
    public static final String NAME = "vector_similarity";
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField QUERY_VECTOR = new ParseField("query_vector");
    private static final ParseField NUM_CANDIDATES = new ParseField("num_candidates");
    private static final ParseField SIMILARITY = new ParseField("similarity");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<VectorSimilarityQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME + "_parser",
        args -> {
            List<Float> vector = (List<Float>) args[1];
            final float[] vectorArray;
            vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
            return new VectorSimilarityQueryBuilder((String) args[0], vectorArray, (Integer) args[2], (Float) args[3]);
        }
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareFloatArray(ConstructingObjectParser.constructorArg(), QUERY_VECTOR);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CANDIDATES);
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), SIMILARITY);
        AbstractQueryBuilder.declareStandardFields(PARSER);
    }

    public static VectorSimilarityQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final float[] queryVector;
    private final int numCandidates;
    private final float similarity;

    public VectorSimilarityQueryBuilder(String field, float[] queryVector, int numCandidates, float similarity) {
        this.field = Objects.requireNonNull(field, Strings.format("[%s] must not be null", FIELD.getPreferredName()));
        this.queryVector = Objects.requireNonNull(queryVector, Strings.format("[%s] must not be null", QUERY_VECTOR.getPreferredName()));
        if (numCandidates <= 0) {
            throw new IllegalArgumentException(Strings.format("[%s] must be greater than 0", NUM_CANDIDATES.getPreferredName()));
        }
        this.numCandidates = numCandidates;
        this.similarity = similarity;
    }

    public VectorSimilarityQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.queryVector = in.readFloatArray();
        this.numCandidates = in.readVInt();
        this.similarity = in.readFloat();
    }

    public String getField() {
        return field;
    }

    public float[] getQueryVector() {
        return queryVector;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public float getSimilarity() {
        return similarity;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.CURRENT;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(queryVector);
        out.writeVInt(numCandidates);
        out.writeFloat(similarity);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        boostAndQueryNameToXContent(builder);
        builder.field(FIELD.getPreferredName(), field);
        builder.field(QUERY_VECTOR.getPreferredName(), queryVector);
        builder.field(NUM_CANDIDATES.getPreferredName(), numCandidates);
        builder.field(SIMILARITY.getPreferredName(), similarity);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + field + "] does not exist in the mapping");
        }

        if (fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        DenseVectorFieldMapper.DenseVectorFieldType vectorFieldType = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;
        return vectorFieldType.createVectorSimilarity(queryVector, similarity, numCandidates);
    }

    @Override
    protected boolean doEquals(VectorSimilarityQueryBuilder other) {
        return Objects.equals(field, other.field)
            && Arrays.equals(queryVector, other.queryVector)
            && similarity == other.similarity
            && numCandidates == other.numCandidates;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.hashCode(queryVector), numCandidates, similarity);
    }
}
