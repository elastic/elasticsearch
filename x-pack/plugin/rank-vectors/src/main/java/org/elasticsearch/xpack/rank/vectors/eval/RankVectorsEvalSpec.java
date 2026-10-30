/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Input for one explicit-query-vector MUVERA recall evaluation. */
public final class RankVectorsEvalSpec implements Writeable, ToXContentObject {

    public static final int MAX_K = 1_000;
    public static final int MAX_NUM_CANDIDATES = 10_000;
    public static final int MAX_QUERY_VECTORS = 256;

    static final ParseField FIELD = new ParseField("field");
    static final ParseField QUERY_VECTOR = new ParseField("query_vector");
    static final ParseField K = new ParseField("k");
    static final ParseField NUM_CANDIDATES = new ParseField("num_candidates");
    static final ParseField ALLOW_EXACT = new ParseField("allow_exact");

    private static final ConstructingObjectParser<RankVectorsEvalSpec, Void> PARSER = new ConstructingObjectParser<>(
        "rank_vectors_eval",
        args -> new RankVectorsEvalSpec((String) args[0], (float[][]) args[1], (int) args[2], (int) args[3], (boolean) args[4])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (parser, context) -> parseVectors(parser),
            QUERY_VECTOR,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), K);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CANDIDATES);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ALLOW_EXACT);
    }

    private final String field;
    private final float[][] queryVector;
    private final int k;
    private final int numCandidates;
    private final boolean allowExact;

    public RankVectorsEvalSpec(String field, float[][] queryVector, int k, int numCandidates, boolean allowExact) {
        if (Strings.hasText(field) == false) {
            throw new IllegalArgumentException("[field] must be a non-empty field name");
        }
        if (queryVector == null || queryVector.length == 0 || queryVector.length > MAX_QUERY_VECTORS) {
            throw new IllegalArgumentException("[query_vector] must contain between 1 and " + MAX_QUERY_VECTORS + " vectors");
        }
        for (int i = 0; i < queryVector.length; i++) {
            if (queryVector[i] == null || queryVector[i].length == 0) {
                throw new IllegalArgumentException("[query_vector] vector [" + i + "] must not be empty");
            }
            for (float value : queryVector[i]) {
                if (Float.isFinite(value) == false) {
                    throw new IllegalArgumentException("[query_vector] contains a non-finite value");
                }
            }
        }
        if (k < 1 || k > MAX_K) {
            throw new IllegalArgumentException("[k] must be between 1 and " + MAX_K);
        }
        if (numCandidates < k || numCandidates > MAX_NUM_CANDIDATES) {
            throw new IllegalArgumentException("[num_candidates] must be between [k] and " + MAX_NUM_CANDIDATES);
        }
        if (allowExact == false) {
            throw new IllegalArgumentException(
                "[allow_exact] must be true because the exact MaxSim baseline can severely slow other searches"
            );
        }
        this.field = field;
        this.queryVector = deepCopy(queryVector);
        this.k = k;
        this.numCandidates = numCandidates;
        this.allowExact = true;
    }

    public RankVectorsEvalSpec(StreamInput in) throws IOException {
        this(in.readString(), in.readArray(StreamInput::readFloatArray, float[][]::new), in.readVInt(), in.readVInt(), in.readBoolean());
    }

    public static RankVectorsEvalSpec parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String field() {
        return field;
    }

    public float[][] queryVector() {
        return deepCopy(queryVector);
    }

    public int k() {
        return k;
    }

    public int numCandidates() {
        return numCandidates;
    }

    public boolean allowExact() {
        return allowExact;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeArray(StreamOutput::writeFloatArray, queryVector);
        out.writeVInt(k);
        out.writeVInt(numCandidates);
        out.writeBoolean(allowExact);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(QUERY_VECTOR.getPreferredName(), queryVector);
        builder.field(K.getPreferredName(), k);
        builder.field(NUM_CANDIDATES.getPreferredName(), numCandidates);
        builder.field(ALLOW_EXACT.getPreferredName(), allowExact);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof RankVectorsEvalSpec other
            && field.equals(other.field)
            && java.util.Arrays.deepEquals(queryVector, other.queryVector)
            && k == other.k
            && numCandidates == other.numCandidates
            && allowExact == other.allowExact;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, java.util.Arrays.deepHashCode(queryVector), k, numCandidates, allowExact);
    }

    private static float[][] parseVectors(XContentParser parser) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("[query_vector] must contain arrays of numbers");
            }
            List<Float> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                values.add(parser.floatValue());
            }
            float[] vector = new float[values.size()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = values.get(i);
            }
            vectors.add(vector);
        }
        return vectors.toArray(float[][]::new);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
