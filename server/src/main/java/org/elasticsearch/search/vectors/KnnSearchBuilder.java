/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Defines a kNN search to run in the search request.
 */
public class KnnSearchBuilder implements Writeable, ToXContentFragment {
    private static final int NUM_CANDS_LIMIT = 10000;
    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    static final ParseField BOOST_FIELD = AbstractQueryBuilder.BOOST_FIELD;

    private static final ConstructingObjectParser<KnnSearchBuilder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        @SuppressWarnings("unchecked")
        List<Float> vector = (List<Float>) args[1];
        float[] vectorArray = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            vectorArray[i] = vector.get(i);
        }
        return new KnnSearchBuilder((String) args[0], vectorArray, (int) args[2], (int) args[3]);
    });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(KnnSearchBuilder::boost, BOOST_FIELD);
    }

    public static KnnSearchBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    // visible for testing
    final String field;
    final float[] queryVector;
    final int k;
    final int numCands;
    float boost = AbstractQueryBuilder.DEFAULT_BOOST;

    /**
     * Defines a kNN search.
     *
     * @param field       the name of the vector field to search against
     * @param queryVector the query vector
     * @param k           the final number of nearest neighbors to return as top hits
     * @param numCands    the number of nearest neighbor candidates to consider per shard
     */
    public KnnSearchBuilder(String field, float[] queryVector, int k, int numCands) {
        this.field = field;
        this.queryVector = queryVector;
        this.k = k;
        this.numCands = numCands;
    }

    public KnnSearchBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        this.queryVector = in.readFloatArray();
        this.boost = in.readFloat();
    }

    /**
     * An optional boost to apply to the kNN search scores.
     */
    public KnnSearchBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * The number of nearest neighbors to return as top hits.
     */
    public int k() {
        return k;
    }

    public KnnVectorQueryBuilder toQueryBuilder() {
        if (k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCands < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than " + "[" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (numCands > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        return new KnnVectorQueryBuilder(field, queryVector, numCands).boost(boost);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnSearchBuilder that = (KnnSearchBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(field, that.field)
            && Arrays.equals(queryVector, that.queryVector);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, k, numCands);
        result = 31 * result + Arrays.hashCode(queryVector);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field)
            .field(K_FIELD.getPreferredName(), k)
            .field(NUM_CANDS_FIELD.getPreferredName(), numCands)
            .array(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);

        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            builder.field(BOOST_FIELD.getPreferredName(), boost);
        }

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(k);
        out.writeVInt(numCands);
        out.writeFloatArray(queryVector);
        out.writeFloat(boost);
    }
}
