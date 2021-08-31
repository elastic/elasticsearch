/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class NXYSignificanceHeuristic extends SignificanceHeuristic {

    protected static final ParseField BACKGROUND_IS_SUPERSET = new ParseField("background_is_superset");

    protected static final ParseField INCLUDE_NEGATIVES_FIELD = new ParseField("include_negatives");

    protected static final String SCORE_ERROR_MESSAGE = ", does your background filter not include all documents in the bucket? "
        + "If so and it is intentional, set \""
        + BACKGROUND_IS_SUPERSET.getPreferredName()
        + "\": false";

    protected final boolean backgroundIsSuperset;

    /**
     * Some heuristics do not differentiate between terms that are descriptive for subset or for
     * the background without the subset. We might want to filter out the terms that are appear much less often
     * in the subset than in the background without the subset.
     */
    protected final boolean includeNegatives;

    protected NXYSignificanceHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
        this.includeNegatives = includeNegatives;
        this.backgroundIsSuperset = backgroundIsSuperset;
    }

    /**
     * Read from a stream.
     */
    protected NXYSignificanceHeuristic(StreamInput in) throws IOException {
        includeNegatives = in.readBoolean();
        backgroundIsSuperset = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeNegatives);
        out.writeBoolean(backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NXYSignificanceHeuristic other = (NXYSignificanceHeuristic) obj;
        if (backgroundIsSuperset != other.backgroundIsSuperset) return false;
        if (includeNegatives != other.includeNegatives) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = (includeNegatives ? 1 : 0);
        result = 31 * result + (backgroundIsSuperset ? 1 : 0);
        return result;
    }

    protected static class Frequencies {
        public double N00, N01, N10, N11, N0_, N1_, N_0, N_1, N;
    }

    protected Frequencies computeNxys(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencies(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        Frequencies frequencies = new Frequencies();
        if (backgroundIsSuperset) {
            // documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq - (subsetSize - subsetFreq);
            // documents in class and do not contain term
            frequencies.N01 = (subsetSize - subsetFreq);
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq - subsetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            // documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq;
            // documents that contain term
            frequencies.N1_ = supersetFreq;
            // documents that are not in class
            frequencies.N_0 = supersetSize - subsetSize;
            // documents that are in class
            frequencies.N_1 = subsetSize;
            // all docs
            frequencies.N = supersetSize;
        } else {
            // documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq;
            // documents in class and do not contain term
            frequencies.N01 = subsetSize - subsetFreq;
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            // documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq + subsetSize - subsetFreq;
            // documents that contain term
            frequencies.N1_ = supersetFreq + subsetFreq;
            // documents that are not in class
            frequencies.N_0 = supersetSize;
            // documents that are in class
            frequencies.N_1 = subsetSize;
            // all docs
            frequencies.N = supersetSize + subsetSize;
        }
        return frequencies;
    }

    protected void checkFrequencies(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        if (backgroundIsSuperset) {
            if (subsetFreq > supersetFreq) {
                throw new IllegalArgumentException("subsetFreq > supersetFreq" + SCORE_ERROR_MESSAGE);
            }
            if (subsetSize > supersetSize) {
                throw new IllegalArgumentException("subsetSize > supersetSize" + SCORE_ERROR_MESSAGE);
            }
            if (supersetFreq - subsetFreq > supersetSize - subsetSize) {
                throw new IllegalArgumentException("supersetFreq - subsetFreq > supersetSize - subsetSize" + SCORE_ERROR_MESSAGE);
            }
        }
    }

    protected void build(XContentBuilder builder) throws IOException {
        builder.field(INCLUDE_NEGATIVES_FIELD.getPreferredName(), includeNegatives)
            .field(BACKGROUND_IS_SUPERSET.getPreferredName(), backgroundIsSuperset);
    }

    /**
     * Set up and {@linkplain ConstructingObjectParser} to accept the standard arguments for an {@linkplain NXYSignificanceHeuristic}.
     */
    protected static void declareParseFields(ConstructingObjectParser<? extends NXYSignificanceHeuristic, ?> parser) {
        parser.declareBoolean(optionalConstructorArg(), INCLUDE_NEGATIVES_FIELD);
        parser.declareBoolean(optionalConstructorArg(), BACKGROUND_IS_SUPERSET);
    }

    /**
     * Adapt a standard two argument ctor into one that consumes a {@linkplain ConstructingObjectParser}'s fields.
     */
    protected static <T> Function<Object[], T> buildFromParsedArgs(BiFunction<Boolean, Boolean, T> ctor) {
        return args -> {
            boolean includeNegatives = args[0] == null ? false : (boolean) args[0];
            boolean backgroundIsSuperset = args[1] == null ? true : (boolean) args[1];
            return ctor.apply(includeNegatives, backgroundIsSuperset);
        };
    }

    protected abstract static class NXYBuilder implements SignificanceHeuristicBuilder {
        protected boolean includeNegatives = true;
        protected boolean backgroundIsSuperset = true;

        public NXYBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
            this.includeNegatives = includeNegatives;
            this.backgroundIsSuperset = backgroundIsSuperset;
        }

        protected void build(XContentBuilder builder) throws IOException {
            builder.field(INCLUDE_NEGATIVES_FIELD.getPreferredName(), includeNegatives)
                .field(BACKGROUND_IS_SUPERSET.getPreferredName(), backgroundIsSuperset);
        }
    }
}
