/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.search.aggregations.bucket.terms.heuristic;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ChiSquare extends NXYSignificanceHeuristic {
    public static final String NAME = "chi_square";
    public static final ConstructingObjectParser<ChiSquare, Void> PARSER = new ConstructingObjectParser<>(
        NAME, buildFromParsedArgs(ChiSquare::new));
    static {
        NXYSignificanceHeuristic.declareParseFields(PARSER);
    }

    public ChiSquare(boolean includeNegatives, boolean backgroundIsSuperset) {
        super(includeNegatives, backgroundIsSuperset);
    }

    /**
     * Read from a stream.
     */
    public ChiSquare(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean equals(Object other) {
        if ((other instanceof ChiSquare) == false) {
            return false;
        }
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        int result = NAME.hashCode();
        result = 31 * result + super.hashCode();
        return result;
    }

    /**
     * Calculates Chi^2
     * see "Information Retrieval", Manning et al., Eq. 13.19
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        Frequencies frequencies = computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize, "ChiSquare");

        // here we check if the term appears more often in subset than in background without subset.
        if (includeNegatives == false && frequencies.N11 / frequencies.N_1 < frequencies.N10 / frequencies.N_0) {
            return Double.NEGATIVE_INFINITY;
        }
        return (frequencies.N * Math.pow((frequencies.N11 * frequencies.N00 - frequencies.N01 * frequencies.N10), 2.0) /
                ((frequencies.N_1) * (frequencies.N1_) * (frequencies.N0_) * (frequencies.N_0)));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        super.build(builder);
        builder.endObject();
        return builder;
    }

    public static class ChiSquareBuilder extends NXYSignificanceHeuristic.NXYBuilder {
        public ChiSquareBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
            super(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            super.build(builder);
            builder.endObject();
            return builder;
        }
    }
}

