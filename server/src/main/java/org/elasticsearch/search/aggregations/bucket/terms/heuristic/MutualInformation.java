/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class MutualInformation extends NXYSignificanceHeuristic {
    public static final String NAME = "mutual_information";
    public static final ConstructingObjectParser<MutualInformation, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        buildFromParsedArgs(MutualInformation::new)
    );
    static {
        NXYSignificanceHeuristic.declareParseFields(PARSER);
    }

    private static final double log2 = Math.log(2.0);

    public MutualInformation(boolean includeNegatives, boolean backgroundIsSuperset) {
        super(includeNegatives, backgroundIsSuperset);
    }

    /**
     * Read from a stream.
     */
    public MutualInformation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean equals(Object other) {
        if ((other instanceof MutualInformation) == false) {
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
     * Calculates mutual information
     * see "Information Retrieval", Manning et al., Eq. 13.17
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        Frequencies frequencies = computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize, "MutualInformation");

        double score = (getMITerm(frequencies.N00, frequencies.N0_, frequencies.N_0, frequencies.N) + getMITerm(
            frequencies.N01,
            frequencies.N0_,
            frequencies.N_1,
            frequencies.N
        ) + getMITerm(frequencies.N10, frequencies.N1_, frequencies.N_0, frequencies.N) + getMITerm(
            frequencies.N11,
            frequencies.N1_,
            frequencies.N_1,
            frequencies.N
        )) / log2;

        if (Double.isNaN(score)) {
            score = Double.NEGATIVE_INFINITY;
        }
        // here we check if the term appears more often in subset than in background without subset.
        if (includeNegatives == false && frequencies.N11 / frequencies.N_1 < frequencies.N10 / frequencies.N_0) {
            score = Double.NEGATIVE_INFINITY;
        }
        return score;
    }

    /*  make sure that
        0 * log(0/0) = 0
        0 * log(0) = 0
        Else, this would be the score:
        double score =
                  N11 / N * Math.log((N * N11) / (N1_ * N_1))
                + N01 / N * Math.log((N * N01) / (N0_ * N_1))
                + N10 / N * Math.log((N * N10) / (N1_ * N_0))
                + N00 / N * Math.log((N * N00) / (N0_ * N_0));

        but we get many NaN if we do not take case of the 0s */

    static double getMITerm(double Nxy, double Nx_, double N_y, double N) {
        double numerator = Math.abs(N * Nxy);
        double denominator = Math.abs(Nx_ * N_y);
        double factor = Math.abs(Nxy / N);
        if (numerator < 1.e-7 && factor < 1.e-7) {
            return 0.0;
        } else {
            return factor * Math.log(numerator / denominator);
        }
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

    public static class MutualInformationBuilder extends NXYBuilder {
        public MutualInformationBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
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
