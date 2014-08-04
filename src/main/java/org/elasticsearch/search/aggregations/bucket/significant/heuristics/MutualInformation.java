/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class MutualInformation extends NXYSignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("mutual_information");

    private static final double log2 = Math.log(2.0);

    public MutualInformation(boolean includeNegatives, boolean backgroundIsSuperset) {
        super(includeNegatives, backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MutualInformation)) {
            return false;
        }
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        int result = NAMES_FIELD.getPreferredName().hashCode();
        result = 31 * result + super.hashCode();
        return result;
    }

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return new MutualInformation(in.readBoolean(), in.readBoolean());
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    /**
     * Calculates mutual information
     * see "Information Retrieval", Manning et al., Eq. 13.17
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        Frequencies frequencies = computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize, "MutualInformation");

        double score = (getMITerm(frequencies.N00, frequencies.N0_, frequencies.N_0, frequencies.N) +
                getMITerm(frequencies.N01, frequencies.N0_, frequencies.N_1, frequencies.N) +
                getMITerm(frequencies.N10, frequencies.N1_, frequencies.N_0, frequencies.N) +
                getMITerm(frequencies.N11, frequencies.N1_, frequencies.N_1, frequencies.N))
                / log2;

        if (Double.isNaN(score)) {
            score = Double.NEGATIVE_INFINITY;
        }
        // here we check if the term appears more often in subset than in background without subset.
        if (!includeNegatives && frequencies.N11 / frequencies.N_1 < frequencies.N10 / frequencies.N_0) {
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

    double getMITerm(double Nxy, double Nx_, double N_y, double N) {
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        super.writeTo(out);
    }

    public static class MutualInformationParser extends NXYParser {

        @Override
        protected SignificanceHeuristic newHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
            return new MutualInformation(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class MutualInformationBuilder extends NXYBuilder {

        public MutualInformationBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
            super(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName());
            super.build(builder);
            builder.endObject();
        }
    }
}

