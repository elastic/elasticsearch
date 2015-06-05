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

public class ChiSquare extends NXYSignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("chi_square");

    public ChiSquare(boolean includeNegatives, boolean backgroundIsSuperset) {
        super(includeNegatives, backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ChiSquare)) {
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
            return new ChiSquare(in.readBoolean(), in.readBoolean());
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    /**
     * Calculates Chi^2
     * see "Information Retrieval", Manning et al., Eq. 13.19
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        Frequencies frequencies = computeNxys(subsetFreq, subsetSize, supersetFreq, supersetSize, "ChiSquare");

        // here we check if the term appears more often in subset than in background without subset.
        if (!includeNegatives && frequencies.N11 / frequencies.N_1 < frequencies.N10 / frequencies.N_0) {
            return Double.NEGATIVE_INFINITY;
        }
        return (frequencies.N * Math.pow((frequencies.N11 * frequencies.N00 - frequencies.N01 * frequencies.N10), 2.0) /
                ((frequencies.N_1) * (frequencies.N1_) * (frequencies.N0_) * (frequencies.N_0)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        super.writeTo(out);
    }

    public static class ChiSquareParser extends NXYParser {

        @Override
        protected SignificanceHeuristic newHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
            return new ChiSquare(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class ChiSquareBuilder extends NXYSignificanceHeuristic.NXYBuilder {

        public ChiSquareBuilder(boolean includeNegatives, boolean backgroundIsSuperset) {
            super(includeNegatives, backgroundIsSuperset);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(STREAM.getName());
            super.build(builder);
            builder.endObject();
            return builder;
        }
    }
}

