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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.EnumSet;

public class MutualInformation implements SignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("mutual_information");

    protected static final ParseField INCLUDE_NEGATIVES_FIELD = new ParseField("include_negatives");

    /**
     * Mutual information does not differentiate between terms that are descriptive for subset or for
     * the background without the subset. We might want to filter out the terms that are appear much less often
     * in the subset than in the background without the subset.
     */
    protected boolean includeNegatives = false;

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof MutualInformation)) {
            return false;
        }
        return ((MutualInformation)other).includeNegatives == includeNegatives;
    }

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return new MutualInformation().setIncludeNegatives(in.readBoolean());
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    /**
     * Calculates mutual information
     * see "Information Retrieval", Manning et al., Eq. 13.17
     *
     * @param subsetDf     The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetDf   The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetDf, long subsetSize, long supersetDf, long supersetSize) {


        //documents not in class and do not contain term
        double N00 = supersetSize - supersetDf - (subsetSize - subsetDf);
        //documents in class and do not contain term
        double N01 = (subsetSize - subsetDf);
        // documents not in class and do contain term
        double N10 = supersetDf - subsetDf;
        // documents in class and do contain term
        double N11 = subsetDf;
        //documents that do not contain term
        double N0_ = supersetSize - supersetDf;
        //documents that contain term
        double N1_ = supersetDf;
        //documents that are not in class
        double N_0 = supersetSize - subsetSize;
        //documents that are in class
        double N_1 = subsetSize;
        //all docs
        double N = supersetSize;

        double score = getMITerm(N00, N0_, N_0, N) +
                getMITerm(N01, N0_, N_1, N) +
                getMITerm(N10, N1_, N_0, N) +
                getMITerm(N11, N1_, N_1, N);

        if (Double.isNaN(score)) {
            score = -1.0 * Float.MAX_VALUE;
        }
        // here we check if the term appears more often in subset than in background without subset.
        if (!includeNegatives && N11 / N_1 < N10 / N_0) {
            score = -1.0 * Float.MAX_VALUE;
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
            return 0;
        } else {
            return factor * Math.log(numerator / denominator);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        out.writeBoolean(includeNegatives);

    }

    public MutualInformation setIncludeNegatives(boolean includeNegatives) {
        this.includeNegatives = includeNegatives;
        return this;
    }

    public boolean getIncludeNegatives() {
        return includeNegatives;
    }

    public static class MutualInformationParser implements SignificanceHeuristicParser {

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            NAMES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS);
            boolean includeNegatives = false;
            XContentParser.Token token = parser.nextToken();
            if (!token.equals(XContentParser.Token.END_OBJECT)) {
                if (INCLUDE_NEGATIVES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS)) {
                    parser.nextToken();
                    includeNegatives = parser.booleanValue();
                }
            }
            // move to the closing bracket
            parser.nextToken();
            return new MutualInformation().setIncludeNegatives(includeNegatives);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class MutualInformationBuilder implements SignificanceHeuristicBuilder {

        boolean includeNegatives = true;

        public MutualInformationBuilder setIncludeNegatives(boolean includeNegatives) {
            this.includeNegatives = includeNegatives;
            return this;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName()).field(INCLUDE_NEGATIVES_FIELD.getPreferredName(), includeNegatives).endObject();
        }
    }
}

