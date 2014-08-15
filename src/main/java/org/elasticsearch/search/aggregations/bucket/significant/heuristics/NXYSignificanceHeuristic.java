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


import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public abstract class NXYSignificanceHeuristic extends SignificanceHeuristic {

    protected static final ParseField BACKGROUND_IS_SUPERSET = new ParseField("background_is_superset");

    protected static final ParseField INCLUDE_NEGATIVES_FIELD = new ParseField("include_negatives");

    protected static final String SCORE_ERROR_MESSAGE = ", does your background filter not include all documents in the bucket? If so and it is intentional, set \"" + BACKGROUND_IS_SUPERSET.getPreferredName() + "\": false";

    protected final boolean backgroundIsSuperset;

    /**
     * Some heuristics do not differentiate between terms that are descriptive for subset or for
     * the background without the subset. We might want to filter out the terms that are appear much less often
     * in the subset than in the background without the subset.
     */
    protected final boolean includeNegatives;

    public NXYSignificanceHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
        this.includeNegatives = includeNegatives;
        this.backgroundIsSuperset = backgroundIsSuperset;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeNegatives);
        out.writeBoolean(backgroundIsSuperset);
    }

    @Override
    public boolean equals(Object other) {
        return ((NXYSignificanceHeuristic) other).includeNegatives == includeNegatives && ((NXYSignificanceHeuristic) other).backgroundIsSuperset == backgroundIsSuperset;
    }

    @Override
    public int hashCode() {
        int result = (includeNegatives ? 1 : 0);
        result = 31 * result + (backgroundIsSuperset ? 1 : 0);
        return result;
    }

    protected static class Frequencies {
        double N00, N01, N10, N11, N0_, N1_, N_0, N_1, N;
    }

    protected Frequencies computeNxys(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencies(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        Frequencies frequencies = new Frequencies();
        if (backgroundIsSuperset) {
            //documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq - (subsetSize - subsetFreq);
            //documents in class and do not contain term
            frequencies.N01 = (subsetSize - subsetFreq);
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq - subsetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            //documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq;
            //documents that contain term
            frequencies.N1_ = supersetFreq;
            //documents that are not in class
            frequencies.N_0 = supersetSize - subsetSize;
            //documents that are in class
            frequencies.N_1 = subsetSize;
            //all docs
            frequencies.N = supersetSize;
        } else {
            //documents not in class and do not contain term
            frequencies.N00 = supersetSize - supersetFreq;
            //documents in class and do not contain term
            frequencies.N01 = subsetSize - subsetFreq;
            // documents not in class and do contain term
            frequencies.N10 = supersetFreq;
            // documents in class and do contain term
            frequencies.N11 = subsetFreq;
            //documents that do not contain term
            frequencies.N0_ = supersetSize - supersetFreq + subsetSize - subsetFreq;
            //documents that contain term
            frequencies.N1_ = supersetFreq + subsetFreq;
            //documents that are not in class
            frequencies.N_0 = supersetSize;
            //documents that are in class
            frequencies.N_1 = subsetSize;
            //all docs
            frequencies.N = supersetSize + subsetSize;
        }
        return frequencies;
    }

    protected void checkFrequencies(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize, String scoreFunctionName) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, scoreFunctionName);
        if (backgroundIsSuperset) {
            if (subsetFreq > supersetFreq) {
                throw new ElasticsearchIllegalArgumentException("subsetFreq > supersetFreq" + SCORE_ERROR_MESSAGE);
            }
            if (subsetSize > supersetSize) {
                throw new ElasticsearchIllegalArgumentException("subsetSize > supersetSize" + SCORE_ERROR_MESSAGE);
            }
            if (supersetFreq - subsetFreq > supersetSize - subsetSize) {
                throw new ElasticsearchIllegalArgumentException("supersetFreq - subsetFreq > supersetSize - subsetSize" + SCORE_ERROR_MESSAGE);
            }
        }
    }

    public static abstract class NXYParser implements SignificanceHeuristicParser {

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            String givenName = parser.currentName();
            boolean includeNegatives = false;
            boolean backgroundIsSuperset = true;
            XContentParser.Token token = parser.nextToken();
            while (!token.equals(XContentParser.Token.END_OBJECT)) {
                if (INCLUDE_NEGATIVES_FIELD.match(parser.currentName())) {
                    parser.nextToken();
                    includeNegatives = parser.booleanValue();
                } else if (BACKGROUND_IS_SUPERSET.match(parser.currentName())) {
                    parser.nextToken();
                    backgroundIsSuperset = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException("Field " + parser.currentName().toString() + " unknown for " + givenName);
                }
                token = parser.nextToken();
            }
            return newHeuristic(includeNegatives, backgroundIsSuperset);
        }

        protected abstract SignificanceHeuristic newHeuristic(boolean includeNegatives, boolean backgroundIsSuperset);
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

