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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public class GND implements SignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("gnd");

    public static final GND INSTANCE = new GND();

    private GND() {
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GND)) {
            return false;
        }
        return true;
    }

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return INSTANCE;
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    /**
     * Calculates Google Normalized Distance, as described in "The Google Similarity Distance", Cilibrasi and Vitanyi, 2007
     * link: http://arxiv.org/pdf/cs/0412098v3.pdf
     *
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {

        if (subsetFreq < 0 || subsetSize < 0 || supersetFreq < 0 || supersetSize < 0) {
            throw new ElasticsearchIllegalArgumentException("Frequencies of subset and superset must be positive in GND.getScore()");
        }
        if (subsetFreq > subsetSize) {
            throw new ElasticsearchIllegalArgumentException("subsetFreq > subsetSize, in GND.score(..)");
        }
        if (supersetFreq > supersetSize) {
            throw new ElasticsearchIllegalArgumentException("supersetFreq > supersetSize, in GND.score(..)");
        }

        double fx = subsetSize;
        double fy = supersetFreq;
        double fxy = subsetFreq;
        double N = supersetSize;

        double score = (Math.max(Math.log(fx), Math.log(fy)) - Math.log(fxy))
                / (Math.log(N) - Math.min(Math.log(fx), Math.log(fy)));

        //we must invert the order of terms because GND scores relevant terms low
        score = Math.exp(-1.0d * score);
        return score;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
    }

    public static class GNDParser implements SignificanceHeuristicParser {

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            NAMES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS);
            // move to the closing bracket
            if (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
                throw new ElasticsearchParseException("expected }, got " + parser.currentName() + " instead in gnd score");
            }
            return new GND();
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class GNDBuilder implements SignificanceHeuristicBuilder {

        public GNDBuilder() {
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName())
                    .endObject();
        }
    }
}

