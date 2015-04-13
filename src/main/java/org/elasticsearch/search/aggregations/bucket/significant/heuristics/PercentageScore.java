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


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

public class PercentageScore extends SignificanceHeuristic {

    public static final PercentageScore INSTANCE = new PercentageScore();

    protected static final String[] NAMES = {"percentage"};

    private PercentageScore() {};

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            return readFrom(in);
        }

        @Override
        public String getName() {
            return NAMES[0];
        }
    };

    public static SignificanceHeuristic readFrom(StreamInput in) throws IOException {
        return INSTANCE;
    }

    /**
     * Indicates the significance of a term in a sample by determining what percentage
     * of all occurrences of a term are found in the sample. 
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, "PercentageScore");
        if (supersetFreq == 0) {
            // avoid a divide by zero issue
            return 0;
        }        
        return (double) subsetFreq / (double) supersetFreq;
   }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
    }

    public static class PercentageScoreParser implements SignificanceHeuristicParser {

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            // move to the closing bracket
            if (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
                throw new ElasticsearchParseException("expected }, got " + parser.currentName() + " instead in percentage score");
            }
            return new PercentageScore();
        }

        @Override
        public String[] getNames() {
            return NAMES;
        }
    }

    public static class PercentageScoreBuilder implements SignificanceHeuristicBuilder {

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject(STREAM.getName()).endObject();
        }
    }
}

