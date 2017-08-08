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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;

public class PercentageScore extends SignificanceHeuristic {
    public static final String NAME = "percentage";

    private static final ParseField MAX_SCORE = new ParseField("max_score");
    private static final ParseField MIN_SCORE = new ParseField("min_score");
    private final double maxScore;
    private final double minScore;

    public PercentageScore(double minScore,double maxScore) {
        this.minScore = minScore;
        this.maxScore = maxScore;
    }

    public PercentageScore() {
        this(0,1);
    }
    public PercentageScore(StreamInput in) throws  IOException{
        minScore = in.readLong();
        maxScore = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(minScore);
        out.writeDouble(maxScore);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME)
            .field(MIN_SCORE.getPreferredName(), minScore)
            .field(MAX_SCORE.getPreferredName(), maxScore)
            .endObject();
        return builder;
    }

    public double getMinScore() {
        return minScore;
    }

    public double getMaxScore() {
        return  maxScore;
    }

    public static SignificanceHeuristic parse(XContentParser parser)
            throws IOException, QueryShardException {
        double minScore = 0;
        double maxScore  = 1;
        XContentParser.Token token = parser.nextToken();
        while (!token.equals(XContentParser.Token.END_OBJECT)) {
            if (MIN_SCORE.match(parser.currentName())) {
                parser.nextToken();
                minScore = parser.doubleValue();
            } else if (MAX_SCORE.match(parser.currentName())) {
                parser.nextToken();
                maxScore = parser.doubleValue();
            } else {
                throw new ElasticsearchParseException("failed to parse percent heuristic. unknown field [{}]", parser.currentName());
            }
            token = parser.nextToken();
        }
        return new PercentageScore(minScore,maxScore);
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
        double score =  (double) subsetFreq / (double) supersetFreq;
        if ( score >= minScore && score <= maxScore) {
            return score;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    public static class PercentageScoreBuilder implements SignificanceHeuristicBuilder {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).endObject();
            return builder;
        }
    }
}

