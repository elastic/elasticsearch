/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PercentageScore extends SignificanceHeuristic {
    public static final String NAME = "percentage";
    public static final ObjectParser<PercentageScore, Void> PARSER = new ObjectParser<>(NAME, PercentageScore::new);

    public PercentageScore() {}

    public PercentageScore(StreamInput in) {
        // Nothing to read.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
        return builder;
    }

    public static SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryShardException {
        // move to the closing bracket
        if (parser.nextToken().equals(XContentParser.Token.END_OBJECT) == false) {
            throw new ElasticsearchParseException(
                "failed to parse [percentage] significance heuristic. expected an empty object, " + "but got [{}] instead",
                parser.currentToken()
            );
        }
        return new PercentageScore();
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
