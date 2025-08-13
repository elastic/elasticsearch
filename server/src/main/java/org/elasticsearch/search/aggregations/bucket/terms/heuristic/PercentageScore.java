/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

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
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}
