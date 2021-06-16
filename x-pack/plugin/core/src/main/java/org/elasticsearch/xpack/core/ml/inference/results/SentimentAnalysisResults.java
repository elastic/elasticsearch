/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class SentimentAnalysisResults implements InferenceResults {

    public static final String NAME = "sentiment_analysis_result";

    static final String POSITIVE_SCORE = "positive_score";
    static final String NEGATIVE_SCORE = "negative_score";

    private final double positiveScore;
    private final double negativeScore;

    public SentimentAnalysisResults(double positiveScore, double negativeScore) {
        this.positiveScore = positiveScore;
        this.negativeScore = negativeScore;
    }

    public SentimentAnalysisResults(StreamInput in) throws IOException {
        positiveScore = in.readDouble();
        negativeScore = in.readDouble();
    }

    public double getPositiveScore() {
        return positiveScore;
    }

    public double getNegativeScore() {
        return negativeScore;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(POSITIVE_SCORE, positiveScore);
        builder.field(NEGATIVE_SCORE, negativeScore);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(positiveScore);
        out.writeDouble(negativeScore);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(POSITIVE_SCORE, positiveScore);
        map.put(NEGATIVE_SCORE, negativeScore);
        return map;
    }

    @Override
    public Object predictedValue() {
        return positiveScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SentimentAnalysisResults that = (SentimentAnalysisResults) o;
        return Double.compare(that.positiveScore, positiveScore) == 0 &&
            Double.compare(that.negativeScore, negativeScore) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(positiveScore, negativeScore);
    }
}
