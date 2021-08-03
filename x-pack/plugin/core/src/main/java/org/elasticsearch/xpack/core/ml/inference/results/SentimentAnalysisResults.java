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

    private final String class1Label;
    private final String class2Label;
    private final double class1Score;
    private final double class2Score;

    public SentimentAnalysisResults(String class1Label, double class1Score,
                                    String class2Label, double class2Score) {
        this.class1Label = class1Label;
        this.class1Score = class1Score;
        this.class2Label = class2Label;
        this.class2Score = class2Score;
    }

    public SentimentAnalysisResults(StreamInput in) throws IOException {
        class1Label = in.readString();
        class1Score = in.readDouble();
        class2Label = in.readString();
        class2Score = in.readDouble();
    }

    public String getClass1Label() {
        return class1Label;
    }

    public double getClass1Score() {
        return class1Score;
    }

    public String getClass2Label() {
        return class2Label;
    }

    public double getClass2Score() {
        return class2Score;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(class1Label, class1Score);
        builder.field(class2Label, class2Score);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(class1Label);
        out.writeDouble(class1Score);
        out.writeString(class2Label);
        out.writeDouble(class2Score);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(class1Label, class1Score);
        map.put(class2Label, class2Score);
        return map;
    }

    @Override
    public Object predictedValue() {
        return class1Score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SentimentAnalysisResults that = (SentimentAnalysisResults) o;
        return Double.compare(that.class1Score, class1Score) == 0 &&
            Double.compare(that.class2Score, class2Score) == 0 &&
            Objects.equals(this.class1Label, that.class1Label) &&
            Objects.equals(this.class2Label, that.class2Label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(class1Label, class1Score, class2Label, class2Score);
    }
}
