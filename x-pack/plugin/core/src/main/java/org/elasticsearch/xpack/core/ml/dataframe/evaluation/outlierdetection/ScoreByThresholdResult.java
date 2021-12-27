/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

public class ScoreByThresholdResult implements EvaluationMetricResult {

    public static final ParseField NAME = new ParseField("score_by_threshold_result");

    private final String name;
    private final double[] thresholds;
    private final double[] scores;

    public ScoreByThresholdResult(String name, double[] thresholds, double[] scores) {
        assert thresholds.length == scores.length;
        this.name = Objects.requireNonNull(name);
        this.thresholds = thresholds;
        this.scores = scores;
    }

    public ScoreByThresholdResult(StreamInput in) throws IOException {
        this.name = in.readString();
        this.thresholds = in.readDoubleArray();
        this.scores = in.readDoubleArray();
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(OutlierDetection.NAME, NAME);
    }

    @Override
    public String getMetricName() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeDoubleArray(thresholds);
        out.writeDoubleArray(scores);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (int i = 0; i < thresholds.length; i++) {
            builder.field(String.valueOf(thresholds[i]), scores[i]);
        }
        builder.endObject();
        return builder;
    }
}
