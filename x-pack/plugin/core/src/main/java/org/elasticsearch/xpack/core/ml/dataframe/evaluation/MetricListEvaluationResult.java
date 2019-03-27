/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MetricListEvaluationResult implements EvaluationResult {

    public static final String WRITEABLE_NAME = "metric_list_evaluation_result";

    private final String name;
    private final List<EvaluationMetricResult> metrics;

    public MetricListEvaluationResult(String name, List<EvaluationMetricResult> metrics) {
        this.name = Objects.requireNonNull(name);
        this.metrics = Objects.requireNonNull(metrics);
    }

    public MetricListEvaluationResult(StreamInput in) throws IOException {
        this.name = in.readString();
        this.metrics = in.readNamedWriteableList(EvaluationMetricResult.class);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (EvaluationMetricResult metric : metrics) {
            builder.field(metric.getName(), metric);
        }
        builder.endObject();
        return builder;
    }
}
