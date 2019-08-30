/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Calculates the mean squared error between two known numerical fields.
 *
 * equation: mse = 1/n * Σ(y - y´)^2
 */
public class MeanSquaredError implements RegressionMetric {

    public static final ParseField NAME = new ParseField("mean_squared_error");

    private static final String PAINLESS_TEMPLATE = "def diff = doc[''{0}''].value - doc[''{1}''].value;return diff * diff;";
    private static final String AGG_NAME = "regression_" + NAME.getPreferredName();

    private static String buildScript(Object...args) {
        return new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args);
    }

    private static final ObjectParser<MeanSquaredError, Void> PARSER =
        new ObjectParser<>("mean_squared_error", true, MeanSquaredError::new);

    public static MeanSquaredError fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MeanSquaredError(StreamInput in) {

    }

    public MeanSquaredError() {

    }

    @Override
    public String getMetricName() {
        return NAME.getPreferredName();
    }

    @Override
    public List<AggregationBuilder> aggs(String actualField, String predictedField) {
        return Collections.singletonList(AggregationBuilders.avg(AGG_NAME).script(new Script(buildScript(actualField, predictedField))));
    }

    @Override
    public EvaluationMetricResult evaluate(Aggregations aggs) {
        NumericMetricsAggregation.SingleValue value = aggs.get(AGG_NAME);
        return value == null ? new Result(0.0) : new Result(value.value());
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        // create static hash code from name as there are currently no unique fields per class instance
        return Objects.hashCode(NAME.getPreferredName());
    }

    public static class Result implements EvaluationMetricResult {

        private static final String ERROR = "error";
        private final double error;

        public Result(double error) {
            this.error = error;
        }

        public Result(StreamInput in) throws IOException {
            this.error = in.readDouble();
        }

        @Override
        public String getWriteableName() {
            return NAME.getPreferredName();
        }

        @Override
        public String getName() {
            return NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(error);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ERROR, error);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result other = (Result)o;
            return error == other.error;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(error);
        }
    }
}
