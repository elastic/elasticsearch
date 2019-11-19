/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link Accuracy} is a metric that answers the question:
 *   "What fraction of examples have been classified correctly by the classifier?"
 *
 * equation: accuracy = 1/n * Σ(y == y´)
 */
public class Accuracy implements ClassificationMetric {

    public static final ParseField NAME = new ParseField("accuracy");

    private static final String PAINLESS_TEMPLATE = "doc[''{0}''].value == doc[''{1}''].value";
    private static final String AGG_NAME = "classification_" + NAME.getPreferredName();

    private static String buildScript(Object...args) {
        return new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args);
    }

    private static final ObjectParser<Accuracy, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Accuracy::new);

    public static Accuracy fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private EvaluationMetricResult result;

    public Accuracy() {}

    public Accuracy(StreamInput in) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public final List<AggregationBuilder> aggs(String actualField, String predictedField) {
        if (result != null) {
            return List.of();
        }
        return List.of(AggregationBuilders.avg(AGG_NAME).script(new Script(buildScript(actualField, predictedField))));
    }

    @Override
    public void process(Aggregations aggs) {
        if ((result == null) && (aggs.get(AGG_NAME) instanceof NumericMetricsAggregation.SingleValue)) {
            NumericMetricsAggregation.SingleValue value = aggs.get(AGG_NAME);
            result = new Result(value.value());
        }
    }

    @Override
    public Optional<EvaluationMetricResult> getResult() {
        return Optional.ofNullable(result);
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
        return Objects.hashCode(NAME.getPreferredName());
    }

    public static class Result implements EvaluationMetricResult {

        private static final String OVERALL_ACCURACY = "overall_accuracy";

        private final double overallAccuracy;

        public Result(double overallAccuracy) {
            this.overallAccuracy = overallAccuracy;
        }

        public Result(StreamInput in) throws IOException {
            this.overallAccuracy = in.readDouble();
        }

        @Override
        public String getWriteableName() {
            return NAME.getPreferredName();
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public double getOverallAccuracy() {
            return overallAccuracy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(overallAccuracy);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(OVERALL_ACCURACY, overallAccuracy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return this.overallAccuracy == that.overallAccuracy;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(overallAccuracy);
        }
    }
}
