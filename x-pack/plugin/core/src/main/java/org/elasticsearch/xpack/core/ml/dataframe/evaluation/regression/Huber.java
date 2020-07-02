/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression.LossFunction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Calculates the pseudo Huber loss function.
 *
 * equation: huber = 1/n * Σ(δ^2 * sqrt(1 + a^2 / δ^2) - 1)
 * where: a = y - y´
 *        δ - parameter that controls the steepness
 */
public class Huber implements EvaluationMetric {

    public static final ParseField NAME = new ParseField(LossFunction.HUBER.toString());

    public static final ParseField DELTA = new ParseField("delta");
    private static final double DEFAULT_DELTA = 1.0;

    private static final String PAINLESS_TEMPLATE =
        "def a = doc[''{0}''].value - doc[''{1}''].value;" +
        "def delta2 = {2};" +
        "return delta2 * (Math.sqrt(1.0 + Math.pow(a, 2) / delta2) - 1.0);";
    private static final String AGG_NAME = "regression_" + NAME.getPreferredName();

    private static String buildScript(Object...args) {
        return new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args);
    }

    private static final ConstructingObjectParser<Huber, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(), true, args -> new Huber((Double) args[0]));

    static {
        PARSER.declareDouble(optionalConstructorArg(), DELTA);
    }

    public static Huber fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final double delta;
    private EvaluationMetricResult result;

    public Huber(StreamInput in) throws IOException {
        this.delta = in.readDouble();
    }

    public Huber(@Nullable Double delta) {
        this.delta = delta != null ? delta : DEFAULT_DELTA;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                  String actualField,
                                                                                  String predictedField) {
        if (result != null) {
            return Tuple.tuple(Collections.emptyList(), Collections.emptyList());
        }
        return Tuple.tuple(
            Arrays.asList(AggregationBuilders.avg(AGG_NAME).script(new Script(buildScript(actualField, predictedField, delta * delta)))),
            Collections.emptyList());
    }

    @Override
    public void process(Aggregations aggs) {
        NumericMetricsAggregation.SingleValue value = aggs.get(AGG_NAME);
        result = value == null ? new Result(0.0) : new Result(value.value());
    }

    @Override
    public Optional<EvaluationMetricResult> getResult() {
        return Optional.ofNullable(result);
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(Regression.NAME, NAME);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(delta);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DELTA.getPreferredName(), delta);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Huber that = (Huber) o;
        return this.delta == that.delta;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(delta);
    }

    public static class Result implements EvaluationMetricResult {

        private static final String VALUE = "value";
        private final double value;

        public Result(double value) {
            this.value = value;
        }

        public Result(StreamInput in) throws IOException {
            this.value = in.readDouble();
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Regression.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public double getValue() {
            return value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(VALUE, value);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result other = (Result)o;
            return value == other.value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }
    }
}
