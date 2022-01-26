/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression.LossFunction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
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
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Calculates the mean squared error between two known numerical fields.
 *
 * equation: msle = 1/n * Σ(log(y + offset) - log(y´ + offset))^2
 * where offset is used to make sure the argument to log function is always positive
 */
public class MeanSquaredLogarithmicError implements EvaluationMetric {

    public static final ParseField NAME = new ParseField(LossFunction.MSLE.toString());

    public static final ParseField OFFSET = new ParseField("offset");
    private static final double DEFAULT_OFFSET = 1.0;

    private static final String PAINLESS_TEMPLATE = "def offset = {2};"
        + "def diff = Math.log(doc[''{0}''].value + offset) - Math.log(doc[''{1}''].value + offset);"
        + "return diff * diff;";
    private static final String AGG_NAME = "regression_" + NAME.getPreferredName();

    private static String buildScript(Object... args) {
        return new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args);
    }

    private static final ConstructingObjectParser<MeanSquaredLogarithmicError, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        true,
        args -> new MeanSquaredLogarithmicError((Double) args[0])
    );

    static {
        PARSER.declareDouble(optionalConstructorArg(), OFFSET);
    }

    public static MeanSquaredLogarithmicError fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final double offset;
    private EvaluationMetricResult result;

    public MeanSquaredLogarithmicError(StreamInput in) throws IOException {
        this.offset = in.readDouble();
    }

    public MeanSquaredLogarithmicError(@Nullable Double offset) {
        this.offset = offset != null ? offset : DEFAULT_OFFSET;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
    }

    @Override
    public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(
        EvaluationParameters parameters,
        EvaluationFields fields
    ) {
        if (result != null) {
            return Tuple.tuple(Collections.emptyList(), Collections.emptyList());
        }
        String actualField = fields.getActualField();
        String predictedField = fields.getPredictedField();
        return Tuple.tuple(
            Arrays.asList(AggregationBuilders.avg(AGG_NAME).script(new Script(buildScript(actualField, predictedField, offset)))),
            Collections.emptyList()
        );
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
        out.writeDouble(offset);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(OFFSET.getPreferredName(), offset);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeanSquaredLogarithmicError that = (MeanSquaredLogarithmicError) o;
        return this.offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(offset);
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
            Result other = (Result) o;
            return value == other.value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }
    }
}
