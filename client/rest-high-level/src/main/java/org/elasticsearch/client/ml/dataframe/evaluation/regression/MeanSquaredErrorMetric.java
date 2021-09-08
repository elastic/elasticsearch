/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.regression;

import org.elasticsearch.client.ml.dataframe.Regression.LossFunction;
import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Calculates the mean squared error between two known numerical fields.
 *
 * equation: mse = 1/n * Σ(y - y´)^2
 */
public class MeanSquaredErrorMetric implements EvaluationMetric {

    public static final String NAME = LossFunction.MSE.toString();

    private static final ObjectParser<MeanSquaredErrorMetric, Void> PARSER = new ObjectParser<>(NAME, true, MeanSquaredErrorMetric::new);

    public static MeanSquaredErrorMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MeanSquaredErrorMetric() {}

    @Override
    public String getName() {
        return NAME;
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
        return Objects.hashCode(NAME);
    }

    public static class Result implements EvaluationMetric.Result {

        public static final ParseField VALUE = new ParseField("value");
        private final double value;

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(NAME + "_result", true, args -> new Result((double) args[0]));

        static {
            PARSER.declareDouble(constructorArg(), VALUE);
        }

        public Result(double value) {
            this.value = value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(VALUE.getPreferredName(), value);
            builder.endObject();
            return builder;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return this.value == that.value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }
    }
}
