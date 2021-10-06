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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Calculates the pseudo Huber loss function.
 *
 * equation: huber = 1/n * Σ(δ^2 * sqrt(1 + a^2 / δ^2) - 1)
 * where: a = y - y´
 *        δ - parameter that controls the steepness
 */
public class HuberMetric implements EvaluationMetric {

    public static final String NAME = LossFunction.HUBER.toString();

    public static final ParseField DELTA = new ParseField("delta");

    private static final ConstructingObjectParser<HuberMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, args -> new HuberMetric((Double) args[0]));

    static {
        PARSER.declareDouble(optionalConstructorArg(), DELTA);
    }

    public static HuberMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Double delta;

    public HuberMetric(@Nullable Double delta) {
        this.delta = delta;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (delta != null) {
            builder.field(DELTA.getPreferredName(), delta);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuberMetric that = (HuberMetric) o;
        return Objects.equals(this.delta, that.delta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delta);
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
