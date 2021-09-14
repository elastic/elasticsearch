/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * {@link AccuracyMetric} is a metric that answers the following two questions:
 *
 *   1. What is the fraction of documents for which predicted class equals the actual class?
 *
 *      equation: overall_accuracy = 1/n * Σ(y == y')
 *      where: n  = total number of documents
 *             y  = document's actual class
 *             y' = document's predicted class
 *
 *   2. For any given class X, what is the fraction of documents for which either
 *       a) both actual and predicted class are equal to X (true positives)
 *      or
 *       b) both actual and predicted class are not equal to X (true negatives)
 *
 *      equation: accuracy(X) = 1/n * (TP(X) + TN(X))
 *      where: X     = class being examined
 *             n     = total number of documents
 *             TP(X) = number of true positives wrt X
 *             TN(X) = number of true negatives wrt X
 */
public class AccuracyMetric implements EvaluationMetric {

    public static final String NAME = "accuracy";

    private static final ObjectParser<AccuracyMetric, Void> PARSER = new ObjectParser<>(NAME, true, AccuracyMetric::new);

    public static AccuracyMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public AccuracyMetric() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
        return Objects.hashCode(NAME);
    }

    public static class Result implements EvaluationMetric.Result {

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField OVERALL_ACCURACY = new ParseField("overall_accuracy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("accuracy_result", true, a -> new Result((List<PerClassSingleValue>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassSingleValue.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), OVERALL_ACCURACY);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassSingleValue> classes;
        /** Fraction of documents for which predicted class equals the actual class. */
        private final double overallAccuracy;

        public Result(List<PerClassSingleValue> classes, double overallAccuracy) {
            this.classes = Collections.unmodifiableList(Objects.requireNonNull(classes));
            this.overallAccuracy = overallAccuracy;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<PerClassSingleValue> getClasses() {
            return classes;
        }

        public double getOverallAccuracy() {
            return overallAccuracy;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASSES.getPreferredName(), classes);
            builder.field(OVERALL_ACCURACY.getPreferredName(), overallAccuracy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.classes, that.classes)
                && this.overallAccuracy == that.overallAccuracy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, overallAccuracy);
        }
    }
}
