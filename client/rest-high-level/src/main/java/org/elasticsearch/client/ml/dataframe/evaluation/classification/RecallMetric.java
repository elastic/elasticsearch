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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * {@link RecallMetric} is a metric that answers the question:
 *   "What fraction of documents belonging to X have been predicted as X by the classifier?"
 * for any given class X
 *
 * equation: recall(X) = TP(X) / (TP(X) + FN(X))
 * where: TP(X) - number of true positives wrt X
 *        FN(X) - number of false negatives wrt X
 */
public class RecallMetric implements EvaluationMetric {

    public static final String NAME = "recall";

    private static final ObjectParser<RecallMetric, Void> PARSER = new ObjectParser<>(NAME, true, RecallMetric::new);

    public static RecallMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RecallMetric() {}

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
        return Objects.hashCode(NAME);
    }

    public static class Result implements EvaluationMetric.Result {

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField AVG_RECALL = new ParseField("avg_recall");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("recall_result", true, a -> new Result((List<PerClassSingleValue>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassSingleValue.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), AVG_RECALL);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassSingleValue> classes;
        /** Average of per-class recalls. */
        private final double avgRecall;

        public Result(List<PerClassSingleValue> classes, double avgRecall) {
            this.classes = Collections.unmodifiableList(Objects.requireNonNull(classes));
            this.avgRecall = avgRecall;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<PerClassSingleValue> getClasses() {
            return classes;
        }

        public double getAvgRecall() {
            return avgRecall;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASSES.getPreferredName(), classes);
            builder.field(AVG_RECALL.getPreferredName(), avgRecall);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.classes, that.classes)
                && this.avgRecall == that.avgRecall;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, avgRecall);
        }
    }
}
