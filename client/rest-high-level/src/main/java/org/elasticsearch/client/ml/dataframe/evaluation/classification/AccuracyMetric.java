/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * {@link AccuracyMetric} is a metric that answers the question:
 *   "What fraction of documents have been classified correctly by the classifier?"
 *
 * equation: accuracy = 1/n * Σ(y == y´)
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
            new ConstructingObjectParser<>("accuracy_result", true, a -> new Result((List<PerClassResult>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassResult.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), OVERALL_ACCURACY);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassResult> classes;
        /** Fraction of documents predicted correctly. */
        private final double overallAccuracy;

        public Result(List<PerClassResult> classes, double overallAccuracy) {
            this.classes = Collections.unmodifiableList(Objects.requireNonNull(classes));
            this.overallAccuracy = overallAccuracy;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<PerClassResult> getClasses() {
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

    public static class PerClassResult implements ToXContentObject {

        private static final ParseField CLASS_NAME = new ParseField("class_name");
        private static final ParseField ACTUAL_CLASS_DOC_COUNT = new ParseField("actual_class_doc_count");
        private static final ParseField ACCURACY = new ParseField("accuracy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PerClassResult, Void> PARSER =
            new ConstructingObjectParser<>(
                "accuracy_per_class_result", true, a -> new PerClassResult((String) a[0], (long) a[1], (double) a[2]));

        static {
            PARSER.declareString(constructorArg(), CLASS_NAME);
            PARSER.declareLong(constructorArg(), ACTUAL_CLASS_DOC_COUNT);
            PARSER.declareDouble(constructorArg(), ACCURACY);
        }

        /** Name of the class. */
        private final String className;
        /** Number of documents actually belonging to the {@code className} class. */
        private final long actualClassDocCount;
        /** Fraction of documents actually belonging to the {@code className} class predicted correctly. */
        private final double accuracy;

        public PerClassResult(String className, long actualClassDocCount, double accuracy) {
            this.className = Objects.requireNonNull(className);
            this.actualClassDocCount = actualClassDocCount;
            this.accuracy = accuracy;
        }

        public String getClassName() {
            return className;
        }

        public long getActualClassDocCount() {
            return actualClassDocCount;
        }

        public double getAccuracy() {
            return accuracy;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME.getPreferredName(), className);
            builder.field(ACTUAL_CLASS_DOC_COUNT.getPreferredName(), actualClassDocCount);
            builder.field(ACCURACY.getPreferredName(), accuracy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PerClassResult that = (PerClassResult) o;
            return Objects.equals(this.className, that.className)
                && this.actualClassDocCount == that.actualClassDocCount
                && this.accuracy == that.accuracy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, actualClassDocCount, accuracy);
        }
    }
}
