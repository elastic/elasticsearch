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
import org.elasticsearch.common.xcontent.ToXContentObject;
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
            new ConstructingObjectParser<>("recall_result", true, a -> new Result((List<PerClassResult>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassResult.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), AVG_RECALL);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassResult> classes;
        /** Average of per-class recalls. */
        private final double avgRecall;

        public Result(List<PerClassResult> classes, double avgRecall) {
            this.classes = Collections.unmodifiableList(Objects.requireNonNull(classes));
            this.avgRecall = avgRecall;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<PerClassResult> getClasses() {
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

    public static class PerClassResult implements ToXContentObject {

        private static final ParseField CLASS_NAME = new ParseField("class_name");
        private static final ParseField RECALL = new ParseField("recall");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PerClassResult, Void> PARSER =
            new ConstructingObjectParser<>("recall_per_class_result", true, a -> new PerClassResult((String) a[0], (double) a[1]));

        static {
            PARSER.declareString(constructorArg(), CLASS_NAME);
            PARSER.declareDouble(constructorArg(), RECALL);
        }

        /** Name of the class. */
        private final String className;
        /** Fraction of documents actually belonging to the {@code actualClass} class predicted correctly. */
        private final double recall;

        public PerClassResult(String className, double recall) {
            this.className = Objects.requireNonNull(className);
            this.recall = recall;
        }

        public String getClassName() {
            return className;
        }

        public double getRecall() {
            return recall;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME.getPreferredName(), className);
            builder.field(RECALL.getPreferredName(), recall);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PerClassResult that = (PerClassResult) o;
            return Objects.equals(this.className, that.className)
                && this.recall == that.recall;
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, recall);
        }
    }
}
