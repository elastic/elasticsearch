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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * {@link PrecisionMetric} is a metric that answers the question:
 *   "What fraction of documents classified as X actually belongs to X?"
 * for any given class X
 *
 * equation: precision(X) = TP(X) / (TP(X) + FP(X))
 * where: TP(X) - number of true positives wrt X
 *        FP(X) - number of false positives wrt X
 */
public class PrecisionMetric implements EvaluationMetric {

    public static final String NAME = "precision";

    public static final ParseField SIZE = new ParseField("size");

    private static final ConstructingObjectParser<PrecisionMetric, Void> PARSER = createParser();

    private static ConstructingObjectParser<PrecisionMetric, Void> createParser() {
        ConstructingObjectParser<PrecisionMetric, Void>  parser =
            new ConstructingObjectParser<>(NAME, true, args -> new PrecisionMetric((Integer) args[0]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        return parser;
    }

    public static PrecisionMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Integer size;

    public PrecisionMetric() {
        this(null);
    }

    public PrecisionMetric(@Nullable Integer size) {
        this.size = size;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrecisionMetric that = (PrecisionMetric) o;
        return Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }

    public static class Result implements EvaluationMetric.Result {

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField AVG_PRECISION = new ParseField("avg_precision");
        private static final ParseField OTHER_CLASS_COUNT = new ParseField("other_class_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(
                "precision_result", true, a -> new Result((List<PerClassResult>) a[0], (double) a[1], (long) a[2]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassResult.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), AVG_PRECISION);
            PARSER.declareLong(constructorArg(), OTHER_CLASS_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassResult> classes;
        /** Average of per-class precisions. */
        private final double avgPrecision;
        /** Number of classes that were not included in the per-class results because there were too many of them. */
        private final long otherClassCount;

        public Result(List<PerClassResult> classes, double avgPrecision, long otherClassCount) {
            this.classes = Collections.unmodifiableList(Objects.requireNonNull(classes));
            this.avgPrecision = avgPrecision;
            this.otherClassCount = otherClassCount;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<PerClassResult> getClasses() {
            return classes;
        }

        public double getAvgPrecision() {
            return avgPrecision;
        }

        public long getOtherClassCount() {
            return otherClassCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASSES.getPreferredName(), classes);
            builder.field(AVG_PRECISION.getPreferredName(), avgPrecision);
            builder.field(OTHER_CLASS_COUNT.getPreferredName(), otherClassCount);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.classes, that.classes)
                && this.avgPrecision == that.avgPrecision
                && this.otherClassCount == that.otherClassCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, avgPrecision, otherClassCount);
        }
    }

    public static class PerClassResult implements ToXContentObject {

        private static final ParseField CLASS_NAME = new ParseField("class_name");
        private static final ParseField PRECISION = new ParseField("precision");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PerClassResult, Void> PARSER =
            new ConstructingObjectParser<>("precision_per_class_result", true, a -> new PerClassResult((String) a[0], (double) a[1]));

        static {
            PARSER.declareString(constructorArg(), CLASS_NAME);
            PARSER.declareDouble(constructorArg(), PRECISION);
        }

        /** Name of the class. */
        private final String className;
        /** Fraction of documents predicted as belonging to the {@code predictedClass} class predicted correctly. */
        private final double precision;

        public PerClassResult(String className, double precision) {
            this.className = Objects.requireNonNull(className);
            this.precision = precision;
        }

        public String getClassName() {
            return className;
        }

        public double getPrecision() {
            return precision;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME.getPreferredName(), className);
            builder.field(PRECISION.getPreferredName(), precision);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PerClassResult that = (PerClassResult) o;
            return Objects.equals(this.className, that.className)
                && this.precision == that.precision;
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, precision);
        }
    }
}
