/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * {@link Accuracy} is a metric that answers the question:
 *   "What fraction of examples have been classified correctly by the classifier?"
 *
 * equation: accuracy = 1/n * Σ(y == y´)
 */
public class Accuracy implements ClassificationMetric {

    public static final ParseField NAME = new ParseField("accuracy");

    private static final String PAINLESS_TEMPLATE = "doc[''{0}''].value == doc[''{1}''].value";
    private static final String CLASSES_AGG_NAME = "classification_classes";
    private static final String PER_CLASS_ACCURACY_AGG_NAME = "classification_per_class_accuracy";
    private static final String OVERALL_ACCURACY_AGG_NAME = "classification_overall_accuracy";

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
        Script accuracyScript = new Script(buildScript(actualField, predictedField));
        return List.of(
            AggregationBuilders.terms(CLASSES_AGG_NAME)
                .field(actualField)
                .subAggregation(AggregationBuilders.avg(PER_CLASS_ACCURACY_AGG_NAME).script(accuracyScript)),
            AggregationBuilders.avg(OVERALL_ACCURACY_AGG_NAME).script(accuracyScript));
    }

    @Override
    public void process(Aggregations aggs) {
        if (result != null) {
            return;
        }
        Terms classesAgg = aggs.get(CLASSES_AGG_NAME);
        NumericMetricsAggregation.SingleValue overallAccuracyAgg = aggs.get(OVERALL_ACCURACY_AGG_NAME);
        List<ActualClass> actualClasses = new ArrayList<>(classesAgg.getBuckets().size());
        for (Terms.Bucket bucket : classesAgg.getBuckets()) {
            String actualClass = bucket.getKeyAsString();
            long actualClassDocCount = bucket.getDocCount();
            NumericMetricsAggregation.SingleValue accuracyAgg = bucket.getAggregations().get(PER_CLASS_ACCURACY_AGG_NAME);
            actualClasses.add(new ActualClass(actualClass, actualClassDocCount, accuracyAgg.value()));
        }
        result = new Result(actualClasses, overallAccuracyAgg.value());
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

        private static final ParseField ACTUAL_CLASSES = new ParseField("actual_classes");
        private static final ParseField OVERALL_ACCURACY = new ParseField("overall_accuracy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("accuracy_result", true, a -> new Result((List<ActualClass>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), ActualClass.PARSER, ACTUAL_CLASSES);
            PARSER.declareDouble(constructorArg(), OVERALL_ACCURACY);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of actual classes. */
        private final List<ActualClass> actualClasses;
        /** Fraction of documents predicted correctly. */
        private final double overallAccuracy;

        public Result(List<ActualClass> actualClasses, double overallAccuracy) {
            this.actualClasses = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(actualClasses, ACTUAL_CLASSES));
            this.overallAccuracy = overallAccuracy;
        }

        public Result(StreamInput in) throws IOException {
            this.actualClasses = Collections.unmodifiableList(in.readList(ActualClass::new));
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

        public List<ActualClass> getActualClasses() {
            return actualClasses;
        }

        public double getOverallAccuracy() {
            return overallAccuracy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(actualClasses);
            out.writeDouble(overallAccuracy);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACTUAL_CLASSES.getPreferredName(), actualClasses);
            builder.field(OVERALL_ACCURACY.getPreferredName(), overallAccuracy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.actualClasses, that.actualClasses)
                && this.overallAccuracy == that.overallAccuracy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualClasses, overallAccuracy);
        }
    }

    public static class ActualClass implements ToXContentObject, Writeable {

        private static final ParseField ACTUAL_CLASS = new ParseField("actual_class");
        private static final ParseField ACTUAL_CLASS_DOC_COUNT = new ParseField("actual_class_doc_count");
        private static final ParseField ACCURACY = new ParseField("accuracy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ActualClass, Void> PARSER =
            new ConstructingObjectParser<>("accuracy_actual_class", true, a -> new ActualClass((String) a[0], (long) a[1], (double) a[2]));

        static {
            PARSER.declareString(constructorArg(), ACTUAL_CLASS);
            PARSER.declareLong(constructorArg(), ACTUAL_CLASS_DOC_COUNT);
            PARSER.declareDouble(constructorArg(), ACCURACY);
        }

        /** Name of the actual class. */
        private final String actualClass;
        /** Number of documents (examples) belonging to the {code actualClass} class. */
        private final long actualClassDocCount;
        /** Fraction of documents belonging to the {code actualClass} class predicted correctly. */
        private final double accuracy;

        public ActualClass(
            String actualClass, long actualClassDocCount, double accuracy) {
            this.actualClass = ExceptionsHelper.requireNonNull(actualClass, ACTUAL_CLASS);
            this.actualClassDocCount = actualClassDocCount;
            this.accuracy = accuracy;
        }

        public ActualClass(StreamInput in) throws IOException {
            this.actualClass = in.readString();
            this.actualClassDocCount = in.readVLong();
            this.accuracy = in.readDouble();
        }

        public String getActualClass() {
            return actualClass;
        }

        public double getAccuracy() {
            return accuracy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(actualClass);
            out.writeVLong(actualClassDocCount);
            out.writeDouble(accuracy);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACTUAL_CLASS.getPreferredName(), actualClass);
            builder.field(ACTUAL_CLASS_DOC_COUNT.getPreferredName(), actualClassDocCount);
            builder.field(ACCURACY.getPreferredName(), accuracy);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ActualClass that = (ActualClass) o;
            return Objects.equals(this.actualClass, that.actualClass)
                && this.actualClassDocCount == that.actualClassDocCount
                && this.accuracy == that.accuracy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualClass, actualClassDocCount, accuracy);
        }
    }
}
