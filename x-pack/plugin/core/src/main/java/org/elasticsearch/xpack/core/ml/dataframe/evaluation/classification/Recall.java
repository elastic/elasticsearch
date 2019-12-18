/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
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
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * {@link Recall} is a metric that answers the question:
 *   "What fraction of documents belonging to X have been predicted as X by the classifier?"
 * for any given class X
 *
 * equation: recall(X) = TP(X) / (TP(X) + FN(X))
 * where: TP(X) - number of true positives wrt X
 *        FN(X) - number of false negatives wrt X
 */
public class Recall implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("recall");

    public static final ParseField SIZE = new ParseField("size");

    private static final String PAINLESS_TEMPLATE = "doc[''{0}''].value == doc[''{1}''].value";
    private static final String AGG_NAME_PREFIX = "classification_recall_";
    static final String BY_ACTUAL_CLASS_AGG_NAME = AGG_NAME_PREFIX + "by_actual_class";
    static final String PER_ACTUAL_CLASS_RECALL_AGG_NAME = AGG_NAME_PREFIX + "per_actual_class_recall";
    static final String AVG_RECALL_AGG_NAME = AGG_NAME_PREFIX + "avg_recall";
    static final String CARDINALITY_OF_ACTUAL_CLASS = AGG_NAME_PREFIX + "cardinality_of_actual_class";

    private static Script buildScript(Object...args) {
        return new Script(new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args));
    }

    private static final ConstructingObjectParser<Recall, Void> PARSER = createParser();

    private static ConstructingObjectParser<Recall, Void> createParser() {
        ConstructingObjectParser<Recall, Void> parser =
            new ConstructingObjectParser<>(NAME.getPreferredName(), true, args -> new Recall((Integer) args[0]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        return parser;
    }

    public static Recall fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int DEFAULT_SIZE = 10;
    private static final int MAX_SIZE = 1000;

    private final int size;
    private EvaluationMetricResult result;

    public Recall() {
        this((Integer) null);
    }

    public Recall(@Nullable Integer size) {
        if (size != null && (size <= 0 || size > MAX_SIZE)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, {}]", SIZE.getPreferredName(), MAX_SIZE);
        }
        this.size = size != null ? size : DEFAULT_SIZE;
    }

    public Recall(StreamInput in) throws IOException {
        this.size = in.readVInt();
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(Classification.NAME, NAME);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public final Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(String actualField, String predictedField) {
        if (result != null) {
            return Tuple.tuple(List.of(), List.of());
        }
        Script script = buildScript(actualField, predictedField);
        return Tuple.tuple(
            List.of(
                AggregationBuilders.cardinality(CARDINALITY_OF_ACTUAL_CLASS)
                    .field(actualField),
                AggregationBuilders.terms(BY_ACTUAL_CLASS_AGG_NAME)
                    .field(actualField)
                    .size(size)
                    .subAggregation(AggregationBuilders.avg(PER_ACTUAL_CLASS_RECALL_AGG_NAME).script(script))),
            List.of(
                PipelineAggregatorBuilders.avgBucket(
                    AVG_RECALL_AGG_NAME,
                    BY_ACTUAL_CLASS_AGG_NAME + ">" + PER_ACTUAL_CLASS_RECALL_AGG_NAME)));
    }

    @Override
    public void process(Aggregations aggs) {
        if (result == null &&
                aggs.get(BY_ACTUAL_CLASS_AGG_NAME) instanceof Terms &&
                aggs.get(AVG_RECALL_AGG_NAME) instanceof NumericMetricsAggregation.SingleValue &&
                aggs.get(CARDINALITY_OF_ACTUAL_CLASS) instanceof Cardinality) {
            Terms byActualClassAgg = aggs.get(BY_ACTUAL_CLASS_AGG_NAME);
            NumericMetricsAggregation.SingleValue avgRecallAgg = aggs.get(AVG_RECALL_AGG_NAME);
            Cardinality cardinalityAgg = aggs.get(CARDINALITY_OF_ACTUAL_CLASS);
            List<PerClassResult> classes = new ArrayList<>(byActualClassAgg.getBuckets().size());
            for (Terms.Bucket bucket : byActualClassAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                NumericMetricsAggregation.SingleValue recallAgg = bucket.getAggregations().get(PER_ACTUAL_CLASS_RECALL_AGG_NAME);
                classes.add(new PerClassResult(className, recallAgg.value()));
            }
            result = new Result(classes, avgRecallAgg.value(), Math.max(cardinalityAgg.getValue() - size, 0));
        }
    }

    @Override
    public Optional<EvaluationMetricResult> getResult() {
        return Optional.ofNullable(result);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIZE.getPreferredName(), size);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Recall that = (Recall) o;
        return Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }

    public static class Result implements EvaluationMetricResult {

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField AVG_RECALL = new ParseField("avg_recall");
        private static final ParseField OTHER_CLASS_COUNT = new ParseField("other_class_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("recall_result", true, a -> new Result((List<PerClassResult>) a[0], (double) a[1], (long) a[2]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassResult.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), AVG_RECALL);
            PARSER.declareLong(constructorArg(), OTHER_CLASS_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassResult> classes;
        /** Average of per-class recalls. */
        private final double avgRecall;
        /** Number of classes that were not included in the per-class results because there were too many of them. */
        private final long otherClassCount;

        public Result(List<PerClassResult> classes, double avgRecall, long otherClassCount) {
            this.classes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(classes, CLASSES));
            this.avgRecall = avgRecall;
            this.otherClassCount = otherClassCount;
        }

        public Result(StreamInput in) throws IOException {
            this.classes = Collections.unmodifiableList(in.readList(PerClassResult::new));
            this.avgRecall = in.readDouble();
            this.otherClassCount = in.readVLong();
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Classification.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public List<PerClassResult> getClasses() {
            return classes;
        }

        public double getAvgRecall() {
            return avgRecall;
        }

        public long getOtherClassCount() {
            return otherClassCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(classes);
            out.writeDouble(avgRecall);
            out.writeVLong(otherClassCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASSES.getPreferredName(), classes);
            builder.field(AVG_RECALL.getPreferredName(), avgRecall);
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
                && this.avgRecall == that.avgRecall
                && this.otherClassCount == that.otherClassCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, avgRecall, otherClassCount);
        }
    }

    public static class PerClassResult implements ToXContentObject, Writeable {

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
            this.className = ExceptionsHelper.requireNonNull(className, CLASS_NAME);
            this.recall = recall;
        }

        public PerClassResult(StreamInput in) throws IOException {
            this.className = in.readString();
            this.recall = in.readDouble();
        }

        public String getClassName() {
            return className;
        }

        public double getRecall() {
            return recall;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(className);
            out.writeDouble(recall);
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
