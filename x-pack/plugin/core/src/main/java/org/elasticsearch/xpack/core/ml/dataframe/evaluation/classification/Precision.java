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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
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
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * {@link Precision} is a metric that answers the question:
 *   "What fraction of documents classified as X actually belongs to X?"
 * for any given class X
 *
 * equation: precision(X) = TP(X) / (TP(X) + FP(X))
 * where: TP(X) - number of true positives wrt X
 *        FP(X) - number of false positives wrt X
 */
public class Precision implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("precision");

    public static final ParseField SIZE = new ParseField("size");

    private static final String PAINLESS_TEMPLATE = "doc[''{0}''].value == doc[''{1}''].value";
    private static final String AGG_NAME_PREFIX = "classification_precision_";
    static final String ACTUAL_CLASSES_NAMES_AGG_NAME = AGG_NAME_PREFIX + "by_actual_class";
    static final String BY_PREDICTED_CLASS_AGG_NAME = AGG_NAME_PREFIX + "by_predicted_class";
    static final String PER_PREDICTED_CLASS_PRECISION_AGG_NAME = AGG_NAME_PREFIX + "per_predicted_class_precision";
    static final String AVG_PRECISION_AGG_NAME = AGG_NAME_PREFIX + "avg_precision";
    static final String CARDINALITY_OF_ACTUAL_CLASS = AGG_NAME_PREFIX + "cardinality_of_actual_class";

    private static String buildScript(Object...args) {
        return new MessageFormat(PAINLESS_TEMPLATE, Locale.ROOT).format(args);
    }

    private static final ConstructingObjectParser<Precision, Void> PARSER = createParser();

    private static ConstructingObjectParser<Precision, Void> createParser() {
        ConstructingObjectParser<Precision, Void> parser =
            new ConstructingObjectParser<>(NAME.getPreferredName(), true, args -> new Precision((Integer) args[0]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        return parser;
    }

    public static Precision fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int DEFAULT_SIZE = 10;
    private static final int MAX_SIZE = 1000;

    private final int size;
    private List<String> topActualClassNames;
    private EvaluationMetricResult result;

    public Precision() {
        this((Integer) null);
    }

    public Precision(@Nullable Integer size) {
        if (size != null && (size <= 0 || size > MAX_SIZE)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, {}]", SIZE.getPreferredName(), MAX_SIZE);
        }
        this.size = size != null ? size : DEFAULT_SIZE;
    }

    public Precision(StreamInput in) throws IOException {
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
        if (topActualClassNames == null) {  // This is step 1
            return Tuple.tuple(
                List.of(
                    AggregationBuilders.terms(ACTUAL_CLASSES_NAMES_AGG_NAME)
                        .field(actualField)
                        .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                        .size(size)),
                List.of());
        }
        if (result == null) {  // This is step 2
            KeyedFilter[] keyedFiltersPredicted =
                topActualClassNames.stream()
                    .map(className -> new KeyedFilter(className, QueryBuilders.termQuery(predictedField, className)))
                    .toArray(KeyedFilter[]::new);
            Script script = new Script(buildScript(actualField, predictedField));
            return Tuple.tuple(
                List.of(
                    AggregationBuilders.cardinality(CARDINALITY_OF_ACTUAL_CLASS)
                        .field(actualField),
                    AggregationBuilders.filters(BY_PREDICTED_CLASS_AGG_NAME, keyedFiltersPredicted)
                        .subAggregation(AggregationBuilders.avg(PER_PREDICTED_CLASS_PRECISION_AGG_NAME).script(script))),
                List.of(
                    PipelineAggregatorBuilders.avgBucket(
                        AVG_PRECISION_AGG_NAME,
                        BY_PREDICTED_CLASS_AGG_NAME + ">" + PER_PREDICTED_CLASS_PRECISION_AGG_NAME)));
        }
        return Tuple.tuple(List.of(), List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        if (topActualClassNames == null && aggs.get(ACTUAL_CLASSES_NAMES_AGG_NAME) instanceof Terms) {
            Terms termsAgg = aggs.get(ACTUAL_CLASSES_NAMES_AGG_NAME);
            topActualClassNames = termsAgg.getBuckets().stream().map(Terms.Bucket::getKeyAsString).sorted().collect(Collectors.toList());
        }
        if (result == null &&
                aggs.get(BY_PREDICTED_CLASS_AGG_NAME) instanceof Filters &&
                aggs.get(AVG_PRECISION_AGG_NAME) instanceof NumericMetricsAggregation.SingleValue &&
                aggs.get(CARDINALITY_OF_ACTUAL_CLASS) instanceof Cardinality) {
            Filters byPredictedClassAgg = aggs.get(BY_PREDICTED_CLASS_AGG_NAME);
            NumericMetricsAggregation.SingleValue avgPrecisionAgg = aggs.get(AVG_PRECISION_AGG_NAME);
            Cardinality cardinalityAgg = aggs.get(CARDINALITY_OF_ACTUAL_CLASS);
            List<PerClassResult> classes = new ArrayList<>(byPredictedClassAgg.getBuckets().size());
            for (Filters.Bucket bucket : byPredictedClassAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                NumericMetricsAggregation.SingleValue precisionAgg = bucket.getAggregations().get(PER_PREDICTED_CLASS_PRECISION_AGG_NAME);
                double precision = precisionAgg.value();
                if (Double.isFinite(precision)) {
                    classes.add(new PerClassResult(className, precision));
                }
            }
            result = new Result(classes, avgPrecisionAgg.value(), Math.max(cardinalityAgg.getValue() - size, 0));
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
        Precision that = (Precision) o;
        return Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }

    public static class Result implements EvaluationMetricResult {

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
            this.classes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(classes, CLASSES));
            this.avgPrecision = avgPrecision;
            this.otherClassCount = otherClassCount;
        }

        public Result(StreamInput in) throws IOException {
            this.classes = Collections.unmodifiableList(in.readList(PerClassResult::new));
            this.avgPrecision = in.readDouble();
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

        public double getAvgPrecision() {
            return avgPrecision;
        }

        public long getOtherClassCount() {
            return otherClassCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(classes);
            out.writeDouble(avgPrecision);
            out.writeVLong(otherClassCount);
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

    public static class PerClassResult implements ToXContentObject, Writeable {

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
            this.className = ExceptionsHelper.requireNonNull(className, CLASS_NAME);
            this.precision = precision;
        }

        public PerClassResult(StreamInput in) throws IOException {
            this.className = in.readString();
            this.precision = in.readDouble();
        }

        public String getClassName() {
            return className;
        }

        public double getPrecision() {
            return precision;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(className);
            out.writeDouble(precision);
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
