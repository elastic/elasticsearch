/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * {@link MulticlassConfusionMatrix} is a metric that answers the question:
 *   "How many documents belonging to class X were classified as Y by the classifier?"
 * for all the possible class pairs {X, Y}.
 */
public class MulticlassConfusionMatrix implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("multiclass_confusion_matrix");

    public static final ParseField SIZE = new ParseField("size");
    public static final ParseField AGG_NAME_PREFIX = new ParseField("agg_name_prefix");

    private static final ConstructingObjectParser<MulticlassConfusionMatrix, Void> PARSER = createParser();

    private static ConstructingObjectParser<MulticlassConfusionMatrix, Void> createParser() {
        ConstructingObjectParser<MulticlassConfusionMatrix, Void>  parser =
            new ConstructingObjectParser<>(
                NAME.getPreferredName(), true, args -> new MulticlassConfusionMatrix((Integer) args[0], (String) args[1]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        parser.declareString(optionalConstructorArg(), AGG_NAME_PREFIX);
        return parser;
    }

    public static MulticlassConfusionMatrix fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final String STEP_1_AGGREGATE_BY_ACTUAL_CLASS = NAME.getPreferredName() + "_step_1_by_actual_class";
    static final String STEP_1_CARDINALITY_OF_ACTUAL_CLASS = NAME.getPreferredName() + "_step_1_cardinality_of_actual_class";
    static final String STEP_2_AGGREGATE_BY_ACTUAL_CLASS = NAME.getPreferredName() + "_step_2_by_actual_class";
    static final String STEP_2_AGGREGATE_BY_PREDICTED_CLASS = NAME.getPreferredName() + "_step_2_by_predicted_class";
    private static final String OTHER_BUCKET_KEY = "_other_";
    private static final String DEFAULT_AGG_NAME_PREFIX = "";
    private static final int DEFAULT_SIZE = 10;
    private static final int MAX_SIZE = 1000;

    private final int size;
    private final String aggNamePrefix;
    private final SetOnce<List<String>> topActualClassNames = new SetOnce<>();
    private final SetOnce<Long> actualClassesCardinality = new SetOnce<>();
    /** Accumulates actual classes processed so far. It may take more than 1 call to #process method to fill this field completely. */
    private final List<ActualClass> actualClasses = new ArrayList<>();
    private final SetOnce<Result> result = new SetOnce<>();

    public MulticlassConfusionMatrix() {
        this(null, null);
    }

    public MulticlassConfusionMatrix(@Nullable Integer size, @Nullable String aggNamePrefix) {
        if (size != null && (size <= 0 || size > MAX_SIZE)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, {}]", SIZE.getPreferredName(), MAX_SIZE);
        }
        this.size = size != null ? size : DEFAULT_SIZE;
        this.aggNamePrefix = aggNamePrefix != null ? aggNamePrefix : DEFAULT_AGG_NAME_PREFIX;
    }

    public MulticlassConfusionMatrix(StreamInput in) throws IOException {
        this.size = in.readVInt();
        this.aggNamePrefix = in.readString();
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(Classification.NAME, NAME);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    public int getSize() {
        return size;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
    }

    @Override
    public final Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                        EvaluationFields fields) {
        String actualField = fields.getActualField();
        String predictedField = fields.getPredictedField();
        if (topActualClassNames.get() == null && actualClassesCardinality.get() == null) {  // This is step 1
            return Tuple.tuple(
                List.of(
                    AggregationBuilders.terms(aggName(STEP_1_AGGREGATE_BY_ACTUAL_CLASS))
                        .field(actualField)
                        .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                        .size(size),
                    AggregationBuilders.cardinality(aggName(STEP_1_CARDINALITY_OF_ACTUAL_CLASS))
                        .field(actualField)),
                List.of());
        }
        if (result.get() == null) {  // These are steps 2, 3, 4 etc.
            KeyedFilter[] keyedFiltersPredicted =
                topActualClassNames.get().stream()
                    .map(className -> new KeyedFilter(className, QueryBuilders.matchQuery(predictedField, className).lenient(true)))
                    .toArray(KeyedFilter[]::new);
            // Knowing exactly how many buckets does each aggregation use, we can choose the size of the batch so that
            // too_many_buckets_exception exception is not thrown.
            // The only exception is when "search.max_buckets" is set far too low to even have 1 actual class in the batch.
            // In such case, the exception will be thrown telling the user they should increase the value of "search.max_buckets".
            int actualClassesPerBatch = Math.max(parameters.getMaxBuckets() / (topActualClassNames.get().size() + 2), 1);
            KeyedFilter[] keyedFiltersActual =
                topActualClassNames.get().stream()
                    .skip(actualClasses.size())
                    .limit(actualClassesPerBatch)
                    .map(className -> new KeyedFilter(className, QueryBuilders.matchQuery(actualField, className).lenient(true)))
                    .toArray(KeyedFilter[]::new);
            if (keyedFiltersActual.length > 0) {
                return Tuple.tuple(
                    List.of(
                        AggregationBuilders.filters(aggName(STEP_2_AGGREGATE_BY_ACTUAL_CLASS), keyedFiltersActual)
                            .subAggregation(AggregationBuilders.filters(aggName(STEP_2_AGGREGATE_BY_PREDICTED_CLASS), keyedFiltersPredicted)
                                .otherBucket(true)
                                .otherBucketKey(OTHER_BUCKET_KEY))),
                    List.of());
            }
        }
        return Tuple.tuple(List.of(), List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        if (topActualClassNames.get() == null && aggs.get(aggName(STEP_1_AGGREGATE_BY_ACTUAL_CLASS)) != null) {
            Terms termsAgg = aggs.get(aggName(STEP_1_AGGREGATE_BY_ACTUAL_CLASS));
            topActualClassNames.set(termsAgg.getBuckets().stream().map(Terms.Bucket::getKeyAsString).sorted().collect(Collectors.toList()));
        }
        if (actualClassesCardinality.get() == null && aggs.get(aggName(STEP_1_CARDINALITY_OF_ACTUAL_CLASS)) != null) {
            Cardinality cardinalityAgg = aggs.get(aggName(STEP_1_CARDINALITY_OF_ACTUAL_CLASS));
            actualClassesCardinality.set(cardinalityAgg.getValue());
        }
        if (result.get() == null && aggs.get(aggName(STEP_2_AGGREGATE_BY_ACTUAL_CLASS)) != null) {
            Filters filtersAgg = aggs.get(aggName(STEP_2_AGGREGATE_BY_ACTUAL_CLASS));
            for (Filters.Bucket bucket : filtersAgg.getBuckets()) {
                String actualClass = bucket.getKeyAsString();
                long actualClassDocCount = bucket.getDocCount();
                Filters subAgg = bucket.getAggregations().get(aggName(STEP_2_AGGREGATE_BY_PREDICTED_CLASS));
                List<PredictedClass> predictedClasses = new ArrayList<>();
                long otherPredictedClassDocCount = 0;
                for (Filters.Bucket subBucket : subAgg.getBuckets()) {
                    String predictedClass = subBucket.getKeyAsString();
                    long docCount = subBucket.getDocCount();
                    if (OTHER_BUCKET_KEY.equals(predictedClass)) {
                        otherPredictedClassDocCount = docCount;
                    } else {
                        predictedClasses.add(new PredictedClass(predictedClass, docCount));
                    }
                }
                predictedClasses.sort(comparing(PredictedClass::getPredictedClass));
                actualClasses.add(new ActualClass(actualClass, actualClassDocCount, predictedClasses, otherPredictedClassDocCount));
            }
            if (actualClasses.size() == topActualClassNames.get().size()) {
                result.set(new Result(actualClasses, Math.max(actualClassesCardinality.get() - size, 0)));
            }
        }
    }

    private String aggName(String aggNameWithoutPrefix) {
        return aggNamePrefix + aggNameWithoutPrefix;
    }

    @Override
    public Optional<Result> getResult() {
        return Optional.ofNullable(result.get());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeString(aggNamePrefix);
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
        MulticlassConfusionMatrix that = (MulticlassConfusionMatrix) o;
        return this.size == that.size
            && Objects.equals(this.aggNamePrefix, that.aggNamePrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, aggNamePrefix);
    }

    public static class Result implements EvaluationMetricResult {

        private static final ParseField CONFUSION_MATRIX = new ParseField("confusion_matrix");
        private static final ParseField OTHER_ACTUAL_CLASS_COUNT = new ParseField("other_actual_class_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_result", true, a -> new Result((List<ActualClass>) a[0], (long) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), ActualClass.PARSER, CONFUSION_MATRIX);
            PARSER.declareLong(constructorArg(), OTHER_ACTUAL_CLASS_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of actual classes. */
        private final List<ActualClass> actualClasses;
        /** Number of actual classes that were not included in the confusion matrix because there were too many of them. */
        private final long otherActualClassCount;

        public Result(List<ActualClass> actualClasses, long otherActualClassCount) {
            this.actualClasses = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(actualClasses, CONFUSION_MATRIX));
            this.otherActualClassCount = requireNonNegative(otherActualClassCount, OTHER_ACTUAL_CLASS_COUNT);
        }

        public Result(StreamInput in) throws IOException {
            this.actualClasses = Collections.unmodifiableList(in.readList(ActualClass::new));
            this.otherActualClassCount = in.readVLong();
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Classification.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public List<ActualClass> getConfusionMatrix() {
            return actualClasses;
        }

        public long getOtherActualClassCount() {
            return otherActualClassCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(actualClasses);
            out.writeVLong(otherActualClassCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONFUSION_MATRIX.getPreferredName(), actualClasses);
            builder.field(OTHER_ACTUAL_CLASS_COUNT.getPreferredName(), otherActualClassCount);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.actualClasses, that.actualClasses)
                && this.otherActualClassCount == that.otherActualClassCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualClasses, otherActualClassCount);
        }
    }

    public static class ActualClass implements ToXContentObject, Writeable {

        private static final ParseField ACTUAL_CLASS = new ParseField("actual_class");
        private static final ParseField ACTUAL_CLASS_DOC_COUNT = new ParseField("actual_class_doc_count");
        private static final ParseField PREDICTED_CLASSES = new ParseField("predicted_classes");
        private static final ParseField OTHER_PREDICTED_CLASS_DOC_COUNT = new ParseField("other_predicted_class_doc_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ActualClass, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_actual_class",
                true,
                a -> new ActualClass((String) a[0], (long) a[1], (List<PredictedClass>) a[2], (long) a[3]));

        static {
            PARSER.declareString(constructorArg(), ACTUAL_CLASS);
            PARSER.declareLong(constructorArg(), ACTUAL_CLASS_DOC_COUNT);
            PARSER.declareObjectArray(constructorArg(), PredictedClass.PARSER, PREDICTED_CLASSES);
            PARSER.declareLong(constructorArg(), OTHER_PREDICTED_CLASS_DOC_COUNT);
        }

        /** Name of the actual class. */
        private final String actualClass;
        /** Number of documents belonging to the {code actualClass} class. */
        private final long actualClassDocCount;
        /** List of predicted classes. */
        private final List<PredictedClass> predictedClasses;
        /** Number of documents that were not predicted as any of the {@code predictedClasses}. */
        private final long otherPredictedClassDocCount;

        public ActualClass(
                String actualClass, long actualClassDocCount, List<PredictedClass> predictedClasses, long otherPredictedClassDocCount) {
            this.actualClass = ExceptionsHelper.requireNonNull(actualClass, ACTUAL_CLASS);
            this.actualClassDocCount = requireNonNegative(actualClassDocCount, ACTUAL_CLASS_DOC_COUNT);
            this.predictedClasses = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(predictedClasses, PREDICTED_CLASSES));
            this.otherPredictedClassDocCount = requireNonNegative(otherPredictedClassDocCount, OTHER_PREDICTED_CLASS_DOC_COUNT);
        }

        public ActualClass(StreamInput in) throws IOException {
            this.actualClass = in.readString();
            this.actualClassDocCount = in.readVLong();
            this.predictedClasses = Collections.unmodifiableList(in.readList(PredictedClass::new));
            this.otherPredictedClassDocCount = in.readVLong();
        }

        public String getActualClass() {
            return actualClass;
        }

        public long getActualClassDocCount() {
            return actualClassDocCount;
        }

        public List<PredictedClass> getPredictedClasses() {
            return predictedClasses;
        }

        public long getOtherPredictedClassDocCount() {
            return otherPredictedClassDocCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(actualClass);
            out.writeVLong(actualClassDocCount);
            out.writeList(predictedClasses);
            out.writeVLong(otherPredictedClassDocCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACTUAL_CLASS.getPreferredName(), actualClass);
            builder.field(ACTUAL_CLASS_DOC_COUNT.getPreferredName(), actualClassDocCount);
            builder.field(PREDICTED_CLASSES.getPreferredName(), predictedClasses);
            builder.field(OTHER_PREDICTED_CLASS_DOC_COUNT.getPreferredName(), otherPredictedClassDocCount);
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
                && Objects.equals(this.predictedClasses, that.predictedClasses)
                && this.otherPredictedClassDocCount == that.otherPredictedClassDocCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualClass, actualClassDocCount, predictedClasses, otherPredictedClassDocCount);
        }
    }

    public static class PredictedClass implements ToXContentObject, Writeable {

        private static final ParseField PREDICTED_CLASS = new ParseField("predicted_class");
        private static final ParseField COUNT = new ParseField("count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PredictedClass, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_predicted_class", true, a -> new PredictedClass((String) a[0], (long) a[1]));

        static {
            PARSER.declareString(constructorArg(), PREDICTED_CLASS);
            PARSER.declareLong(constructorArg(), COUNT);
        }

        private final String predictedClass;
        private final long count;

        public PredictedClass(String predictedClass, long count) {
            this.predictedClass = ExceptionsHelper.requireNonNull(predictedClass, PREDICTED_CLASS);
            this.count = requireNonNegative(count, COUNT);
        }

        public PredictedClass(StreamInput in) throws IOException {
            this.predictedClass = in.readString();
            this.count = in.readVLong();
        }

        public String getPredictedClass() {
            return predictedClass;
        }

        public long getCount() {
            return count;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(predictedClass);
            out.writeVLong(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PREDICTED_CLASS.getPreferredName(), predictedClass);
            builder.field(COUNT.getPreferredName(), count);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PredictedClass that = (PredictedClass) o;
            return Objects.equals(this.predictedClass, that.predictedClass)
                && this.count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(predictedClass, count);
        }
    }

    private static long requireNonNegative(long value, ParseField field) {
        if (value < 0) {
            throw ExceptionsHelper.serverError("[" + field.getPreferredName() + "] must be >= 0, was: " + value);
        }
        return value;
    }
}
