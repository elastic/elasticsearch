/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * {@link MulticlassConfusionMatrix} is a metric that answers the question:
 *   "How many examples belonging to class X were classified as Y by the classifier?"
 * for all the possible class pairs {X, Y}.
 */
public class MulticlassConfusionMatrix implements ClassificationMetric {

    public static final ParseField NAME = new ParseField("multiclass_confusion_matrix");

    public static final ParseField SIZE = new ParseField("size");

    private static final ConstructingObjectParser<MulticlassConfusionMatrix, Void> PARSER = createParser();

    private static ConstructingObjectParser<MulticlassConfusionMatrix, Void> createParser() {
        ConstructingObjectParser<MulticlassConfusionMatrix, Void>  parser =
            new ConstructingObjectParser<>(NAME.getPreferredName(), true, args -> new MulticlassConfusionMatrix((Integer) args[0]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        return parser;
    }

    public static MulticlassConfusionMatrix fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final String STEP_1_AGGREGATE_BY_ACTUAL_CLASS = NAME.getPreferredName() + "_step_1_by_actual_class";
    private static final String STEP_2_AGGREGATE_BY_ACTUAL_CLASS = NAME.getPreferredName() + "_step_2_by_actual_class";
    private static final String STEP_2_AGGREGATE_BY_PREDICTED_CLASS = NAME.getPreferredName() + "_step_2_by_predicted_class";
    private static final String STEP_2_CARDINALITY_OF_ACTUAL_CLASS = NAME.getPreferredName() + "_step_2_cardinality_of_actual_class";
    private static final String OTHER_BUCKET_KEY = "_other_";
    private static final int DEFAULT_SIZE = 10;
    private static final int MAX_SIZE = 1000;

    private final int size;
    private List<String> topActualClassNames;
    private Result result;

    public MulticlassConfusionMatrix() {
        this((Integer) null);
    }

    public MulticlassConfusionMatrix(@Nullable Integer size) {
        if (size != null && (size <= 0 || size > MAX_SIZE)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, {}]", SIZE.getPreferredName(), MAX_SIZE);
        }
        this.size = size != null ? size : DEFAULT_SIZE;
    }

    public MulticlassConfusionMatrix(StreamInput in) throws IOException {
        this.size = in.readVInt();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    public int getSize() {
        return size;
    }

    @Override
    public final List<AggregationBuilder> aggs(String actualField, String predictedField) {
        if (topActualClassNames == null) {  // This is step 1
            return List.of(
                AggregationBuilders.terms(STEP_1_AGGREGATE_BY_ACTUAL_CLASS)
                    .field(actualField)
                    .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                    .size(size));
        }
        if (result == null) {  // This is step 2
            KeyedFilter[] keyedFilters =
                topActualClassNames.stream()
                    .map(className -> new KeyedFilter(className, QueryBuilders.termQuery(predictedField, className)))
                    .toArray(KeyedFilter[]::new);
            return List.of(
                AggregationBuilders.cardinality(STEP_2_CARDINALITY_OF_ACTUAL_CLASS)
                    .field(actualField),
                AggregationBuilders.terms(STEP_2_AGGREGATE_BY_ACTUAL_CLASS)
                    .field(actualField)
                    .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                    .size(size)
                    .subAggregation(AggregationBuilders.filters(STEP_2_AGGREGATE_BY_PREDICTED_CLASS, keyedFilters)
                        .otherBucket(true)
                        .otherBucketKey(OTHER_BUCKET_KEY)));
        }
        return List.of();
    }

    @Override
    public void process(Aggregations aggs) {
        if (topActualClassNames == null && aggs.get(STEP_1_AGGREGATE_BY_ACTUAL_CLASS) != null) {
            Terms termsAgg = aggs.get(STEP_1_AGGREGATE_BY_ACTUAL_CLASS);
            topActualClassNames = termsAgg.getBuckets().stream().map(Terms.Bucket::getKeyAsString).collect(Collectors.toList());
        }
        if (result == null && aggs.get(STEP_2_AGGREGATE_BY_ACTUAL_CLASS) != null) {
            Cardinality cardinalityAgg = aggs.get(STEP_2_CARDINALITY_OF_ACTUAL_CLASS);
            Terms termsAgg = aggs.get(STEP_2_AGGREGATE_BY_ACTUAL_CLASS);
            Map<String, Map<String, Long>> counts = new TreeMap<>();
            for (Terms.Bucket bucket : termsAgg.getBuckets()) {
                String actualClass = bucket.getKeyAsString();
                Map<String, Long> subCounts = new TreeMap<>();
                counts.put(actualClass, subCounts);
                Filters subAgg = bucket.getAggregations().get(STEP_2_AGGREGATE_BY_PREDICTED_CLASS);
                for (Filters.Bucket subBucket : subAgg.getBuckets()) {
                    String predictedClass = subBucket.getKeyAsString();
                    Long docCount = subBucket.getDocCount();
                    if ((OTHER_BUCKET_KEY.equals(predictedClass) && docCount == 0L) == false) {
                        subCounts.put(predictedClass, docCount);
                    }
                }
            }
            result = new Result(counts, termsAgg.getSumOfOtherDocCounts() == 0 ? 0 : cardinalityAgg.getValue() - size);
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
        MulticlassConfusionMatrix that = (MulticlassConfusionMatrix) o;
        return Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }

    public static class Result implements EvaluationMetricResult {

        private static final ParseField CONFUSION_MATRIX = new ParseField("confusion_matrix");
        private static final ParseField OTHER_CLASSES_COUNT = new ParseField("_other_");

        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_result", true, a -> new Result((Map<String, Map<String, Long>>) a[0], (long) a[1]));

        static {
            PARSER.declareObject(
                constructorArg(),
                (p, c) -> p.map(TreeMap::new, p2 -> p2.map(TreeMap::new, XContentParser::longValue)),
                CONFUSION_MATRIX);
            PARSER.declareLong(constructorArg(), OTHER_CLASSES_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        // Immutable
        private final Map<String, Map<String, Long>> confusionMatrix;
        private final long otherClassesCount;

        public Result(Map<String, Map<String, Long>> confusionMatrix, long otherClassesCount) {
            this.confusionMatrix = Collections.unmodifiableMap(Objects.requireNonNull(confusionMatrix));
            this.otherClassesCount = otherClassesCount;
        }

        public Result(StreamInput in) throws IOException {
            this.confusionMatrix = Collections.unmodifiableMap(
                in.readMap(StreamInput::readString, in2 -> in2.readMap(StreamInput::readString, StreamInput::readLong)));
            this.otherClassesCount = in.readLong();
        }

        @Override
        public String getWriteableName() {
            return NAME.getPreferredName();
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public Map<String, Map<String, Long>> getConfusionMatrix() {
            return confusionMatrix;
        }

        public long getOtherClassesCount() {
            return otherClassesCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(
                confusionMatrix,
                StreamOutput::writeString,
                (out2, row) -> out2.writeMap(row, StreamOutput::writeString, StreamOutput::writeLong));
            out.writeLong(otherClassesCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONFUSION_MATRIX.getPreferredName(), confusionMatrix);
            builder.field(OTHER_CLASSES_COUNT.getPreferredName(), otherClassesCount);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.confusionMatrix, that.confusionMatrix)
                && this.otherClassesCount == that.otherClassesCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(confusionMatrix, otherClassesCount);
        }
    }
}
