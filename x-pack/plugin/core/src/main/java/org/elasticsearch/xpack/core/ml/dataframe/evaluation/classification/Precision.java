/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
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

    private static final String AGG_NAME_PREFIX = "classification_precision_";
    static final String ACTUAL_CLASSES_NAMES_AGG_NAME = AGG_NAME_PREFIX + "by_actual_class";
    static final String BY_PREDICTED_CLASS_AGG_NAME = AGG_NAME_PREFIX + "by_predicted_class";
    static final String PER_PREDICTED_CLASS_PRECISION_AGG_NAME = AGG_NAME_PREFIX + "per_predicted_class_precision";
    static final String AVG_PRECISION_AGG_NAME = AGG_NAME_PREFIX + "avg_precision";

    private static final ObjectParser<Precision, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Precision::new);

    public static Precision fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int MAX_CLASSES_CARDINALITY = 1000;

    private final SetOnce<String> actualField = new SetOnce<>();
    private final SetOnce<List<String>> topActualClassNames = new SetOnce<>();
    private final SetOnce<Result> result = new SetOnce<>();

    public Precision() {}

    public Precision(StreamInput in) throws IOException {}

    @Override
    public String getWriteableName() {
        return registeredMetricName(Classification.NAME, NAME);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
    }

    @Override
    public final Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(
        EvaluationParameters parameters,
        EvaluationFields fields
    ) {
        String actualFieldName = fields.getActualField();
        String predictedField = fields.getPredictedField();
        // Store given {@code actualField} for the purpose of generating error message in {@code process}.
        this.actualField.trySet(actualFieldName);
        if (topActualClassNames.get() == null) {  // This is step 1
            return Tuple.tuple(
                List.of(
                    AggregationBuilders.terms(ACTUAL_CLASSES_NAMES_AGG_NAME)
                        .field(actualFieldName)
                        .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                        .size(MAX_CLASSES_CARDINALITY)
                ),
                List.of()
            );
        }
        if (result.get() == null) {  // This is step 2
            KeyedFilter[] keyedFiltersPredicted = topActualClassNames.get()
                .stream()
                .map(className -> new KeyedFilter(className, QueryBuilders.matchQuery(predictedField, className).lenient(true)))
                .toArray(KeyedFilter[]::new);
            Script script = PainlessScripts.buildIsEqualScript(actualFieldName, predictedField);
            return Tuple.tuple(
                List.of(
                    AggregationBuilders.filters(BY_PREDICTED_CLASS_AGG_NAME, keyedFiltersPredicted)
                        .subAggregation(AggregationBuilders.avg(PER_PREDICTED_CLASS_PRECISION_AGG_NAME).script(script))
                ),
                List.of(
                    PipelineAggregatorBuilders.avgBucket(
                        AVG_PRECISION_AGG_NAME,
                        BY_PREDICTED_CLASS_AGG_NAME + ">" + PER_PREDICTED_CLASS_PRECISION_AGG_NAME
                    )
                )
            );
        }
        return Tuple.tuple(List.of(), List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        final Aggregation classNamesAgg = aggs.get(ACTUAL_CLASSES_NAMES_AGG_NAME);
        if (topActualClassNames.get() == null && classNamesAgg instanceof Terms topActualClassesAgg) {
            if (topActualClassesAgg.getSumOfOtherDocCounts() > 0) {
                // This means there were more than {@code MAX_CLASSES_CARDINALITY} buckets.
                // We cannot calculate average precision accurately, so we fail.
                throw ExceptionsHelper.badRequestException(
                    "Cannot calculate average precision. Cardinality of field [{}] is too high",
                    actualField.get()
                );
            }
            topActualClassNames.set(
                topActualClassesAgg.getBuckets().stream().map(Terms.Bucket::getKeyAsString).sorted().collect(Collectors.toList())
            );
        }
        final Aggregation byPredicted = aggs.get(BY_PREDICTED_CLASS_AGG_NAME);
        final Aggregation avgPrecision = aggs.get(AVG_PRECISION_AGG_NAME);
        if (result.get() == null
            && byPredicted instanceof Filters byPredictedClassAgg
            && avgPrecision instanceof NumericMetricsAggregation.SingleValue avgPrecisionAgg) {
            List<PerClassSingleValue> classes = new ArrayList<>(byPredictedClassAgg.getBuckets().size());
            for (Filters.Bucket bucket : byPredictedClassAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                NumericMetricsAggregation.SingleValue precisionAgg = bucket.getAggregations().get(PER_PREDICTED_CLASS_PRECISION_AGG_NAME);
                double precision = precisionAgg.value();
                if (Double.isFinite(precision)) {
                    classes.add(new PerClassSingleValue(className, precision));
                }
            }
            result.set(new Result(classes, avgPrecisionAgg.value()));
        }
    }

    @Override
    public Optional<Result> getResult() {
        return Optional.ofNullable(result.get());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

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

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField AVG_PRECISION = new ParseField("avg_precision");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER = new ConstructingObjectParser<>(
            "precision_result",
            true,
            a -> new Result((List<PerClassSingleValue>) a[0], (double) a[1])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassSingleValue.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), AVG_PRECISION);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassSingleValue> classes;
        /** Average of per-class precisions. */
        private final double avgPrecision;

        public Result(List<PerClassSingleValue> classes, double avgPrecision) {
            this.classes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(classes, CLASSES));
            this.avgPrecision = avgPrecision;
        }

        public Result(StreamInput in) throws IOException {
            this.classes = Collections.unmodifiableList(in.readList(PerClassSingleValue::new));
            this.avgPrecision = in.readDouble();
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Classification.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        public List<PerClassSingleValue> getClasses() {
            return classes;
        }

        public double getAvgPrecision() {
            return avgPrecision;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(classes);
            out.writeDouble(avgPrecision);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASSES.getPreferredName(), classes);
            builder.field(AVG_PRECISION.getPreferredName(), avgPrecision);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.classes, that.classes) && this.avgPrecision == that.avgPrecision;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, avgPrecision);
        }
    }
}
