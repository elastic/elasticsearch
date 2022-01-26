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
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
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

    private static final String AGG_NAME_PREFIX = "classification_recall_";
    static final String BY_ACTUAL_CLASS_AGG_NAME = AGG_NAME_PREFIX + "by_actual_class";
    static final String PER_ACTUAL_CLASS_RECALL_AGG_NAME = AGG_NAME_PREFIX + "per_actual_class_recall";
    static final String AVG_RECALL_AGG_NAME = AGG_NAME_PREFIX + "avg_recall";

    private static final ObjectParser<Recall, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Recall::new);

    public static Recall fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int MAX_CLASSES_CARDINALITY = 1000;

    private final SetOnce<String> actualField = new SetOnce<>();
    private final SetOnce<Result> result = new SetOnce<>();

    public Recall() {}

    public Recall(StreamInput in) throws IOException {}

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
        if (result.get() != null) {
            return Tuple.tuple(List.of(), List.of());
        }
        Script script = PainlessScripts.buildIsEqualScript(actualFieldName, predictedField);
        return Tuple.tuple(
            List.of(
                AggregationBuilders.terms(BY_ACTUAL_CLASS_AGG_NAME)
                    .field(actualFieldName)
                    .order(List.of(BucketOrder.count(false), BucketOrder.key(true)))
                    .size(MAX_CLASSES_CARDINALITY)
                    .subAggregation(AggregationBuilders.avg(PER_ACTUAL_CLASS_RECALL_AGG_NAME).script(script))
            ),
            List.of(
                PipelineAggregatorBuilders.avgBucket(AVG_RECALL_AGG_NAME, BY_ACTUAL_CLASS_AGG_NAME + ">" + PER_ACTUAL_CLASS_RECALL_AGG_NAME)
            )
        );
    }

    @Override
    public void process(Aggregations aggs) {
        final Aggregation byClass = aggs.get(BY_ACTUAL_CLASS_AGG_NAME);
        final Aggregation avgRecall = aggs.get(AVG_RECALL_AGG_NAME);
        if (result.get() == null
            && byClass instanceof Terms byActualClassAgg
            && avgRecall instanceof NumericMetricsAggregation.SingleValue avgRecallAgg) {
            if (byActualClassAgg.getSumOfOtherDocCounts() > 0) {
                // This means there were more than {@code MAX_CLASSES_CARDINALITY} buckets.
                // We cannot calculate average recall accurately, so we fail.
                throw ExceptionsHelper.badRequestException(
                    "Cannot calculate average recall. Cardinality of field [{}] is too high",
                    actualField.get()
                );
            }
            List<PerClassSingleValue> classes = new ArrayList<>(byActualClassAgg.getBuckets().size());
            for (Terms.Bucket bucket : byActualClassAgg.getBuckets()) {
                String className = bucket.getKeyAsString();
                NumericMetricsAggregation.SingleValue recallAgg = bucket.getAggregations().get(PER_ACTUAL_CLASS_RECALL_AGG_NAME);
                classes.add(new PerClassSingleValue(className, recallAgg.value()));
            }
            result.set(new Result(classes, avgRecallAgg.value()));
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
        private static final ParseField AVG_RECALL = new ParseField("avg_recall");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER = new ConstructingObjectParser<>(
            "recall_result",
            true,
            a -> new Result((List<PerClassSingleValue>) a[0], (double) a[1])
        );

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
            this.classes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(classes, CLASSES));
            this.avgRecall = avgRecall;
        }

        public Result(StreamInput in) throws IOException {
            this.classes = Collections.unmodifiableList(in.readList(PerClassSingleValue::new));
            this.avgRecall = in.readDouble();
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

        public double getAvgRecall() {
            return avgRecall;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(classes);
            out.writeDouble(avgRecall);
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
            return Objects.equals(this.classes, that.classes) && this.avgRecall == that.avgRecall;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classes, avgRecall);
        }
    }
}
