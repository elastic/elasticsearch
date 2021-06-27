/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * {@link Accuracy} is a metric that answers the following two questions:
 *
 *   1. What is the fraction of documents for which predicted class equals the actual class?
 *
 *      equation: overall_accuracy = 1/n * Σ(y == y')
 *      where: n  = total number of documents
 *             y  = document's actual class
 *             y' = document's predicted class
 *
 *   2. For any given class X, what is the fraction of documents for which either
 *       a) both actual and predicted class are equal to X (true positives)
 *      or
 *       b) both actual and predicted class are not equal to X (true negatives)
 *
 *      equation: accuracy(X) = 1/n * (TP(X) + TN(X))
 *      where: X     = class being examined
 *             n     = total number of documents
 *             TP(X) = number of true positives wrt X
 *             TN(X) = number of true negatives wrt X
 */
public class Accuracy implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("accuracy");

    static final String OVERALL_ACCURACY_AGG_NAME = "classification_overall_accuracy";

    private static final ObjectParser<Accuracy, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Accuracy::new);

    public static Accuracy fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int MAX_CLASSES_CARDINALITY = 1000;

    private final MulticlassConfusionMatrix matrix;
    private final SetOnce<String> actualField = new SetOnce<>();
    private final SetOnce<Double> overallAccuracy = new SetOnce<>();
    private final SetOnce<Result> result = new SetOnce<>();

    public Accuracy() {
        this.matrix = new MulticlassConfusionMatrix(MAX_CLASSES_CARDINALITY, NAME.getPreferredName() + "_");
    }

    public Accuracy(StreamInput in) throws IOException {
        this.matrix = new MulticlassConfusionMatrix(in);
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
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
    }

    @Override
    public final Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                        EvaluationFields fields) {
        // Store given {@code actualField} for the purpose of generating error message in {@code process}.
        this.actualField.trySet(fields.getActualField());
        List<AggregationBuilder> aggs = new ArrayList<>();
        List<PipelineAggregationBuilder> pipelineAggs = new ArrayList<>();
        if (overallAccuracy.get() == null) {
            Script script = PainlessScripts.buildIsEqualScript(fields.getActualField(), fields.getPredictedField());
            aggs.add(AggregationBuilders.avg(OVERALL_ACCURACY_AGG_NAME).script(script));
        }
        if (result.get() == null) {
            Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> matrixAggs = matrix.aggs(parameters, fields);
            aggs.addAll(matrixAggs.v1());
            pipelineAggs.addAll(matrixAggs.v2());
        }
        return Tuple.tuple(aggs, pipelineAggs);
    }

    @Override
    public void process(Aggregations aggs) {
        if (overallAccuracy.get() == null && aggs.get(OVERALL_ACCURACY_AGG_NAME) instanceof NumericMetricsAggregation.SingleValue) {
            NumericMetricsAggregation.SingleValue overallAccuracyAgg = aggs.get(OVERALL_ACCURACY_AGG_NAME);
            overallAccuracy.set(overallAccuracyAgg.value());
        }
        matrix.process(aggs);
        if (result.get() == null && matrix.getResult().isPresent()) {
            if (matrix.getResult().get().getOtherActualClassCount() > 0) {
                // This means there were more than {@code maxClassesCardinality} buckets.
                // We cannot calculate per-class accuracy accurately, so we fail.
                throw ExceptionsHelper.badRequestException(
                    "Cannot calculate per-class accuracy. Cardinality of field [{}] is too high", actualField.get());
            }
            result.set(new Result(computePerClassAccuracy(matrix.getResult().get()), overallAccuracy.get()));
        }
    }

    @Override
    public Optional<Result> getResult() {
        return Optional.ofNullable(result.get());
    }

    /**
     * Computes the per-class accuracy results based on multiclass confusion matrix's result.
     * Time complexity of this method is linear wrt multiclass confusion matrix size, so O(n^2) where n is the matrix dimension.
     * This method is visible for testing only.
     */
    static List<PerClassSingleValue> computePerClassAccuracy(MulticlassConfusionMatrix.Result matrixResult) {
        assert matrixResult.getOtherActualClassCount() == 0;
        // Number of actual classes taken into account
        int n = matrixResult.getConfusionMatrix().size();
        // Total number of documents taken into account
        long totalDocCount =
            matrixResult.getConfusionMatrix().stream().mapToLong(MulticlassConfusionMatrix.ActualClass::getActualClassDocCount).sum();
        List<PerClassSingleValue> classes = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            String className = matrixResult.getConfusionMatrix().get(i).getActualClass();
            // Start with the assumption that all the docs were predicted correctly.
            long correctDocCount = totalDocCount;
            for (int j = 0; j < n; ++j) {
                if (i != j) {
                    // Subtract errors (false negatives)
                    correctDocCount -= matrixResult.getConfusionMatrix().get(i).getPredictedClasses().get(j).getCount();
                    // Subtract errors (false positives)
                    correctDocCount -= matrixResult.getConfusionMatrix().get(j).getPredictedClasses().get(i).getCount();
                }
            }
            // Subtract errors (false negatives) for classes other than explicitly listed in confusion matrix
            correctDocCount -= matrixResult.getConfusionMatrix().get(i).getOtherPredictedClassDocCount();
            classes.add(new PerClassSingleValue(className, ((double)correctDocCount) / totalDocCount));
        }
        return classes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        matrix.writeTo(out);
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
        Accuracy that = (Accuracy) o;
        return Objects.equals(this.matrix, that.matrix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matrix);
    }

    public static class Result implements EvaluationMetricResult {

        private static final ParseField CLASSES = new ParseField("classes");
        private static final ParseField OVERALL_ACCURACY = new ParseField("overall_accuracy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("accuracy_result", true, a -> new Result((List<PerClassSingleValue>) a[0], (double) a[1]));

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassSingleValue.PARSER, CLASSES);
            PARSER.declareDouble(constructorArg(), OVERALL_ACCURACY);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /** List of per-class results. */
        private final List<PerClassSingleValue> classes;
        /** Fraction of documents for which predicted class equals the actual class. */
        private final double overallAccuracy;

        public Result(List<PerClassSingleValue> classes, double overallAccuracy) {
            this.classes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(classes, CLASSES));
            this.overallAccuracy = overallAccuracy;
        }

        public Result(StreamInput in) throws IOException {
            this.classes = Collections.unmodifiableList(in.readList(PerClassSingleValue::new));
            this.overallAccuracy = in.readDouble();
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

        public double getOverallAccuracy() {
            return overallAccuracy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(classes);
            out.writeDouble(overallAccuracy);
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
}
