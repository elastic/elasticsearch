/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Evaluation of binary soft classification methods, e.g. outlier detection.
 * This is useful to evaluate problems where a model outputs a probability of whether
 * a data frame row belongs to one of two groups.
 */
public class BinarySoftClassification implements Evaluation {

    public static final ParseField NAME = new ParseField("binary_soft_classification");

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_PROBABILITY_FIELD = new ParseField("predicted_probability_field");
    private static final ParseField METRICS = new ParseField("metrics");

    public static final ConstructingObjectParser<BinarySoftClassification, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(), a -> new BinarySoftClassification((String) a[0], (String) a[1], (List<EvaluationMetric>) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PREDICTED_PROBABILITY_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(EvaluationMetric.class, registeredMetricName(NAME.getPreferredName(), n), c), METRICS);
    }

    public static BinarySoftClassification fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static QueryBuilder actualIsTrueQuery(String actualField) {
        return QueryBuilders.queryStringQuery(actualField + ": (1 OR true)");
    }

    /**
     * The field where the actual class is marked up.
     * The value of this field is assumed to either be 1 or 0, or true or false.
     */
    private final String actualField;

    /**
     * The field of the predicted probability in [0.0, 1.0].
     */
    private final String predictedProbabilityField;

    /**
     * The list of metrics to calculate
     */
    private final List<EvaluationMetric> metrics;

    public BinarySoftClassification(String actualField, String predictedProbabilityField,
                                    @Nullable List<EvaluationMetric> metrics) {
        this.actualField = ExceptionsHelper.requireNonNull(actualField, ACTUAL_FIELD);
        this.predictedProbabilityField = ExceptionsHelper.requireNonNull(predictedProbabilityField, PREDICTED_PROBABILITY_FIELD);
        this.metrics = initMetrics(metrics, BinarySoftClassification::defaultMetrics);
    }

    private static List<EvaluationMetric> defaultMetrics() {
        return Arrays.asList(
            new AucRoc(false),
            new Precision(Arrays.asList(0.25, 0.5, 0.75)),
            new Recall(Arrays.asList(0.25, 0.5, 0.75)),
            new ConfusionMatrix(Arrays.asList(0.25, 0.5, 0.75)));
    }

    public BinarySoftClassification(StreamInput in) throws IOException {
        this.actualField = in.readString();
        this.predictedProbabilityField = in.readString();
        this.metrics = in.readNamedWriteableList(EvaluationMetric.class);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getActualField() {
        return actualField;
    }

    @Override
    public String getPredictedField() {
        return predictedProbabilityField;
    }

    @Override
    public List<EvaluationMetric> getMetrics() {
        return metrics;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(actualField);
        out.writeString(predictedProbabilityField);
        out.writeNamedWriteableList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), actualField);
        builder.field(PREDICTED_PROBABILITY_FIELD.getPreferredName(), predictedProbabilityField);

        builder.startObject(METRICS.getPreferredName());
        for (EvaluationMetric metric : metrics) {
            builder.field(metric.getName(), metric);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinarySoftClassification that = (BinarySoftClassification) o;
        return Objects.equals(actualField, that.actualField)
            && Objects.equals(predictedProbabilityField, that.predictedProbabilityField)
            && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualField, predictedProbabilityField, metrics);
    }
}
