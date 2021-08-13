/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields.ACTUAL_FIELD;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields.PREDICTED_FIELD;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields.TOP_CLASSES_FIELD;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Evaluation of classification results.
 */
public class Classification implements Evaluation {

    public static final ParseField NAME = new ParseField("classification");

    private static final ParseField METRICS = new ParseField("metrics");

    private static final String DEFAULT_TOP_CLASSES_FIELD = "ml.top_classes";
    private static final String DEFAULT_PREDICTED_CLASS_FIELD_SUFFIX = ".class_name";
    private static final String DEFAULT_PREDICTED_PROBABILITY_FIELD_SUFFIX = ".class_probability";

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Classification, Void> PARSER =
        new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            a -> new Classification((String) a[0], (String) a[1], (String) a[2], (List<EvaluationMetric>) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PREDICTED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TOP_CLASSES_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(EvaluationMetric.class, registeredMetricName(NAME.getPreferredName(), n), c), METRICS);
    }

    public static Classification fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * The collection of fields in the index being evaluated.
     *   fields.getActualField() is assumed to be a ground truth label.
     *   fields.getPredictedField() is assumed to be a predicted label.
     *   fields.getPredictedClassField() and fields.getPredictedProbabilityField() are assumed to be properties under the same nested field.
     */
    private final EvaluationFields fields;

    /**
     * The list of metrics to calculate
     */
    private final List<EvaluationMetric> metrics;

    public Classification(String actualField,
                          @Nullable String predictedField,
                          @Nullable String topClassesField,
                          @Nullable List<EvaluationMetric> metrics) {
        if (topClassesField == null) {
            topClassesField = DEFAULT_TOP_CLASSES_FIELD;
        }
        String predictedClassField = topClassesField + DEFAULT_PREDICTED_CLASS_FIELD_SUFFIX;
        String predictedProbabilityField = topClassesField + DEFAULT_PREDICTED_PROBABILITY_FIELD_SUFFIX;
        this.fields =
            new EvaluationFields(
                ExceptionsHelper.requireNonNull(actualField, ACTUAL_FIELD),
                predictedField,
                topClassesField,
                predictedClassField,
                predictedProbabilityField,
                true);
        this.metrics = initMetrics(metrics, Classification::defaultMetrics);
    }

    private static List<EvaluationMetric> defaultMetrics() {
        return Arrays.asList(new Accuracy(), new MulticlassConfusionMatrix(), new Precision(), new Recall());
    }

    public Classification(StreamInput in) throws IOException {
        this.fields =
            new EvaluationFields(
                in.readString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                true);
        this.metrics = in.readNamedWriteableList(EvaluationMetric.class);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public EvaluationFields getFields() {
        return fields;
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
        out.writeString(fields.getActualField());
        out.writeOptionalString(fields.getPredictedField());
        out.writeOptionalString(fields.getTopClassesField());
        out.writeOptionalString(fields.getPredictedClassField());
        out.writeOptionalString(fields.getPredictedProbabilityField());
        out.writeNamedWriteableList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), fields.getActualField());
        if (fields.getPredictedField() != null) {
            builder.field(PREDICTED_FIELD.getPreferredName(), fields.getPredictedField());
        }
        if (fields.getTopClassesField() != null) {
            builder.field(TOP_CLASSES_FIELD.getPreferredName(), fields.getTopClassesField());
        }
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
        Classification that = (Classification) o;
        return Objects.equals(that.fields, this.fields)
            && Objects.equals(that.metrics, this.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, metrics);
    }
}
