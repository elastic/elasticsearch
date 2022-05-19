/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
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
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Evaluation of regression results.
 */
public class Regression implements Evaluation {

    public static final ParseField NAME = new ParseField("regression");

    private static final ParseField METRICS = new ParseField("metrics");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Regression, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new Regression((String) a[0], (String) a[1], (List<EvaluationMetric>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PREDICTED_FIELD);
        PARSER.declareNamedObjects(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(EvaluationMetric.class, registeredMetricName(NAME.getPreferredName(), n), c),
            METRICS
        );
    }

    public static Regression fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * The collection of fields in the index being evaluated.
     *   fields.getActualField() is assumed to be numeric.
     *   fields.getPredictedField() is assumed to be numeric.
     * Other fields are not needed by this evaluation.
     */
    private final EvaluationFields fields;

    /**
     * The list of metrics to calculate
     */
    private final List<EvaluationMetric> metrics;

    public Regression(String actualField, String predictedField, @Nullable List<EvaluationMetric> metrics) {
        this.fields = new EvaluationFields(
            ExceptionsHelper.requireNonNull(actualField, ACTUAL_FIELD),
            ExceptionsHelper.requireNonNull(predictedField, PREDICTED_FIELD),
            null,
            null,
            null,
            false
        );
        this.metrics = initMetrics(metrics, Regression::defaultMetrics);
    }

    private static List<EvaluationMetric> defaultMetrics() {
        return Arrays.asList(new MeanSquaredError(), new RSquared(), new Huber());
    }

    public Regression(StreamInput in) throws IOException {
        this.fields = new EvaluationFields(in.readString(), in.readString(), null, null, null, false);
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
        out.writeString(fields.getPredictedField());
        out.writeNamedWriteableList(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), fields.getActualField());
        builder.field(PREDICTED_FIELD.getPreferredName(), fields.getPredictedField());

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
        Regression that = (Regression) o;
        return Objects.equals(that.fields, this.fields) && Objects.equals(that.metrics, this.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, metrics);
    }
}
