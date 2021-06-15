/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Evaluation of classification results.
 */
public class Classification implements Evaluation {

    public static final String NAME = "classification";

    private static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    private static final ParseField PREDICTED_FIELD = new ParseField("predicted_field");
    private static final ParseField TOP_CLASSES_FIELD = new ParseField("top_classes_field");

    private static final ParseField METRICS = new ParseField("metrics");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Classification, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new Classification((String) a[0], (String) a[1], (String) a[2], (List<EvaluationMetric>) a[3]));

    static {
        PARSER.declareString(constructorArg(), ACTUAL_FIELD);
        PARSER.declareString(optionalConstructorArg(), PREDICTED_FIELD);
        PARSER.declareString(optionalConstructorArg(), TOP_CLASSES_FIELD);
        PARSER.declareNamedObjects(
            optionalConstructorArg(), (p, c, n) -> p.namedObject(EvaluationMetric.class, registeredMetricName(NAME, n), c), METRICS);
    }

    public static Classification fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * The field containing the actual value
     */
    private final String actualField;

    /**
     * The field containing the predicted value
     */
    private final String predictedField;

    /**
     * The field containing the array of top classes
     */
    private final String topClassesField;

    /**
     * The list of metrics to calculate
     */
    private final List<EvaluationMetric> metrics;

    public Classification(String actualField,
                          String predictedField,
                          String topClassesField) {
        this(actualField, predictedField, topClassesField, (List<EvaluationMetric>)null);
    }

    public Classification(String actualField,
                          String predictedField,
                          String topClassesField,
                          EvaluationMetric... metrics) {
        this(actualField, predictedField, topClassesField, Arrays.asList(metrics));
    }

    public Classification(String actualField,
                          @Nullable String predictedField,
                          @Nullable String topClassesField,
                          @Nullable List<EvaluationMetric> metrics) {
        this.actualField = Objects.requireNonNull(actualField);
        this.predictedField = predictedField;
        this.topClassesField = topClassesField;
        if (metrics != null) {
            metrics.sort(Comparator.comparing(EvaluationMetric::getName));
        }
        this.metrics = metrics;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTUAL_FIELD.getPreferredName(), actualField);
        if (predictedField != null) {
            builder.field(PREDICTED_FIELD.getPreferredName(), predictedField);
        }
        if (topClassesField != null) {
            builder.field(TOP_CLASSES_FIELD.getPreferredName(), topClassesField);
        }
        if (metrics != null) {
           builder.startObject(METRICS.getPreferredName());
           for (EvaluationMetric metric : metrics) {
               builder.field(metric.getName(), metric);
           }
           builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Classification that = (Classification) o;
        return Objects.equals(that.actualField, this.actualField)
            && Objects.equals(that.predictedField, this.predictedField)
            && Objects.equals(that.topClassesField, this.topClassesField)
            && Objects.equals(that.metrics, this.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualField, predictedField, topClassesField, metrics);
    }
}
