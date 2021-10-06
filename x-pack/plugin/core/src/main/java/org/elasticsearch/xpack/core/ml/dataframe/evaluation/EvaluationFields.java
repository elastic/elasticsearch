/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates fields needed by evaluation.
 */
public final class EvaluationFields {

    public static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    public static final ParseField PREDICTED_FIELD = new ParseField("predicted_field");
    public static final ParseField TOP_CLASSES_FIELD = new ParseField("top_classes_field");
    public static final ParseField PREDICTED_CLASS_FIELD = new ParseField("predicted_class_field");
    public static final ParseField PREDICTED_PROBABILITY_FIELD = new ParseField("predicted_probability_field");

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
     * The field containing the predicted class name value
     */
    private final String predictedClassField;

    /**
     * The field containing the predicted probability value in [0.0, 1.0]
     */
    private final String predictedProbabilityField;

    /**
     * Whether the {@code predictedProbabilityField} should be treated as nested (e.g.: when used in exists queries).
     */
    private final boolean predictedProbabilityFieldNested;

    public EvaluationFields(@Nullable String actualField,
                            @Nullable String predictedField,
                            @Nullable String topClassesField,
                            @Nullable String predictedClassField,
                            @Nullable String predictedProbabilityField,
                            boolean predictedProbabilityFieldNested) {

        this.actualField = actualField;
        this.predictedField = predictedField;
        this.topClassesField = topClassesField;
        this.predictedClassField = predictedClassField;
        this.predictedProbabilityField = predictedProbabilityField;
        this.predictedProbabilityFieldNested = predictedProbabilityFieldNested;
    }

    /**
     * Returns the field containing the actual value
     */
    public String getActualField() {
        return actualField;
    }

    /**
     * Returns the field containing the predicted value
     */
    public String getPredictedField() {
        return predictedField;
    }

    /**
     * Returns the field containing the array of top classes
     */
    public String getTopClassesField() {
        return topClassesField;
    }

    /**
     * Returns the field containing the predicted class name value
     */
    public String getPredictedClassField() {
        return predictedClassField;
    }

    /**
     * Returns the field containing the predicted probability value in [0.0, 1.0]
     */
    public String getPredictedProbabilityField() {
        return predictedProbabilityField;
    }

    /**
     * Returns whether the {@code predictedProbabilityField} should be treated as nested (e.g.: when used in exists queries).
     */
    public boolean isPredictedProbabilityFieldNested() {
        return predictedProbabilityFieldNested;
    }

    public List<Tuple<String, String>> listPotentiallyRequiredFields() {
        return Arrays.asList(
            Tuple.tuple(ACTUAL_FIELD.getPreferredName(), actualField),
            Tuple.tuple(PREDICTED_FIELD.getPreferredName(), predictedField),
            Tuple.tuple(TOP_CLASSES_FIELD.getPreferredName(), topClassesField),
            Tuple.tuple(PREDICTED_CLASS_FIELD.getPreferredName(), predictedClassField),
            Tuple.tuple(PREDICTED_PROBABILITY_FIELD.getPreferredName(), predictedProbabilityField));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvaluationFields that = (EvaluationFields) o;
        return Objects.equals(that.actualField, this.actualField)
            && Objects.equals(that.predictedField, this.predictedField)
            && Objects.equals(that.topClassesField, this.topClassesField)
            && Objects.equals(that.predictedClassField, this.predictedClassField)
            && Objects.equals(that.predictedProbabilityField, this.predictedProbabilityField)
            && Objects.equals(that.predictedProbabilityFieldNested, this.predictedProbabilityFieldNested);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            actualField, predictedField, topClassesField, predictedClassField, predictedProbabilityField, predictedProbabilityFieldNested);
    }
}
