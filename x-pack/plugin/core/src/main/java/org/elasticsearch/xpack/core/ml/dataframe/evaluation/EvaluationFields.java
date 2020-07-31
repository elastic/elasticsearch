/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;
import java.util.Objects;

/**
 * Encapsulates fields needed by evaluation.
 */
public final class EvaluationFields {

    public static final ParseField ACTUAL_FIELD = new ParseField("actual_field");
    public static final ParseField PREDICTED_FIELD = new ParseField("predicted_field");
    public static final ParseField RESULTS_NESTED_FIELD = new ParseField("results_nested_field");
    public static final ParseField PREDICTED_CLASS_NAME_FIELD = new ParseField("predicted_class_name_field");
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
     * The field containing the array of prediction results
     */
    private final String resultsNestedField;

    /**
     * The field containing the predicted class name value
     */
    private final String predictedClassNameField;

    /**
     * The field containing the predicted probability value in [0.0, 1.0]
     */
    private final String predictedProbabilityField;

    public EvaluationFields(@Nullable String actualField,
                            @Nullable String predictedField,
                            @Nullable String resultsNestedField,
                            @Nullable String predictedClassNameField,
                            @Nullable String predictedProbabilityField) {

        this.actualField = actualField;
        this.predictedField = predictedField;
        this.resultsNestedField = resultsNestedField;
        this.predictedClassNameField = predictedClassNameField;
        this.predictedProbabilityField = predictedProbabilityField;
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
     * Returns the field containing the array of prediction results
     */
    public String getResultsNestedField() {
        return resultsNestedField;
    }

    /**
     * Returns the field containing the predicted class name value
     */
    public String getPredictedClassNameField() {
        return predictedClassNameField;
    }

    /**
     * Returns the field containing the predicted probability value in [0.0, 1.0]
     */
    public String getPredictedProbabilityField() {
        return predictedProbabilityField;
    }

    public List<Tuple<String, String>> listAll() {
        return List.of(
            Tuple.tuple(ACTUAL_FIELD.getPreferredName(), actualField),
            Tuple.tuple(PREDICTED_FIELD.getPreferredName(), predictedField),
            Tuple.tuple(RESULTS_NESTED_FIELD.getPreferredName(), resultsNestedField),
            Tuple.tuple(PREDICTED_CLASS_NAME_FIELD.getPreferredName(), predictedClassNameField),
            Tuple.tuple(PREDICTED_PROBABILITY_FIELD.getPreferredName(), predictedProbabilityField));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvaluationFields that = (EvaluationFields) o;
        return Objects.equals(that.actualField, this.actualField)
            && Objects.equals(that.predictedField, this.predictedField)
            && Objects.equals(that.resultsNestedField, this.resultsNestedField)
            && Objects.equals(that.predictedClassNameField, this.predictedClassNameField)
            && Objects.equals(that.predictedProbabilityField, this.predictedProbabilityField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualField, predictedField, resultsNestedField, predictedClassNameField, predictedProbabilityField);
    }
}
