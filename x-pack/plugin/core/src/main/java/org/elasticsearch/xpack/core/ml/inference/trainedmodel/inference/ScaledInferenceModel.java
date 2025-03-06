/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.util.Map;

public class ScaledInferenceModel implements BoundedInferenceModel {

    public static final double DEFAULT_MIN_PREDICTED_VALUE = 0;
    public static final double DEFAULT_MAX_PREDICTED_VALUE = 1;

    private final BoundedInferenceModel model;
    private final double minPredictedValue;
    private final double maxPredictedValue;

    public ScaledInferenceModel(BoundedInferenceModel model) {
        this(model, DEFAULT_MIN_PREDICTED_VALUE, DEFAULT_MAX_PREDICTED_VALUE);
    }

    public ScaledInferenceModel(BoundedInferenceModel model, double minPredictedValue, double maxPredictedValue) {
        this.model = model;
        this.minPredictedValue = minPredictedValue;
        this.maxPredictedValue = maxPredictedValue;
    }

    @Override
    public String[] getFeatureNames() {
        return model.getFeatureNames();
    }

    @Override
    public TargetType targetType() {
        return model.targetType();
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        return scaleInferenceResult(model.infer(fields, config, featureDecoderMap));
    }

    @Override
    public InferenceResults infer(double[] features, InferenceConfig config) {
        return scaleInferenceResult(model.infer(features, config));
    }

    @Override
    public boolean supportsFeatureImportance() {
        return model.supportsFeatureImportance();
    }

    @Override
    public String getName() {
        return "scaled[" + model.getName() + "]";
    }

    @Override
    public void rewriteFeatureIndices(Map<String, Integer> newFeatureIndexMapping) {
        model.rewriteFeatureIndices(newFeatureIndexMapping);
    }

    @Override
    public long ramBytesUsed() {
        return model.ramBytesUsed();
    }

    @Override
    public double getMinPredictedValue() {
        return minPredictedValue;
    }

    @Override
    public double getMaxPredictedValue() {
        return maxPredictedValue;
    }

    private InferenceResults scaleInferenceResult(InferenceResults inferenceResult) {
        if (inferenceResult instanceof RegressionInferenceResults regressionInferenceResults) {
            double predictedValue = ((Number) regressionInferenceResults.predictedValue()).doubleValue();
            // First we scale the data to [0 ,1]
            predictedValue = (predictedValue - model.getMinPredictedValue()) / (model.getMaxPredictedValue() - model.getMinPredictedValue());

            // Then we scale the data to the desired interval
            predictedValue = predictedValue * (getMaxPredictedValue() - getMinPredictedValue()) + getMinPredictedValue();

            return new RegressionInferenceResults(predictedValue, inferenceResult.getResultsField(), ((RegressionInferenceResults) inferenceResult).getFeatureImportance());
        }

        throw new IllegalStateException(
            LoggerMessageFormat.format(
            "Model used within a {} should return a {} but got {} instead",
            ScaledInferenceModel.class.getSimpleName(),
            RegressionInferenceResults.class.getSimpleName(),
            inferenceResult.getClass().getSimpleName()
        ));
    }

    @Override
    public String toString() {
        return "ScaledInferenceModel{"
            + "model="
            + model
            + ", minPredictedValue="
            + getMinPredictedValue()
            + ", maxPredictedValue="
            + getMaxPredictedValue()
            + '}';
    }
}
