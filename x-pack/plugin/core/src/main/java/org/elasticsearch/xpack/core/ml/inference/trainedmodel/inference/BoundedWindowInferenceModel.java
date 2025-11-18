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

public class BoundedWindowInferenceModel implements BoundedInferenceModel {
    public static final double DEFAULT_MIN_PREDICTED_VALUE = 0;

    private final BoundedInferenceModel model;
    private final double minPredictedValue;
    private final double maxPredictedValue;
    private final double adjustmentValue;

    public BoundedWindowInferenceModel(BoundedInferenceModel model) {
        this.model = model;
        this.minPredictedValue = model.getMinPredictedValue();
        this.maxPredictedValue = model.getMaxPredictedValue();

        if (this.minPredictedValue < DEFAULT_MIN_PREDICTED_VALUE) {
            this.adjustmentValue = DEFAULT_MIN_PREDICTED_VALUE - this.minPredictedValue;
        } else {
            this.adjustmentValue = 0.0;
        }
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
        return boundInferenceResultScores(model.infer(fields, config, featureDecoderMap));
    }

    @Override
    public InferenceResults infer(double[] features, InferenceConfig config) {
        return boundInferenceResultScores(model.infer(features, config));
    }

    @Override
    public boolean supportsFeatureImportance() {
        return model.supportsFeatureImportance();
    }

    @Override
    public String getName() {
        return "bounded_window[" + model.getName() + "]";
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

    private InferenceResults boundInferenceResultScores(InferenceResults inferenceResult) {
        // if the min value < the default minimum, slide the values up by the adjustment value
        if (inferenceResult instanceof RegressionInferenceResults regressionInferenceResults) {
            double predictedValue = ((Number) regressionInferenceResults.predictedValue()).doubleValue();

            predictedValue += this.adjustmentValue;

            return new RegressionInferenceResults(
                predictedValue,
                inferenceResult.getResultsField(),
                ((RegressionInferenceResults) inferenceResult).getFeatureImportance()
            );
        }

        throw new IllegalStateException(
            LoggerMessageFormat.format(
                "Model used within a {} should return a {} but got {} instead",
                BoundedWindowInferenceModel.class.getSimpleName(),
                RegressionInferenceResults.class.getSimpleName(),
                inferenceResult.getClass().getSimpleName()
            )
        );
    }

    @Override
    public String toString() {
        return "BoundedWindowInferenceModel{"
            + "model="
            + model
            + ", minPredictedValue="
            + getMinPredictedValue()
            + ", maxPredictedValue="
            + getMaxPredictedValue()
            + '}';
    }
}
