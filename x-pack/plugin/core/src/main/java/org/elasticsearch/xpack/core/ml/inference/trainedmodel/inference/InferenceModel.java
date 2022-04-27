/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.util.Map;

public interface InferenceModel extends Accountable {

    static double[] extractFeatures(String[] featureNames, Map<String, Object> fields) {
        double[] features = new double[featureNames.length];
        int i = 0;
        for (String featureName : featureNames) {
            Double val = InferenceHelpers.toDouble(fields.get(featureName));
            features[i++] = val == null ? Double.NaN : val;
        }
        return features;
    }

    /**
     * @return The feature names in their desired order
     */
    String[] getFeatureNames();

    /**
     * @return {@link TargetType} for the model.
     */
    TargetType targetType();

    /**
     * Infer against the provided fields
     *
     * @param fields The fields and their values to infer against
     * @param config The configuration options for inference
     * @param featureDecoderMap A map for decoding feature value names to their originating feature.
     *                          Necessary for feature influence.
     * @return The predicted value. For classification this will be discrete values (e.g. 0.0, or 1.0).
     *                              For regression this is continuous.
     */
    InferenceResults infer(Map<String, Object> fields, InferenceConfig config, @Nullable Map<String, String> featureDecoderMap);

    /**
     * Same as {@link InferenceModel#infer(Map, InferenceConfig, Map)} but the features are already extracted.
     */
    InferenceResults infer(double[] features, InferenceConfig config);

    /**
     * @return Does the model support feature importance
     */
    boolean supportsFeatureImportance();

    String getName();

    /**
     * Rewrites underlying feature index mappings.
     * This is to allow optimization of the underlying models.
     */
    void rewriteFeatureIndices(Map<String, Integer> newFeatureIndexMapping);

}
