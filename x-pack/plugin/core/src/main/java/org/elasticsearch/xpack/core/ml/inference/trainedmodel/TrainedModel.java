/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.util.Map;

public interface TrainedModel extends NamedXContentObject, NamedWriteable, Accountable {

    /**
     * Infer against the provided fields
     *
     * NOTE: Must be thread safe
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
     * @return {@link TargetType} for the model.
     */
    TargetType targetType();

    /**
     * Runs validations against the model.
     *
     * Example: {@link org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree} should check if there are any loops
     *
     * @throws org.elasticsearch.ElasticsearchException if validations fail
     */
    void validate();

    /**
     * @return The estimated number of operations required at inference time
     */
    long estimatedNumOperations();

    /**
     * @return Does the model support feature importance
     */
    boolean supportsFeatureImportance();

    /**
     * Calculates the importance of each feature reference by the model for the passed in field values
     *
     * NOTE: Must be thread safe
     * @param fields The fields inferring against
     * @param featureDecoder A Map translating processed feature names to their original feature names
     * @return A {@code Map<String, double[]>} mapping each featureName to its importance
     */
    Map<String, double[]> featureImportance(Map<String, Object> fields, Map<String, String> featureDecoder);

    default Version getMinimalCompatibilityVersion() {
        return Version.V_7_6_0;
    }
}
