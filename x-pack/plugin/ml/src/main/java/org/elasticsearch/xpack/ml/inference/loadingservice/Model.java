/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;

import java.util.Map;
import java.util.Set;

public interface Model {

    String getResultsType();

    default void infer(Map<String, Object> fields, InferenceConfig inferenceConfig, ActionListener<InferenceResults> listener) {
        try {
            listener.onResponse(infer(fields, inferenceConfig));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    InferenceResults infer(Map<String, Object> fields, InferenceConfig inferenceConfig);

    String getModelId();

    Set<String> getFieldNames();
}
