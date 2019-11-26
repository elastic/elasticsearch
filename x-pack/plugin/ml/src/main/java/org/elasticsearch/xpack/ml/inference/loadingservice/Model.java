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

public interface Model {

    String getResultsType();

    /**
     * Infers against the defined model and alerts the listener of the result
     * @param fields The field map indicating the values to infer against
     * @param inferenceConfig The configuration containing inference type specific parameters
     * @param allowMissingFields When {@code false}, an error is returned to the listener when fields are missing
     * @param listener The listener to alert when inference is complete.
     */
    void infer(Map<String, Object> fields,
               InferenceConfig inferenceConfig,
               boolean allowMissingFields,
               ActionListener<InferenceResults> listener);

    String getModelId();
}
