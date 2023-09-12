/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;

import java.util.Map;

public interface InferenceService {

    String name();

    /**
     * Parse model configuration from the {@code config map} and return
     * the parsed {@link Model}.
     * This function modifies {@code config map}, fields are removed
     * from the map as they are read.
     *
     * If the map contains unrecognized configuration option an
     * {@code ElasticsearchStatusException} is thrown.
     *
     * @param modelId Model Id
     * @param taskType The model task type
     * @param config Configuration options
     * @return The parsed {@link Model}
     */
    Model parseConfigStrict(String modelId, TaskType taskType, Map<String, Object> config);

    /**
     * As {@link #parseConfigStrict(String, TaskType, Map)} but the function
     * does not throw on unrecognized options.
     *
     * @param modelId Model Id
     * @param taskType The model task type
     * @param config Configuration options
     * @return The parsed {@link Model}
     */
    Model parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> config);

    /**
     * Start or prepare the model for use.
     * @param model The model
     * @param listener The listener
     */
    void start(Model model, ActionListener<Boolean> listener);

    /**
     * Perform inference on the model.
     *
     * @param model Model configuration
     * @param input Inference input
     * @param requestTaskSettings Settings in the request to override the model's defaults
     * @param listener Inference result listener
     */
    void infer(Model model, String input, Map<String, Object> requestTaskSettings, ActionListener<InferenceResult> listener);
}
