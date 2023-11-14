/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InferenceService extends Closeable {

    String name();

    /**
     * Parse model configuration from the {@code config map} from a request and return
     * the parsed {@link Model}. This requires that both the secrets and service settings be contained in the
     * {@code service_settings} field.
     * This function modifies {@code config map}, fields are removed
     * from the map as they are read.
     *
     * If the map contains unrecognized configuration option an
     * {@code ElasticsearchStatusException} is thrown.
     *
     * @param modelId Model Id
     * @param taskType The model task type
     * @param config Configuration options including the secrets
     * @param platfromArchitectures The Set of platform architectures (OS name and hardware architecture)
     *                             the cluster nodes and models are running on.
     * @return The parsed {@link Model}
     */
    Model parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config, Set<String> platfromArchitectures);

    /**
     * Parse model configuration from {@code config map} from persisted storage and return the parsed {@link Model}. This requires that
     * secrets and service settings be in two separate maps.
     * This function modifies {@code config map}, fields are removed from the map as they are read.
     *
     * If the map contains unrecognized configuration options, no error is thrown.
     *
     * @param modelId Model Id
     * @param taskType The model task type
     * @param config Configuration options
     * @param secrets Sensitive configuration options (e.g. api key)
     * @return The parsed {@link Model}
     */
    Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config, Map<String, Object> secrets);

    /**
     * Perform inference on the model.
     *
     * @param model The model
     * @param input Inference input
     * @param taskSettings Settings in the request to override the model's defaults
     * @param listener Inference result listener
     */
    void infer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        ActionListener<List<? extends InferenceResults>> listener
    );

    /**
     * Start or prepare the model for use.
     * @param model The model
     * @param listener The listener
     */
    void start(Model model, ActionListener<Boolean> listener);

    /**
     * Return true if this model is hosted in the local Elasticsearch cluster
     * @return True if in cluster
     */
    default boolean isInClusterService() {
        return false;
    }

    /**
     * Defines the version required across all clusters to use this service
     * @return {@link TransportVersion} specifying the version
     */
    TransportVersion getMinimalSupportedVersion();
}
