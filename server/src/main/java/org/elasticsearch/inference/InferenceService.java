/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InferenceService extends Closeable {

    default void init(Client client) {}

    String name();

    /**
     * Parse model configuration from the {@code config map} from a request and return
     * the parsed {@link Model}. This requires that both the secrets and service settings be contained in the
     * {@code service_settings} field.
     * This function modifies {@code config map}, fields are removed
     * from the map as they are read.
     * <p>
     * If the map contains unrecognized configuration option an
     * {@code ElasticsearchStatusException} is thrown.
     *
     * @param modelId               Model Id
     * @param taskType              The model task type
     * @param config                Configuration options including the secrets
     * @param platformArchitectures The Set of platform architectures (OS name and hardware architecture)
     *                              the cluster nodes and models are running on.
     * @param parsedModelListener   A listener which will handle the resulting model or failure
     */
    void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> parsedModelListener
    );

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
    Model parsePersistedConfigWithSecrets(String modelId, TaskType taskType, Map<String, Object> config, Map<String, Object> secrets);

    /**
     * Parse model configuration from {@code config map} from persisted storage and return the parsed {@link Model}.
     * This function modifies {@code config map}, fields are removed from the map as they are read.
     *
     * If the map contains unrecognized configuration options, no error is thrown.
     *
     * @param modelId Model Id
     * @param taskType The model task type
     * @param config Configuration options
     * @return The parsed {@link Model}
     */
    Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config);

    /**
     * Perform inference on the model.
     *
     * @param model        The model
     * @param query        Inference query, mainly for re-ranking
     * @param input        Inference input
     * @param stream       Stream inference results
     * @param taskSettings Settings in the request to override the model's defaults
     * @param inputType    For search, ingest etc
     * @param timeout      The timeout for the request
     * @param listener     Inference result listener
     */
    void infer(
        Model model,
        @Nullable String query,
        List<String> input,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    /**
     * Chunk long text according to {@code chunkingOptions} or the
     * model defaults if {@code chunkingOptions} contains unset
     * values.
     *
     * @param model           The model
     * @param query           Inference query, mainly for re-ranking
     * @param input           Inference input
     * @param taskSettings    Settings in the request to override the model's defaults
     * @param inputType       For search, ingest etc
     * @param chunkingOptions The window and span options to apply
     * @param timeout         The timeout for the request
     * @param listener        Chunked Inference result listener
     */
    void chunkedInfer(
        Model model,
        @Nullable String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    );

    /**
     * Start or prepare the model for use.
     * @param model The model
     * @param listener The listener
     */
    void start(Model model, ActionListener<Boolean> listener);

    /**
     * Stop the model deployment.
     * The default action does nothing except acknowledge the request (true).
     * @param modelId The ID of the model to be stopped
     * @param listener The listener
     */
    default void stop(String modelId, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    /**
     * Put the model definition (if applicable)
     * The main purpose of this function is to download ELSER
     * The default action does nothing except acknowledge the request (true).
     * @param modelVariant The configuration of the model variant to be downloaded
     * @param listener The listener
     */
    default void putModel(Model modelVariant, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    /**
     * Checks if the modelId has been downloaded to the local Elasticsearch cluster using the trained models API
     * The default action does nothing except acknowledge the request (false).
     * Any internal services should Override this method.
     * @param model
     * @param listener The listener
     */
    default void isModelDownloaded(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(false);
    };

    /**
     * Optionally test the new model configuration in the inference service.
     * This function should be called when the model is first created, the
     * default action is to do nothing.
     * @param model The new model
     * @param listener The listener
     */
    default void checkModelConfig(Model model, ActionListener<Model> listener) {
        listener.onResponse(model);
    };

    /**
     * Update a text embedding model's dimensions based on a provided embedding
     * size and set the default similarity if required. The default behaviour is to just return the model.
     * @param model The original model without updated embedding details
     * @param embeddingSize The embedding size to update the model with
     * @return The model with updated embedding details
     */
    default Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        return model;
    }

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

    /**
     * The set of tasks where this service provider supports using the streaming API.
     * @return set of supported task types. Defaults to empty.
     */
    default Set<TaskType> supportedStreamingTasks() {
        return Set.of();
    }

    /**
     * Checks the task type against the set of supported streaming tasks returned by {@link #supportedStreamingTasks()}.
     * @param taskType the task that supports streaming
     * @return true if the taskType is supported
     */
    default boolean canStream(TaskType taskType) {
        return supportedStreamingTasks().contains(taskType);
    }
}
