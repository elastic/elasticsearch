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
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;

import java.io.Closeable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InferenceService extends Closeable {

    default void init(Client client) {}

    String name();

    /**
     * The aliases that map to {@link #name()}. {@link InferenceServiceRegistry} allows users to create and use inference services by one
     * of their aliases.
     */
    default List<String> aliases() {
        return List.of();
    }

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
     * @param parsedModelListener   A listener which will handle the resulting model or failure
     */
    void parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config, ActionListener<Model> parsedModelListener);

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

    InferenceServiceConfiguration getConfiguration();

    /**
     * Whether this service should be hidden from the API. Should be used for services
     * that are not ready to be used.
     */
    default boolean hideFromConfigurationApi() {
        return false;
    }

    /**
     * The task types supported by the service
     * @return Set of supported.
     */
    EnumSet<TaskType> supportedTaskTypes();

    /**
     * Perform inference on the model.
     *
     * @param model           The model
     * @param query           Inference query, mainly for re-ranking
     * @param returnDocuments For re-ranking task type, whether to return documents
     * @param topN            For re-ranking task type, how many docs to return
     * @param input           Inference input
     * @param stream          Stream inference results
     * @param taskSettings    Settings in the request to override the model's defaults
     * @param inputType       For search, ingest etc
     * @param timeout         The timeout for the request
     * @param listener        Inference result listener
     */
    void infer(
        Model model,
        @Nullable String query,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        List<String> input,
        boolean stream,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    /**
     * Perform completion inference on the model using the unified schema.
     *
     * @param model        The model
     * @param request Parameters for the request
     * @param timeout      The timeout for the request
     * @param listener     Inference result listener
     */
    void unifiedCompletionInfer(
        Model model,
        UnifiedCompletionRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    /**
     * Chunk long text.
     *
     * @param model            The model
     * @param query            Inference query, mainly for re-ranking
     * @param input            Inference input
     * @param taskSettings     Settings in the request to override the model's defaults
     * @param inputType        For search, ingest etc
     * @param timeout          The timeout for the request
     * @param listener         Chunked Inference result listener
     */
    void chunkedInfer(
        Model model,
        @Nullable String query,
        List<ChunkInferenceInput> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    );

    /**
     * Start or prepare the model for use.
     * @param model The model
     * @param timeout Start timeout
     * @param listener The listener
     */
    void start(Model model, TimeValue timeout, ActionListener<Boolean> listener);

    /**
     * Stop the model deployment.
     * The default action does nothing except acknowledge the request (true).
     * @param model The model configuration
     * @param listener The listener
     */
    default void stop(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

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
     * Update a chat completion model's max tokens if required. The default behaviour is to just return the model.
     * @param model The original model without updated embedding details
     * @return The model with updated chat completion details
     */
    default Model updateModelWithChatCompletionDetails(Model model) {
        return model;
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

    record DefaultConfigId(String inferenceId, MinimalServiceSettings settings, InferenceService service) {};

    /**
     * Get the Ids and task type of any default configurations provided by this service
     * @return Defaults
     */
    default List<DefaultConfigId> defaultConfigIds() {
        return List.of();
    }

    /**
     * Call the listener with the default model configurations defined by
     * the service
     * @param defaultsListener The listener
     */
    default void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
        defaultsListener.onResponse(List.of());
    }

    default void updateModelsWithDynamicFields(List<Model> model, ActionListener<List<Model>> listener) {
        listener.onResponse(model);
    }

    /**
     * Called after the Elasticsearch node has completed its start up. This allows the service to perform initialization
     * after ensuring the node's internals are set up (for example if this ensures the internal ES client is ready for use).
     */
    default void onNodeStarted() {}

    /**
     * Get the service integration validator for the given task type.
     * This allows services to provide custom validation logic.
     * @param taskType The task type
     * @return The service integration validator or null if the default should be used
     */
    default ServiceIntegrationValidator getServiceIntegrationValidator(TaskType taskType) {
        return null;
    }
}
