/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudService;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.net.URI;
import java.util.Map;

/**
 * Represents a TencentCloud chat completion model for inference.
 * This class extends the {@link TencentCloudModel} and provides specific configurations and settings for chat completion
 * tasks against the TencentCloud AI Gateway.
 */
public class TencentCloudChatCompletionModel extends TencentCloudModel {

    /**
     * Constructor for creating a TencentCloudChatCompletionModel from raw configuration maps.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the raw service settings map, specific to chat completion
     * @param secrets the raw secret settings map for the model, such as the API key (may be null)
     * @param context the context for parsing configuration settings
     */
    public TencentCloudChatCompletionModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            TencentCloudChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    /**
     * Constructor for creating a TencentCloudChatCompletionModel with typed parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the service settings specific to chat completion
     * @param secretSettings the secret settings for the model, such as the API key (may be null)
     */
    public TencentCloudChatCompletionModel(
        String inferenceId,
        TaskType taskType,
        TencentCloudChatCompletionServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceId, taskType, TencentCloudService.NAME, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secretSettings),
            resolveUri(serviceSettings)
        );
    }

    /**
     * Constructor for creating a TencentCloudChatCompletionModel from model configurations and secrets.
     *
     * @param config the configurations for the model
     * @param secrets the secret settings for the model
     */
    public TencentCloudChatCompletionModel(ModelConfigurations config, ModelSecrets secrets) {
        super(config, secrets, resolveUri((TencentCloudChatCompletionServiceSettings) config.getServiceSettings()));
    }

    /**
     * Returns the upstream model identifier for this chat completion model.
     *
     * @return the model id from the service settings
     */
    public String model() {
        return getServiceSettings().modelId();
    }

    /**
     * Returns the service settings specific to TencentCloud chat completion.
     *
     * @return the TencentCloudChatCompletionServiceSettings associated with this model
     */
    @Override
    public TencentCloudChatCompletionServiceSettings getServiceSettings() {
        return (TencentCloudChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this TencentCloud chat completion model.
     *
     * @param visitor the visitor that creates the executable action
     * @param taskSettings the task-level settings for this request
     * @return an ExecutableAction representing this model
     */
    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static URI resolveUri(TencentCloudChatCompletionServiceSettings serviceSettings) {
        return TencentCloudUtils.buildUri(
            serviceSettings.region(),
            TencentCloudUtils.VERSION_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_2
        );
    }
}
