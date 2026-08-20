/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.core.Nullable;
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
 * Represents a TencentCloud rerank model for inference.
 * This class extends the {@link TencentCloudModel} and provides specific configurations and settings for rerank
 * tasks against the TencentCloud AI Gateway.
 */
public class TencentCloudRerankModel extends TencentCloudModel {

    /**
     * Returns a TencentCloudRerankModel with rerank task settings overridden by the request, or the original model
     * when no overrides are present.
     *
     * @param model the original TencentCloudRerankModel
     * @param taskSettings the raw task settings map from the rerank request, used to override the model's task settings
     * @return a new TencentCloudRerankModel with overridden task settings, or the original model if no overrides apply
     */
    public static TencentCloudRerankModel of(TencentCloudRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = TencentCloudRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new TencentCloudRerankModel(model, TencentCloudRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    /**
     * Constructor for creating a TencentCloudRerankModel from raw configuration maps.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the raw service settings map, specific to rerank
     * @param taskSettings the raw task settings map for the rerank request
     * @param secrets the raw secret settings map for the model, such as the API key (may be null)
     * @param context the context for parsing configuration settings
     */
    public TencentCloudRerankModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            TencentCloudRerankServiceSettings.fromMap(serviceSettings, context),
            TencentCloudRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    /**
     * Constructor for creating a TencentCloudRerankModel with typed parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the service settings specific to rerank
     * @param taskSettings the task settings for the rerank request
     * @param secretSettings the secret settings for the model, such as the API key (may be null)
     */
    public TencentCloudRerankModel(
        String inferenceId,
        TencentCloudRerankServiceSettings serviceSettings,
        TencentCloudRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.RERANK, TencentCloudService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            resolveUri(serviceSettings)
        );
    }

    /**
     * Constructor for creating a TencentCloudRerankModel from model configurations and secrets.
     *
     * @param config the configurations for the model
     * @param secrets the secret settings for the model
     */
    public TencentCloudRerankModel(ModelConfigurations config, ModelSecrets secrets) {
        super(config, secrets, resolveUri((TencentCloudRerankServiceSettings) config.getServiceSettings()));
    }

    /**
     * Private constructor for creating a TencentCloudRerankModel by copying an existing model with new task settings.
     *
     * @param model the base TencentCloudRerankModel to copy properties from
     * @param taskSettings the new task settings to apply
     */
    private TencentCloudRerankModel(TencentCloudRerankModel model, TencentCloudRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    /**
     * Constructor for creating a TencentCloudRerankModel by copying an existing model with new service settings.
     *
     * @param model the base TencentCloudRerankModel to copy properties from
     * @param serviceSettings the new service settings to apply
     */
    public TencentCloudRerankModel(TencentCloudRerankModel model, TencentCloudRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Returns the service settings specific to TencentCloud rerank.
     *
     * @return the TencentCloudRerankServiceSettings associated with this model
     */
    @Override
    public TencentCloudRerankServiceSettings getServiceSettings() {
        return (TencentCloudRerankServiceSettings) super.getServiceSettings();
    }

    /**
     * Returns the task settings specific to TencentCloud rerank.
     *
     * @return the TencentCloudRerankTaskSettings associated with this model
     */
    @Override
    public TencentCloudRerankTaskSettings getTaskSettings() {
        return (TencentCloudRerankTaskSettings) super.getTaskSettings();
    }

    /**
     * Returns the secret settings for this model.
     *
     * @return the DefaultSecretSettings associated with this model
     */
    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this TencentCloud rerank model.
     *
     * @param visitor the visitor that creates the executable action
     * @param taskSettings the task-level settings for this request
     * @return an ExecutableAction representing this model
     */
    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static URI resolveUri(TencentCloudRerankServiceSettings serviceSettings) {
        return TencentCloudUtils.buildUri(serviceSettings.region(), TencentCloudUtils.VERSION_1, TencentCloudUtils.RERANK_PATH);
    }
}
