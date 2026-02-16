/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadModel;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

/**
 * Represents a Mixedbread rerank model for inference.
 * This class extends the {@link MixedbreadModel} and provides specific configurations and settings for rerank tasks.
 */
public class MixedbreadRerankModel extends MixedbreadModel {
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(MixedbreadUtils.HOST)
        .setPathSegments(MixedbreadUtils.VERSION_1, MixedbreadUtils.RERANK_PATH);

    /**
     * Creates a new {@link MixedbreadRerankModel} with updated task settings if they differ from the existing ones.
     *
     * @param model the existing Mixedbread rerank model
     * @param taskSettings the task-specific settings to be applied
     * @return a new {@link MixedbreadRerankModel} with updated task settings, or the original model if settings are unchanged
     */
    public static MixedbreadRerankModel of(MixedbreadRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = MixedbreadRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new MixedbreadRerankModel(model, MixedbreadRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    /**
     * Constructor for creating an {@link MixedbreadRerankModel} with specified parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the settings for the inference service, specific to reranking
     * @param taskSettings the task-specific settings to be applied
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public MixedbreadRerankModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            MixedbreadRerankServiceSettings.fromMap(serviceSettings, context),
            MixedbreadRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets),
            null
        );
    }

    /**
     * Constructor for creating an {@link MixedbreadRerankModel} with specified parameters.
     * Should only be used for testing
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the settings for the inference service, specific to reranking
     * @param taskSettings the task-specific settings to be applied
     * @param secretSettings the secret settings for the model, such as API keys or tokens
     */
    MixedbreadRerankModel(
        String inferenceId,
        MixedbreadRerankServiceSettings serviceSettings,
        MixedbreadRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings,
        @Nullable String uri
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.RERANK, MixedbreadService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            Objects.requireNonNullElse(ServiceUtils.createOptionalUri(uri), buildUri("Mixedbread", DEFAULT_URI_BUILDER::build))
        );
    }

    /**
     * Constructor for creating an {@link MixedbreadRerankModel} from model configurations and secrets.
     *
     * @param modelConfigurations the configurations for the model
     * @param modelSecrets the secret settings for the model
     */
    public MixedbreadRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, buildUri("Mixedbread", DEFAULT_URI_BUILDER::build));
    }

    /**
     * Constructor for creating a {@link MixedbreadRerankModel} with specified parameters.
     *
     * @param model the base {@link MixedbreadRerankModel} to copy properties from
     * @param taskSettings the task-specific settings to be applied
     */
    public MixedbreadRerankModel(MixedbreadRerankModel model, MixedbreadRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    @Override
    public MixedbreadRerankServiceSettings getServiceSettings() {
        return (MixedbreadRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public MixedbreadRerankTaskSettings getTaskSettings() {
        return (MixedbreadRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Mixedbread rerank model.
     *
     * @param visitor the visitor that creates the executable action
     * @param taskSettings the task-specific settings to be applied
     * @return an {@link ExecutableAction} representing the Mixedbread rerank model
     */
    @Override
    public ExecutableAction accept(MixedbreadActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
