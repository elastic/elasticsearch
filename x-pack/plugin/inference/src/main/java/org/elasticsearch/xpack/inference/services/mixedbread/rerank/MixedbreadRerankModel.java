/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

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

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class MixedbreadRerankModel extends MixedbreadModel {
    public static MixedbreadRerankModel of(MixedbreadRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = MixedbreadRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new MixedbreadRerankModel(model, MixedbreadRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

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

    // should only be used for testing
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
            secretSettings,
            Objects.requireNonNullElse(
                ServiceUtils.createOptionalUri(uri),
                buildUri(
                    MixedbreadService.SERVICE_NAME,
                    MixedbreadUtils.DEFAULT_URI_BUILDER.setPathSegments(MixedbreadUtils.VERSION_1, MixedbreadUtils.RERANK_PATH)::build
                )
            )
        );
    }

    /**
     * Constructor for creating an {@link MixedbreadRerankModel} from model configurations and secrets.
     *
     * @param modelConfigurations the configurations for the model
     * @param modelSecrets the secret settings for the model
     */
    public MixedbreadRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, (DefaultSecretSettings) modelSecrets.getSecretSettings(), null);
    }

    public MixedbreadRerankModel(MixedbreadRerankModel model, MixedbreadRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public MixedbreadRerankServiceSettings getServiceSettings() {
        return super.getServiceSettings();
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
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for Mixedbread models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(MixedbreadActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
