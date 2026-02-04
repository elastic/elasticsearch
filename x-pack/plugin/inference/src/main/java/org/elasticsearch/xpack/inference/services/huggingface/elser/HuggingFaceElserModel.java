/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.action.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class HuggingFaceElserModel extends HuggingFaceModel {
    public HuggingFaceElserModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            HuggingFaceElserServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public HuggingFaceElserModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        HuggingFaceElserServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings), new ModelSecrets(secretSettings));
    }

    public HuggingFaceElserModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(
            modelConfigurations,
            modelSecrets,
            (HuggingFaceElserServiceSettings) modelConfigurations.getServiceSettings(),
            (DefaultSecretSettings) modelSecrets.getSecretSettings()
        );
    }

    @Override
    public HuggingFaceElserServiceSettings getServiceSettings() {
        return (HuggingFaceElserServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(HuggingFaceActionVisitor creator) {
        return creator.create(this);
    }

    @Override
    public Integer getTokenLimit() {
        return getServiceSettings().maxInputTokens();
    }
}
