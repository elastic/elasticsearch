/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.action.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class HuggingFaceRerankModel extends HuggingFaceModel {
    public HuggingFaceRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            HuggingFaceRerankServiceSettings.fromMap(serviceSettings, context),
            HuggingFaceRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public HuggingFaceRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        HuggingFaceRerankServiceSettings serviceSettings,
        HuggingFaceRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            secrets
        );
    }

    @Override
    public HuggingFaceRerankServiceSettings getServiceSettings() {
        return (HuggingFaceRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public HuggingFaceRerankTaskSettings getTaskSettings() {
        return (HuggingFaceRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public Integer getTokenLimit() {
        return getServiceSettings().maxInputTokens();
    }

    @Override
    public ExecutableAction accept(HuggingFaceActionVisitor creator) {
        return creator.create(this);
    }
}
