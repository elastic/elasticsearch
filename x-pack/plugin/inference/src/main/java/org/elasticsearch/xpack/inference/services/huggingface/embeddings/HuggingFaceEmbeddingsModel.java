/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.huggingface.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

public class HuggingFaceEmbeddingsModel extends HuggingFaceModel {
    public HuggingFaceEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            HuggingFaceServiceSettings.fromMap(serviceSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    HuggingFaceEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        HuggingFaceServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings), new ModelSecrets(secrets));
    }

    public HuggingFaceEmbeddingsModel(HuggingFaceEmbeddingsModel model, HuggingFaceServiceSettings serviceSettings) {
        this(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            serviceSettings,
            model.getSecretSettings()
        );
    }

    @Override
    public HuggingFaceServiceSettings getServiceSettings() {
        return (HuggingFaceServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public URI getUri() {
        return getServiceSettings().uri();
    }

    @Override
    public SecureString getApiKey() {
        return getSecretSettings().apiKey();
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
