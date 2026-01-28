/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiModel;
import org.elasticsearch.xpack.inference.services.openshiftai.action.OpenShiftAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

/**
 * Represents an OpenShift AI embeddings model for inference.
 * This class extends the {@link OpenShiftAiModel} and provides specific configurations and settings for embeddings tasks.
 */
public class OpenShiftAiEmbeddingsModel extends OpenShiftAiModel {

    public OpenShiftAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OpenShiftAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public OpenShiftAiEmbeddingsModel(OpenShiftAiEmbeddingsModel model, OpenShiftAiEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Constructor for creating an OpenShiftAiEmbeddingsModel with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public OpenShiftAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OpenShiftAiEmbeddingsServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    @Override
    public OpenShiftAiEmbeddingsServiceSettings getServiceSettings() {
        return (OpenShiftAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public ExecutableAction accept(OpenShiftAiActionVisitor creator, Map<String, Object> taskSettings) {
        // Embeddings models do not have task settings, so we ignore the taskSettings parameter.
        return creator.create(this);
    }
}
