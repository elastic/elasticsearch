/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

/**
 * Represents an Nvidia embeddings model for inference.
 * This class extends the NvidiaModel and provides specific configurations and settings for embeddings tasks.
 */
public class NvidiaRerankModel extends NvidiaModel {

    /**
     * Constructor for creating a NvidiaEmbeddingsModel with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public NvidiaRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            NvidiaRerankServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Constructor for creating a NvidiaEmbeddingsModel with specified parameters.
     *
     * @param model the base NvidiaEmbeddingsModel to copy properties from
     * @param serviceSettings the settings for the inference service, specific to embeddings
     */
    public NvidiaRerankModel(NvidiaRerankModel model, NvidiaRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Constructor for creating a NvidiaEmbeddingsModel with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public NvidiaRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        NvidiaRerankServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secrets)
        );
    }

    @Override
    public NvidiaRerankServiceSettings getServiceSettings() {
        return (NvidiaRerankServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Nvidia embeddings model.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing the Nvidia embeddings model
     */
    public ExecutableAction accept(NvidiaActionVisitor creator) {
        return creator.create(this);
    }
}
