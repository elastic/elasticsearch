/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaModel;
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionVisitor;

import java.util.Map;

public class LlamaEmbeddingsModel extends LlamaModel {

    public LlamaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            LlamaEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,    // no task settings for Llama embeddings
            chunkingSettings,
            retrieveSecretSettings(secrets)
        );
    }

    public LlamaEmbeddingsModel(LlamaEmbeddingsModel model, LlamaEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
        setPropertiesFromServiceSettings(serviceSettings);
    }

    private void setPropertiesFromServiceSettings(LlamaEmbeddingsServiceSettings serviceSettings) {
        this.modelId = serviceSettings.modelId();
        this.uri = serviceSettings.uri();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
    }

    public LlamaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        LlamaEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, chunkingSettings),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    @Override
    public LlamaEmbeddingsServiceSettings getServiceSettings() {
        return (LlamaEmbeddingsServiceSettings) super.getServiceSettings();
    }

    public ExecutableAction accept(LlamaActionVisitor creator) {
        return creator.create(this);
    }
}
