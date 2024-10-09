/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.cohere.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

public class CohereEmbeddingsModel extends CohereModel {
    public static CohereEmbeddingsModel of(CohereEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var requestTaskSettings = CohereEmbeddingsTaskSettings.fromMap(taskSettings);
        return new CohereEmbeddingsModel(model, CohereEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings, inputType));
    }

    public CohereEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            service,
            CohereEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            CohereEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    CohereEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        CohereEmbeddingsServiceSettings serviceSettings,
        CohereEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings()
        );
    }

    private CohereEmbeddingsModel(CohereEmbeddingsModel model, CohereEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public CohereEmbeddingsModel(CohereEmbeddingsModel model, CohereEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public CohereEmbeddingsServiceSettings getServiceSettings() {
        return (CohereEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public CohereEmbeddingsTaskSettings getTaskSettings() {
        return (CohereEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(CohereActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings, inputType);
    }

    @Override
    public URI uri() {
        return getServiceSettings().getCommonSettings().uri();
    }
}
