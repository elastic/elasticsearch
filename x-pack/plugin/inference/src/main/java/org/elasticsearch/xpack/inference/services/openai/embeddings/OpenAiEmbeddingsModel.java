/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUtils;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class OpenAiEmbeddingsModel extends OpenAiModel {

    public static OpenAiEmbeddingsModel of(OpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new OpenAiEmbeddingsModel(model, OpenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public OpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            OpenAiEmbeddingsTaskSettings.fromMap(taskSettings, context),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    OpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OpenAiEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            secrets,
            buildUri(serviceSettings.uri(), OpenAiService.NAME, OpenAiEmbeddingsModel::buildDefaultUri)
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(OpenAiUtils.HOST)
            .setPathSegments(OpenAiUtils.VERSION_1, OpenAiUtils.EMBEDDINGS_PATH)
            .build();
    }

    private OpenAiEmbeddingsModel(OpenAiEmbeddingsModel originalModel, OpenAiEmbeddingsTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    public OpenAiEmbeddingsModel(OpenAiEmbeddingsModel originalModel, OpenAiEmbeddingsServiceSettings serviceSettings) {
        super(originalModel, serviceSettings);
    }

    @Override
    public OpenAiEmbeddingsServiceSettings getServiceSettings() {
        return (OpenAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
