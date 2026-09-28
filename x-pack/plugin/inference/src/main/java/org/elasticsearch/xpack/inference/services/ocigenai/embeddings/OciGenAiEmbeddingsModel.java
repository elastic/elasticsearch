/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiModel;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.action.OciGenAiActionVisitor;

import java.util.Map;
import java.util.function.BiConsumer;

public class OciGenAiEmbeddingsModel extends OciGenAiModel {

    public static OciGenAiEmbeddingsModel of(OciGenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = OciGenAiEmbeddingsTaskSettings.fromMap(taskSettings);
        return new OciGenAiEmbeddingsModel(model, OciGenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public OciGenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OciGenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            OciGenAiEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            OciGenAiSecretSettings.fromMap(secrets, context)
        );
    }

    public OciGenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OciGenAiEmbeddingsServiceSettings serviceSettings,
        OciGenAiEmbeddingsTaskSettings taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable OciGenAiSecretSettings secrets
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    public OciGenAiEmbeddingsModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, OciGenAiUtils.EMBED_TEXT);
    }

    // Should only be used directly for testing. Allows overriding the URL and bypassing request signing.
    public OciGenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        @Nullable String url,
        OciGenAiEmbeddingsServiceSettings serviceSettings,
        OciGenAiEmbeddingsTaskSettings taskSettings,
        @Nullable OciGenAiSecretSettings secrets,
        BiConsumer<HttpPost, OciGenAiModel> requestSigner
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            OciGenAiUtils.EMBED_TEXT,
            url,
            requestSigner
        );
    }

    public OciGenAiEmbeddingsModel(OciGenAiEmbeddingsModel model, OciGenAiEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    private OciGenAiEmbeddingsModel(OciGenAiEmbeddingsModel model, OciGenAiEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public OciGenAiEmbeddingsServiceSettings getServiceSettings() {
        return (OciGenAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public OciGenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OciGenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public ExecutableAction accept(OciGenAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
