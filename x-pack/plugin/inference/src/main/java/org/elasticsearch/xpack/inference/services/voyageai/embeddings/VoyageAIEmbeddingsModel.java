/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.voyageai.VoyageAIActionVisitor;
import org.elasticsearch.xpack.inference.external.request.voyageai.VoyageAIUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.voyageai.VoyageAIUtils.HOST;

public class VoyageAIEmbeddingsModel extends VoyageAIModel {
    public static VoyageAIEmbeddingsModel of(VoyageAIEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var requestTaskSettings = VoyageAIEmbeddingsTaskSettings.fromMap(taskSettings);
        return new VoyageAIEmbeddingsModel(
            model,
            VoyageAIEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings, inputType)
        );
    }

    public VoyageAIEmbeddingsModel(
        String inferenceId,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            service,
            VoyageAIEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            VoyageAIEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    VoyageAIEmbeddingsModel(
        String modelId,
        String service,
        VoyageAIEmbeddingsServiceSettings serviceSettings,
        VoyageAIEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.TEXT_EMBEDDING, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings()
        );
    }

    VoyageAIEmbeddingsModel(
        String modelId,
        String service,
        String url,
        VoyageAIEmbeddingsServiceSettings serviceSettings,
        VoyageAIEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.TEXT_EMBEDDING, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            url
        );
    }

    private VoyageAIEmbeddingsModel(VoyageAIEmbeddingsModel model, VoyageAIEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public VoyageAIEmbeddingsModel(VoyageAIEmbeddingsModel model, VoyageAIEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public VoyageAIEmbeddingsServiceSettings getServiceSettings() {
        return (VoyageAIEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public VoyageAIEmbeddingsTaskSettings getTaskSettings() {
        return (VoyageAIEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(VoyageAIActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings, inputType);
    }

    protected URI buildRequestUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(HOST)
            .setPathSegments(VoyageAIUtils.VERSION_1, VoyageAIUtils.EMBEDDINGS_PATH)
            .build();
    }
}
