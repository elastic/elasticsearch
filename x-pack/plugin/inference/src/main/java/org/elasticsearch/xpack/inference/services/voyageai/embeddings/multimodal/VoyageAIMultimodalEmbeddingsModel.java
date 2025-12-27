/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIUtils.HOST;

public class VoyageAIMultimodalEmbeddingsModel extends VoyageAIModel {
    public static VoyageAIMultimodalEmbeddingsModel of(VoyageAIMultimodalEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = VoyageAIMultimodalEmbeddingsTaskSettings.fromMap(taskSettings);
        return new VoyageAIMultimodalEmbeddingsModel(
            model, 
            VoyageAIMultimodalEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public VoyageAIMultimodalEmbeddingsModel(
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
            VoyageAIMultimodalEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            VoyageAIMultimodalEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets),
            buildUri(VoyageAIService.NAME, VoyageAIMultimodalEmbeddingsModel::buildRequestUri)
        );
    }

    public static URI buildRequestUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(HOST)
            .setPathSegments(VoyageAIUtils.VERSION_1, VoyageAIUtils.MULTIMODAL_EMBEDDINGS_PATH)
            .build();
    }

    // should only be used for testing
    VoyageAIMultimodalEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        String service,
        String url,
        VoyageAIMultimodalEmbeddingsServiceSettings serviceSettings,
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, ServiceUtils.createUri(url));
    }

    private VoyageAIMultimodalEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        String service,
        VoyageAIMultimodalEmbeddingsServiceSettings serviceSettings,
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings,
        URI uri
    ) {
        super(
            new ModelConfigurations(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            uri
        );
    }

    private VoyageAIMultimodalEmbeddingsModel(
        VoyageAIMultimodalEmbeddingsModel model, 
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings
    ) {
        super(model, taskSettings);
    }

    public VoyageAIMultimodalEmbeddingsModel(
        VoyageAIMultimodalEmbeddingsModel model, 
        VoyageAIMultimodalEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    @Override
    public VoyageAIMultimodalEmbeddingsServiceSettings getServiceSettings() {
        return (VoyageAIMultimodalEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public VoyageAIMultimodalEmbeddingsTaskSettings getTaskSettings() {
        return (VoyageAIMultimodalEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

}
