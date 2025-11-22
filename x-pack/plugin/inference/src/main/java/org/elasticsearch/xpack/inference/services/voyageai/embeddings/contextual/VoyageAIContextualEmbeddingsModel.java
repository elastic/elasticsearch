/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual;

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

public class VoyageAIContextualEmbeddingsModel extends VoyageAIModel {
    public static VoyageAIContextualEmbeddingsModel of(VoyageAIContextualEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = VoyageAIContextualEmbeddingsTaskSettings.fromMap(taskSettings);
        return new VoyageAIContextualEmbeddingsModel(
            model, 
            VoyageAIContextualEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public VoyageAIContextualEmbeddingsModel(
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
            VoyageAIContextualEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            VoyageAIContextualEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets),
            buildUri(VoyageAIService.NAME, VoyageAIContextualEmbeddingsModel::buildRequestUri)
        );
    }

    public static URI buildRequestUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(HOST)
            .setPathSegments(VoyageAIUtils.VERSION_1, VoyageAIUtils.CONTEXTUALIZED_EMBEDDINGS_PATH)
            .build();
    }

    // should only be used for testing
    VoyageAIContextualEmbeddingsModel(
        String inferenceId,
        String service,
        String url,
        VoyageAIContextualEmbeddingsServiceSettings serviceSettings,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(inferenceId, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, ServiceUtils.createUri(url));
    }

    private VoyageAIContextualEmbeddingsModel(
        String inferenceId,
        String service,
        VoyageAIContextualEmbeddingsServiceSettings serviceSettings,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings,
        URI uri
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.TEXT_EMBEDDING, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            uri
        );
    }

    private VoyageAIContextualEmbeddingsModel(
        VoyageAIContextualEmbeddingsModel model, 
        VoyageAIContextualEmbeddingsTaskSettings taskSettings
    ) {
        super(model, taskSettings);
    }

    public VoyageAIContextualEmbeddingsModel(
        VoyageAIContextualEmbeddingsModel model, 
        VoyageAIContextualEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    @Override
    public VoyageAIContextualEmbeddingsServiceSettings getServiceSettings() {
        return (VoyageAIContextualEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public VoyageAIContextualEmbeddingsTaskSettings getTaskSettings() {
        return (VoyageAIContextualEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

}
