/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIModel;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.jinaai.action.JinaAIActionVisitor;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class JinaAIEmbeddingsModel extends JinaAIModel {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(JinaAIUtils.HOST)
        .setPathSegments(JinaAIUtils.VERSION_1, JinaAIUtils.EMBEDDINGS_PATH);

    public static JinaAIEmbeddingsModel of(JinaAIEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = JinaAIEmbeddingsTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new JinaAIEmbeddingsModel(model, JinaAIEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public JinaAIEmbeddingsModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            JinaAIEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            JinaAIEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets),
            null
        );
    }

    // should only be used for testing
    JinaAIEmbeddingsModel(
        String modelId,
        JinaAIEmbeddingsServiceSettings serviceSettings,
        JinaAIEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings,
        @Nullable String uri
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.TEXT_EMBEDDING, JinaAIService.NAME, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            uri
        );
    }

    private JinaAIEmbeddingsModel(JinaAIEmbeddingsModel model, JinaAIEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public JinaAIEmbeddingsModel(JinaAIEmbeddingsModel model, JinaAIEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public JinaAIEmbeddingsServiceSettings getServiceSettings() {
        return (JinaAIEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public JinaAIEmbeddingsTaskSettings getTaskSettings() {
        return (JinaAIEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(JinaAIActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public URI getDefaultUri() {
        return buildUri("JinaAI", DEFAULT_URI_BUILDER::build);
    }
}
