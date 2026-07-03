/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudService;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class TencentCloudEmbeddingsModel extends TencentCloudModel {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme(TencentCloudUtils.SCHEME)
        .setHost(TencentCloudUtils.HOST)
        .setPathSegments(TencentCloudUtils.VERSION_1, TencentCloudUtils.EMBEDDINGS_PATH);

    public TencentCloudEmbeddingsModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            TencentCloudEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            TencentCloudEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TencentCloudEmbeddingsServiceSettings serviceSettings,
        TencentCloudEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.TEXT_EMBEDDING, TencentCloudService.NAME, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            resolveUri(serviceSettings)
        );
    }

    public TencentCloudEmbeddingsModel(ModelConfigurations config, ModelSecrets secrets) {
        super(
            config,
            secrets,
            (DefaultSecretSettings) secrets.getSecretSettings(),
            ((TencentCloudEmbeddingsServiceSettings) config.getServiceSettings()).getCommonSettings(),
            resolveUri((TencentCloudEmbeddingsServiceSettings) config.getServiceSettings())
        );
    }

    public TencentCloudEmbeddingsModel(TencentCloudEmbeddingsModel model, TencentCloudEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public TencentCloudEmbeddingsServiceSettings getServiceSettings() {
        return (TencentCloudEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public TencentCloudEmbeddingsTaskSettings getTaskSettings() {
        return (TencentCloudEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static java.net.URI resolveUri(TencentCloudEmbeddingsServiceSettings serviceSettings) {
        var override = serviceSettings.getCommonSettings().uri();
        return Objects.requireNonNullElseGet(override, () -> buildUri("TencentCloud", DEFAULT_URI_BUILDER::build));
    }
}
