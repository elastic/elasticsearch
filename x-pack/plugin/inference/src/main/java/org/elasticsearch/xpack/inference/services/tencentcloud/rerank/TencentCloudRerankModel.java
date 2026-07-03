/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
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

import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class TencentCloudRerankModel extends TencentCloudModel {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme(TencentCloudUtils.SCHEME)
        .setHost(TencentCloudUtils.HOST)
        .setPathSegments(TencentCloudUtils.VERSION_1, TencentCloudUtils.RERANK_PATH);

    public static TencentCloudRerankModel of(TencentCloudRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = TencentCloudRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new TencentCloudRerankModel(model, TencentCloudRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public TencentCloudRerankModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            TencentCloudRerankServiceSettings.fromMap(serviceSettings, context),
            TencentCloudRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    public TencentCloudRerankModel(
        String inferenceId,
        TencentCloudRerankServiceSettings serviceSettings,
        TencentCloudRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.RERANK, TencentCloudService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            resolveUri(serviceSettings)
        );
    }

    public TencentCloudRerankModel(ModelConfigurations config, ModelSecrets secrets) {
        super(
            config,
            secrets,
            (DefaultSecretSettings) secrets.getSecretSettings(),
            ((TencentCloudRerankServiceSettings) config.getServiceSettings()).getCommonSettings(),
            resolveUri((TencentCloudRerankServiceSettings) config.getServiceSettings())
        );
    }

    private TencentCloudRerankModel(TencentCloudRerankModel model, TencentCloudRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public TencentCloudRerankModel(TencentCloudRerankModel model, TencentCloudRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public TencentCloudRerankServiceSettings getServiceSettings() {
        return (TencentCloudRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public TencentCloudRerankTaskSettings getTaskSettings() {
        return (TencentCloudRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static URI resolveUri(TencentCloudRerankServiceSettings serviceSettings) {
        var override = serviceSettings.getCommonSettings().uri();
        return Objects.requireNonNullElseGet(override, () -> buildUri("TencentCloud", DEFAULT_URI_BUILDER::build));
    }
}
