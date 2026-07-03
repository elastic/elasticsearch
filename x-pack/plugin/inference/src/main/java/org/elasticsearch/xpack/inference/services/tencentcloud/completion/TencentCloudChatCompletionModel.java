/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
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

public class TencentCloudChatCompletionModel extends TencentCloudModel {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme(TencentCloudUtils.SCHEME)
        .setHost(TencentCloudUtils.HOST)
        .setPathSegments(TencentCloudUtils.VERSION_1, TencentCloudUtils.CHAT_COMPLETIONS_PATH_1, TencentCloudUtils.CHAT_COMPLETIONS_PATH_2);

    public TencentCloudChatCompletionModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            TencentCloudChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    public TencentCloudChatCompletionModel(
        String inferenceId,
        TaskType taskType,
        TencentCloudChatCompletionServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceId, taskType, TencentCloudService.NAME, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            resolveUri(serviceSettings)
        );
    }

    public TencentCloudChatCompletionModel(ModelConfigurations config, ModelSecrets secrets) {
        super(
            config,
            secrets,
            (DefaultSecretSettings) secrets.getSecretSettings(),
            ((TencentCloudChatCompletionServiceSettings) config.getServiceSettings()).getCommonSettings(),
            resolveUri((TencentCloudChatCompletionServiceSettings) config.getServiceSettings())
        );
    }

    public String model() {
        return getServiceSettings().modelId();
    }

    @Override
    public TencentCloudChatCompletionServiceSettings getServiceSettings() {
        return (TencentCloudChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Chat completion is dispatched via the {@link TencentCloudService#doInfer} / {@link TencentCloudService#doUnifiedCompletionInfer}
     * paths using a dedicated {@code TencentCloudChatCompletionRequestManager}, so this visitor entry is not used.
     */
    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        throw new UnsupportedOperationException(
            "TencentCloud chat completion does not use the visitor-based action creation flow; "
                + "requests are dispatched directly through the request manager."
        );
    }

    private static URI resolveUri(TencentCloudChatCompletionServiceSettings serviceSettings) {
        var override = serviceSettings.getCommonSettings().uri();
        return Objects.requireNonNullElseGet(override, () -> buildUri("TencentCloud", DEFAULT_URI_BUILDER::build));
    }
}
