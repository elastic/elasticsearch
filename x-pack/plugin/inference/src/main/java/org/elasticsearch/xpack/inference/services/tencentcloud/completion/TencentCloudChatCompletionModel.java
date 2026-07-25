/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

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

public class TencentCloudChatCompletionModel extends TencentCloudModel {

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

    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static URI resolveUri(TencentCloudChatCompletionServiceSettings serviceSettings) {
        return TencentCloudUtils.buildUri(
            serviceSettings.getCommonSettings().region(),
            TencentCloudUtils.VERSION_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_2
        );
    }
}
