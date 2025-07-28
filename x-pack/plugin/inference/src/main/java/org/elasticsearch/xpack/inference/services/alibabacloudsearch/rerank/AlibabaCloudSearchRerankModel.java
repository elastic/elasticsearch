/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.action.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class AlibabaCloudSearchRerankModel extends AlibabaCloudSearchModel {
    public static AlibabaCloudSearchRerankModel of(AlibabaCloudSearchRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = AlibabaCloudSearchRerankTaskSettings.fromMap(taskSettings);
        return new AlibabaCloudSearchRerankModel(
            model,
            AlibabaCloudSearchRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AlibabaCloudSearchRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            taskType,
            service,
            AlibabaCloudSearchRerankServiceSettings.fromMap(serviceSettings, context),
            AlibabaCloudSearchRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    AlibabaCloudSearchRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        AlibabaCloudSearchRerankServiceSettings serviceSettings,
        AlibabaCloudSearchRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            serviceSettings.getCommonSettings()
        );
    }

    private AlibabaCloudSearchRerankModel(AlibabaCloudSearchRerankModel model, AlibabaCloudSearchRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public AlibabaCloudSearchRerankModel(AlibabaCloudSearchRerankModel model, AlibabaCloudSearchRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public AlibabaCloudSearchRerankServiceSettings getServiceSettings() {
        return (AlibabaCloudSearchRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public AlibabaCloudSearchRerankTaskSettings getTaskSettings() {
        return (AlibabaCloudSearchRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
