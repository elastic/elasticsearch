/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

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

public class AlibabaCloudSearchCompletionModel extends AlibabaCloudSearchModel {
    public static AlibabaCloudSearchCompletionModel of(AlibabaCloudSearchCompletionModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = AlibabaCloudSearchCompletionTaskSettings.fromMap(taskSettings);
        return new AlibabaCloudSearchCompletionModel(
            model,
            AlibabaCloudSearchCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AlibabaCloudSearchCompletionModel(
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
            AlibabaCloudSearchCompletionServiceSettings.fromMap(serviceSettings, context),
            AlibabaCloudSearchCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    AlibabaCloudSearchCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        AlibabaCloudSearchCompletionServiceSettings serviceSettings,
        AlibabaCloudSearchCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            serviceSettings.getCommonSettings()
        );
    }

    private AlibabaCloudSearchCompletionModel(
        AlibabaCloudSearchCompletionModel model,
        AlibabaCloudSearchCompletionTaskSettings taskSettings
    ) {
        super(model, taskSettings);
    }

    public AlibabaCloudSearchCompletionModel(
        AlibabaCloudSearchCompletionModel model,
        AlibabaCloudSearchCompletionServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    @Override
    public AlibabaCloudSearchCompletionServiceSettings getServiceSettings() {
        return (AlibabaCloudSearchCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AlibabaCloudSearchCompletionTaskSettings getTaskSettings() {
        return (AlibabaCloudSearchCompletionTaskSettings) super.getTaskSettings();
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
