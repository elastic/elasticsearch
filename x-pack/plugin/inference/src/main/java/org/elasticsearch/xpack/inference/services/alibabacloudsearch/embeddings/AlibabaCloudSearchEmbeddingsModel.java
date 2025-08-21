/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.action.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class AlibabaCloudSearchEmbeddingsModel extends AlibabaCloudSearchModel {
    public static AlibabaCloudSearchEmbeddingsModel of(AlibabaCloudSearchEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(taskSettings);
        return new AlibabaCloudSearchEmbeddingsModel(
            model,
            AlibabaCloudSearchEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AlibabaCloudSearchEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            taskType,
            service,
            AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    AlibabaCloudSearchEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings,
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            serviceSettings.getCommonSettings()
        );
    }

    private AlibabaCloudSearchEmbeddingsModel(
        AlibabaCloudSearchEmbeddingsModel model,
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings
    ) {
        super(model, taskSettings);
    }

    public AlibabaCloudSearchEmbeddingsModel(
        AlibabaCloudSearchEmbeddingsModel model,
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    @Override
    public AlibabaCloudSearchEmbeddingsServiceSettings getServiceSettings() {
        return (AlibabaCloudSearchEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AlibabaCloudSearchEmbeddingsTaskSettings getTaskSettings() {
        return (AlibabaCloudSearchEmbeddingsTaskSettings) super.getTaskSettings();
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
