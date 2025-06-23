/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.action.AmazonBedrockActionVisitor;

import java.util.Map;

public class AmazonBedrockEmbeddingsModel extends AmazonBedrockModel {

    public static AmazonBedrockEmbeddingsModel of(AmazonBedrockEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        if (taskSettings != null && taskSettings.isEmpty() == false) {
            var updatedTaskSettings = embeddingsModel.getTaskSettings().updatedTaskSettings(taskSettings);
            return new AmazonBedrockEmbeddingsModel(embeddingsModel, updatedTaskSettings);
        }

        return embeddingsModel;
    }

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AmazonBedrockEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AmazonBedrockEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            AwsSecretSettings.fromMap(secretSettings)
        );
    }

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockEmbeddingsServiceSettings serviceSettings,
        AmazonBedrockEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        AwsSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    public AmazonBedrockEmbeddingsModel(Model model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public AmazonBedrockEmbeddingsModel(Model model, AmazonBedrockEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    @Override
    public AmazonBedrockEmbeddingsServiceSettings getServiceSettings() {
        return (AmazonBedrockEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AmazonBedrockEmbeddingsTaskSettings getTaskSettings() {
        return (AmazonBedrockEmbeddingsTaskSettings) super.getTaskSettings();
    }
}
