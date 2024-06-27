/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;

import java.util.Map;

public class AmazonBedrockEmbeddingsModel extends AmazonBedrockModel {

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AmazonBedrockEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            new EmptyTaskSettings(),
            AmazonBedrockSecretSettings.fromMap(secretSettings)
        );
    }

    public static AmazonBedrockEmbeddingsModel of(AmazonBedrockEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        // no task settings to apply
        return embeddingsModel;
    }

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        AmazonBedrockSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings()),
            new ModelSecrets(secrets)
        );
    }

    public AmazonBedrockEmbeddingsModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    public AmazonBedrockEmbeddingsModel(Model model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public AmazonBedrockEmbeddingsModel(Model model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public AmazonBedrockEmbeddingsModel(ModelConfigurations configurations) {
        super(configurations);
    }

    @Override
    public ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
