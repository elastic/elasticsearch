/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;

import java.util.Map;

public class AmazonBedrockChatCompletionModel extends AmazonBedrockModel {

    public static AmazonBedrockChatCompletionModel of(AmazonBedrockChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return completionModel;
        }

        var requestTaskSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(taskSettings);
        var taskSettingsToUse = AmazonBedrockChatCompletionTaskSettings.of(completionModel.getTaskSettings(), requestTaskSettings);
        return new AmazonBedrockChatCompletionModel(completionModel, taskSettingsToUse);
    }

    public AmazonBedrockChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String name,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            name,
            AmazonBedrockChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AmazonBedrockChatCompletionTaskSettings.fromMap(taskSettings),
            AmazonBedrockSecretSettings.fromMap(secretSettings)
        );
    }

    public AmazonBedrockChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockChatCompletionServiceSettings serviceSettings,
        AmazonBedrockChatCompletionTaskSettings taskSettings,
        AmazonBedrockSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AmazonBedrockChatCompletionModel(Model model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    @Override
    public AmazonBedrockChatCompletionServiceSettings getServiceSettings() {
        return (AmazonBedrockChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AmazonBedrockChatCompletionTaskSettings getTaskSettings() {
        return (AmazonBedrockChatCompletionTaskSettings) super.getTaskSettings();
    }
}
