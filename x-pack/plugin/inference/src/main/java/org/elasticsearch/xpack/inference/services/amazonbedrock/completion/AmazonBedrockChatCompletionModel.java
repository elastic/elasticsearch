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
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.action.AmazonBedrockActionVisitor;

import java.util.Map;

public class AmazonBedrockChatCompletionModel extends AmazonBedrockModel {

    public static AmazonBedrockChatCompletionModel of(AmazonBedrockChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return completionModel;
        }

        var requestTaskSettings = AmazonBedrockCompletionTaskSettings.fromMap(taskSettings);
        var taskSettingsToUse = AmazonBedrockCompletionTaskSettings.of(completionModel.getTaskSettings(), requestTaskSettings);

        // If the task settings didn't change, then return the same model
        if (taskSettingsToUse.equals(completionModel.getTaskSettings())) {
            return completionModel;
        }

        return new AmazonBedrockChatCompletionModel(completionModel, taskSettingsToUse);
    }

    /**
     * Creates a new AmazonBedrockChatCompletionModel with overridden service settings.
     *
     * @param model The original AmazonBedrockChatCompletionModel.
     * @param request The UnifiedCompletionRequest containing the model override.
     * @return A new AmazonBedrockChatCompletionModel with the overridden model ID.
     */
    public static AmazonBedrockChatCompletionModel of(AmazonBedrockChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null || request.model().equals(model.getServiceSettings().modelId())) {
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new AmazonBedrockChatCompletionServiceSettings(
            originalModelServiceSettings.region(),
            request.model(),
            originalModelServiceSettings.provider(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new AmazonBedrockChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getTaskSettings(),
            model.getSecretSettings()
        );
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
            AmazonBedrockCompletionTaskSettings.fromMap(taskSettings),
            AwsSecretSettings.fromMap(secretSettings)
        );
    }

    public AmazonBedrockChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockChatCompletionServiceSettings serviceSettings,
        AmazonBedrockCompletionTaskSettings taskSettings,
        AwsSecretSettings secrets
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AmazonBedrockChatCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
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
    public AmazonBedrockCompletionTaskSettings getTaskSettings() {
        return (AmazonBedrockCompletionTaskSettings) super.getTaskSettings();
    }
}
