/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiModel;
import org.elasticsearch.xpack.inference.services.openshiftai.action.OpenShiftAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class OpenShiftAiRerankModel extends OpenShiftAiModel {
    public static OpenShiftAiRerankModel of(OpenShiftAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = OpenShiftAIRerankTaskSettings.fromMap(taskSettings);
        return new OpenShiftAiRerankModel(model, OpenShiftAIRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public OpenShiftAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OpenShiftAiRerankServiceSettings.fromMap(serviceSettings, context),
            OpenShiftAIRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    OpenShiftAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OpenShiftAiRerankServiceSettings serviceSettings,
        OpenShiftAIRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
    }

    private OpenShiftAiRerankModel(OpenShiftAiRerankModel model, OpenShiftAIRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public OpenShiftAiRerankServiceSettings getServiceSettings() {
        return (OpenShiftAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenShiftAIRerankTaskSettings getTaskSettings() {
        return (OpenShiftAIRerankTaskSettings) super.getTaskSettings();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for IBM watsonx models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    public ExecutableAction accept(OpenShiftAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
