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

/**
 * Represents an OpenShift AI rerank model.
 * This class extends the {@link OpenShiftAiModel} and provides specific configurations for rerank tasks.
 */
public class OpenShiftAiRerankModel extends OpenShiftAiModel {

    /**
     * Creates a new {@link OpenShiftAiRerankModel} with updated task settings if they differ from the existing ones.
     * @param model the existing OpenShift AI rerank model
     * @param taskSettings the new task settings to apply
     * @return a new {@link OpenShiftAiRerankModel} with updated task settings, or the original model if settings are unchanged
     */
    public static OpenShiftAiRerankModel of(OpenShiftAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = OpenShiftAiRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new OpenShiftAiRerankModel(model, OpenShiftAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
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
            OpenShiftAiRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    OpenShiftAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OpenShiftAiRerankServiceSettings serviceSettings,
        OpenShiftAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
    }

    private OpenShiftAiRerankModel(OpenShiftAiRerankModel model, OpenShiftAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public OpenShiftAiRerankServiceSettings getServiceSettings() {
        return (OpenShiftAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenShiftAiRerankTaskSettings getTaskSettings() {
        return (OpenShiftAiRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public ExecutableAction accept(OpenShiftAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
