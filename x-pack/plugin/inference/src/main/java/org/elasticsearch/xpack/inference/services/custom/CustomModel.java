/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;
import java.util.Objects;

public class CustomModel extends Model {
    private final CustomRateLimitServiceSettings rateLimitServiceSettings;

    public CustomModel(ModelConfigurations configurations, ModelSecrets secrets, CustomRateLimitServiceSettings rateLimitServiceSettings) {
        super(configurations, secrets);
        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    public static CustomModel of(CustomModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = CustomTaskSettings.fromMap(taskSettings);
        return new CustomModel(model, CustomTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public CustomModel(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            service,
            CustomServiceSettings.fromMap(serviceSettings, context, taskType, inferenceId),
            CustomTaskSettings.fromMap(taskSettings),
            CustomSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    CustomModel(
        String inferenceId,
        TaskType taskType,
        String service,
        CustomServiceSettings serviceSettings,
        CustomTaskSettings taskSettings,
        @Nullable CustomSecretSettings secretSettings
    ) {
        this(
            new ModelConfigurations(inferenceId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            serviceSettings
        );
    }

    protected CustomModel(CustomModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    protected CustomModel(CustomModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    @Override
    public CustomServiceSettings getServiceSettings() {
        return (CustomServiceSettings) super.getServiceSettings();
    }

    @Override
    public CustomTaskSettings getTaskSettings() {
        return (CustomTaskSettings) super.getTaskSettings();
    }

    @Override
    public CustomSecretSettings getSecretSettings() {
        return (CustomSecretSettings) super.getSecretSettings();
    }

    public CustomRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }
}
