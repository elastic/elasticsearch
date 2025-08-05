/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

public class PredefinedCustomModel extends CustomModel {

    public PredefinedCustomModel(
        String inferenceId,
        TaskType taskType,
        String service,
        PredefinedCustomServiceSchema schema,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            service,
            PredefinedServiceSettings.fromMap(serviceSettings, context, taskType, schema),
            PredefinedTaskSettings.fromMap(schema, taskSettings),
            PredefinedSecretSettings.fromMap(schema, secrets)
        );
    }

    public PredefinedCustomModel(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> parsedServiceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {

        this(
            inferenceId,
            taskType,
            service,
            PredefinedServiceSettings.fromMap(parsedServiceSettings, context, taskType, inferenceId),
            PredefinedTaskSettings.fromMap(taskSettings),
            CustomSecretSettings.fromMap(secrets) // TODO: Switch this to PredefinedSecretSettings with proper serialization
        );
    }

    PredefinedCustomModel(
        String inferenceId,
        TaskType taskType,
        String service,
        PredefinedServiceSettings serviceSettings,
        CustomTaskSettings taskSettings,
        CustomSecretSettings secretSettings
    ) {
        super(inferenceId, taskType, service, serviceSettings, taskSettings, secretSettings);
    }

    protected PredefinedCustomModel(CustomModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public PredefinedServiceSettings getServiceSettings() {
        return (PredefinedServiceSettings) super.getServiceSettings();
    }
}
