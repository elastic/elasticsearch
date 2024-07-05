/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

public class CustomElandRerankModel extends CustomElandModel {

    public CustomElandRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            CustomElandInternalServiceSettings.fromMap(serviceSettings),
            CustomElandRerankTaskSettings.defaultsFromMap(taskSettings)
        );
    }

    // default for testing
    CustomElandRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        CustomElandInternalServiceSettings serviceSettings,
        CustomElandRerankTaskSettings taskSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), serviceSettings);
    }

    @Override
    public CustomElandInternalServiceSettings getServiceSettings() {
        return (CustomElandInternalServiceSettings) super.getServiceSettings();
    }
}
