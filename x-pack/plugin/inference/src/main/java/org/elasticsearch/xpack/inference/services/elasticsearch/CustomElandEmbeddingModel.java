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

public class CustomElandEmbeddingModel extends CustomElandModel {

    public CustomElandEmbeddingModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        ConfigurationParseContext context
    ) {
        this(inferenceEntityId, taskType, service, CustomElandInternalTextEmbeddingServiceSettings.fromMap(serviceSettings, context));
    }

    public CustomElandEmbeddingModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        CustomElandInternalTextEmbeddingServiceSettings serviceSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings),
            serviceSettings.getElasticsearchInternalServiceSettings()
        );
    }

    @Override
    public CustomElandInternalTextEmbeddingServiceSettings getServiceSettings() {
        return (CustomElandInternalTextEmbeddingServiceSettings) super.getServiceSettings();
    }
}
