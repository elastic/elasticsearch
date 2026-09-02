/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;

public class CustomElandEmbeddingModel extends CustomElandModel {

    public CustomElandEmbeddingModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalTextEmbeddingServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, chunkingSettings));
    }

    public CustomElandEmbeddingModel(ModelConfigurations modelConfigurations) {
        super(modelConfigurations);
    }

    @Override
    public ElasticsearchInternalTextEmbeddingServiceSettings getServiceSettings() {
        return (ElasticsearchInternalTextEmbeddingServiceSettings) super.getServiceSettings();
    }
}
