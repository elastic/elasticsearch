/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;

public class ElserInternalModel extends ElasticsearchInternalModel {

    // Ensure that inference endpoints based on ELSER don't go past its truncation window of 512 tokens
    public static final int ELSER_MAX_WINDOW_SIZE = 300;

    public ElserInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElserInternalServiceSettings serviceSettings,
        ElserMlNodeTaskSettings taskSettings,
        ChunkingSettings chunkingSettings
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings));
    }

    public ElserInternalModel(ModelConfigurations modelConfigurations) {
        super(modelConfigurations);
        var chunkingSettings = modelConfigurations.getChunkingSettings();

        if (chunkingSettings != null && chunkingSettings.maxChunkSize() != null && chunkingSettings.maxChunkSize() > ELSER_MAX_WINDOW_SIZE)
            throw new IllegalArgumentException(
                Strings.format(
                    "ELSER based models do not support chunk sizes larger than %d. Requested chunk size: %d",
                    ELSER_MAX_WINDOW_SIZE,
                    chunkingSettings.maxChunkSize()
                )
            );
    }

    @Override
    public ElserInternalServiceSettings getServiceSettings() {
        return (ElserInternalServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElserMlNodeTaskSettings getTaskSettings() {
        return (ElserMlNodeTaskSettings) super.getTaskSettings();
    }
}
