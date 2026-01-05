/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskType;

public class MultilingualE5SmallModel extends ElasticsearchInternalModel {

    // Ensure that inference endpoints based on E5 small don't go past its window size
    public static final int E5_SMALL_MAX_WINDOW_SIZE = 300;

    public MultilingualE5SmallModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        MultilingualE5SmallInternalServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, chunkingSettings);
        if (chunkingSettings != null && chunkingSettings.maxChunkSize() != null) {
            if (chunkingSettings.maxChunkSize() > E5_SMALL_MAX_WINDOW_SIZE) throw new IllegalArgumentException(
                serviceSettings.modelId()
                    + " does not support chunk sizes larger than "
                    + E5_SMALL_MAX_WINDOW_SIZE
                    + ". Requested chunk size: "
                    + chunkingSettings.maxChunkSize()
            );
        }
    }

    @Override
    public MultilingualE5SmallInternalServiceSettings getServiceSettings() {
        return (MultilingualE5SmallInternalServiceSettings) super.getServiceSettings();
    }
}
