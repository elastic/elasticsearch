/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;

public class CustomElandModel extends ElasticsearchInternalModel {

    public CustomElandModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(inferenceEntityId, taskType, service, internalServiceSettings, chunkingSettings);
    }

    public CustomElandModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings,
        TaskSettings taskSettings
    ) {
        super(inferenceEntityId, taskType, service, internalServiceSettings, taskSettings);
    }

    @Override
    protected String modelNotFoundErrorMessage(String modelId) {
        return "Could not deploy model ["
            + modelId
            + "] as the model cannot be found."
            + " Custom models need to be loaded into the cluster with Eland before they can be started.";
    }
}
