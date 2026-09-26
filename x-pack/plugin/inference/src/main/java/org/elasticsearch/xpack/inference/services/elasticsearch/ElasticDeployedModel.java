/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;

import java.util.Map;

public class ElasticDeployedModel extends ElasticsearchInternalModel {

    /**
     * Creates an {@link ElasticDeployedModel} with the appropriate {@link ElasticsearchInternalServiceSettings} based on the task type.
     */
    public static ElasticDeployedModel of(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings.Builder settingsBuilder,
        Map<String, Object> serviceSettingsMap,
        ChunkingSettings chunkingSettings
    ) {
        var deployedServiceSettings = taskType == TaskType.TEXT_EMBEDDING
            ? ElasticsearchInternalTextEmbeddingServiceSettings.fromMap(serviceSettingsMap, settingsBuilder)
            : settingsBuilder.build();

        return new ElasticDeployedModel(inferenceEntityId, taskType, service, deployedServiceSettings, chunkingSettings);
    }

    public ElasticDeployedModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, chunkingSettings);
    }

    @Override
    public boolean usesExistingDeployment() {
        return true;
    }

    @Override
    public StartTrainedModelDeploymentAction.Request getStartTrainedModelDeploymentActionRequest(TimeValue timeout) {
        throw new IllegalStateException("cannot start model that uses an existing deployment");
    }

    @Override
    protected String modelNotFoundErrorMessage(String modelId) {
        throw new IllegalStateException("cannot start model [" + modelId + "] that uses an existing deployment");
    }

    @Override
    public ActionListener<CreateTrainedModelAssignmentAction.Response> getCreateTrainedModelAssignmentActionListener(
        ElasticsearchInternalModel esModel,
        ActionListener<Boolean> listener
    ) {
        throw new IllegalStateException("cannot start model that uses an existing deployment");
    }
}
