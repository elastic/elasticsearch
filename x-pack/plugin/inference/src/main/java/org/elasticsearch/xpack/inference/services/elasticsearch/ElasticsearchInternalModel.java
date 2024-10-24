/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;

public abstract class ElasticsearchInternalModel extends Model {

    protected ElasticsearchInternalServiceSettings internalServiceSettings;

    public ElasticsearchInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, internalServiceSettings, chunkingSettings));
        this.internalServiceSettings = internalServiceSettings;
    }

    public ElasticsearchInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, internalServiceSettings));
        this.internalServiceSettings = internalServiceSettings;
    }

    public ElasticsearchInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings,
        TaskSettings taskSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, internalServiceSettings, taskSettings));
        this.internalServiceSettings = internalServiceSettings;
    }

    public ElasticsearchInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticsearchInternalServiceSettings internalServiceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, internalServiceSettings, taskSettings, chunkingSettings));
        this.internalServiceSettings = internalServiceSettings;
    }

    public StartTrainedModelDeploymentAction.Request getStartTrainedModelDeploymentActionRequest() {
        var startRequest = new StartTrainedModelDeploymentAction.Request(internalServiceSettings.modelId(), this.getInferenceEntityId());
        startRequest.setNumberOfAllocations(internalServiceSettings.getNumAllocations());
        startRequest.setThreadsPerAllocation(internalServiceSettings.getNumThreads());
        startRequest.setAdaptiveAllocationsSettings(internalServiceSettings.getAdaptiveAllocationsSettings());
        startRequest.setWaitForState(STARTED);

        return startRequest;
    }

    public abstract ActionListener<CreateTrainedModelAssignmentAction.Response> getCreateTrainedModelAssignmentActionListener(
        Model model,
        ActionListener<Boolean> listener
    );

    public boolean usesExistingDeployment() {
        return internalServiceSettings.getDeploymentId() != null;
    }

    @Override
    public ElasticsearchInternalServiceSettings getServiceSettings() {
        return (ElasticsearchInternalServiceSettings) super.getServiceSettings();
    }

    public void updateNumAllocations(Integer numAllocations) {
        this.internalServiceSettings.setNumAllocations(numAllocations);
    }

    @Override
    public String toString() {
        return Strings.toString(this.getConfigurations());
    }

    public String mlNodeDeploymentId() {
        return internalServiceSettings.getDeploymentId() == null ? getInferenceEntityId() : internalServiceSettings.getDeploymentId();
    }
}
