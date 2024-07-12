/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTED;

public class CustomElandModel extends Model implements ElasticsearchModel {
    private final InternalServiceSettings internalServiceSettings;

    public CustomElandModel(ModelConfigurations configurations, InternalServiceSettings internalServiceSettings) {
        super(configurations);
        this.internalServiceSettings = Objects.requireNonNull(internalServiceSettings);
    }

    public String getModelId() {
        return internalServiceSettings.getModelId();
    }

    @Override
    public StartTrainedModelDeploymentAction.Request getStartTrainedModelDeploymentActionRequest() {
        var startRequest = new StartTrainedModelDeploymentAction.Request(internalServiceSettings.getModelId(), this.getInferenceEntityId());
        startRequest.setNumberOfAllocations(internalServiceSettings.getNumAllocations());
        startRequest.setThreadsPerAllocation(internalServiceSettings.getNumThreads());
        startRequest.setAdaptiveAllocationsSettings(internalServiceSettings.getAdaptiveAllocationsSettings());
        startRequest.setWaitForState(STARTED);

        return startRequest;
    }

    @Override
    public ActionListener<CreateTrainedModelAssignmentAction.Response> getCreateTrainedModelAssignmentActionListener(
        Model model,
        ActionListener<Boolean> listener
    ) {

        return new ActionListener<>() {
            @Override
            public void onResponse(CreateTrainedModelAssignmentAction.Response response) {
                listener.onResponse(Boolean.TRUE);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            "Could not start the TextEmbeddingService service as the "
                                + "custom eland model [{0}] for this platform cannot be found."
                                + " Custom models need to be loaded into the cluster with eland before they can be started.",
                            getModelId()
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
    }
}
