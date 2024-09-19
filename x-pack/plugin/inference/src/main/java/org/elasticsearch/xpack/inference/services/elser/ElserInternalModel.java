/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalModel;

public class ElserInternalModel extends ElasticsearchInternalModel {

    public ElserInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElserInternalServiceSettings serviceSettings,
        ElserMlNodeTaskSettings taskSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
    }

    @Override
    public ElserInternalServiceSettings getServiceSettings() {
        return (ElserInternalServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElserMlNodeTaskSettings getTaskSettings() {
        return (ElserMlNodeTaskSettings) super.getTaskSettings();
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
                            "Could not start the ELSER service as the ELSER model for this platform cannot be found."
                                + " ELSER needs to be downloaded before it can be started."
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
    }
}
