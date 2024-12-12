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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

public class ElasticRerankerModel extends ElasticsearchInternalModel {

    public ElasticRerankerModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticRerankerServiceSettings serviceSettings,
        RerankTaskSettings taskSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
    }

    @Override
    public ElasticRerankerServiceSettings getServiceSettings() {
        return (ElasticRerankerServiceSettings) super.getServiceSettings();
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
                        new ResourceNotFoundException("Could not start the Elastic Reranker Endpoint due to [{}]", e, e.getMessage())
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
    }

}
