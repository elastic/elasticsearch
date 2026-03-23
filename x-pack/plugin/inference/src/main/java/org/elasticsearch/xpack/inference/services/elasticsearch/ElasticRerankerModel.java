/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ModelConfigurations;
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
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings));
    }

    public ElasticRerankerModel(ModelConfigurations modelConfigurations) {
        super(modelConfigurations);
    }

    @Override
    public ElasticRerankerServiceSettings getServiceSettings() {
        return (ElasticRerankerServiceSettings) super.getServiceSettings();
    }

    @Override
    public ActionListener<CreateTrainedModelAssignmentAction.Response> getCreateTrainedModelAssignmentActionListener(
        ElasticsearchInternalModel esModel,
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
                    var exception = new ResourceNotFoundException(
                        "Could not start the Elastic Reranker Endpoint due to [{}]",
                        e,
                        e.getMessage()
                    );
                    exception.setResources("inference_endpoint", esModel.internalServiceSettings.modelId());
                    listener.onFailure(exception);
                    return;
                }
                listener.onFailure(e);
            }
        };
    }

}
