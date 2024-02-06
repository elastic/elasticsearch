/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.textembedding;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;

public abstract class TextEmbeddingModel extends Model {

    public TextEmbeddingModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        TextEmbeddingInternalServiceSettings serviceSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings));
    }

    @Override
    public TextEmbeddingInternalServiceSettings getServiceSettings() {
        return (TextEmbeddingInternalServiceSettings) super.getServiceSettings();
    }

    abstract StartTrainedModelDeploymentAction.Request getStartTrainedModelDeploymentActionRequest();

    abstract ActionListener<CreateTrainedModelAssignmentAction.Response> getCreateTrainedModelAssignmentActionListener(
        Model model,
        ActionListener<Boolean> listener
    );
}
