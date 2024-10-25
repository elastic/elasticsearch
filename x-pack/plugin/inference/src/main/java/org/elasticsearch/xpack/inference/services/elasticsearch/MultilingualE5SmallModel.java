/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

public class MultilingualE5SmallModel extends ElasticsearchInternalModel {

    public MultilingualE5SmallModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        MultilingualE5SmallInternalServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, chunkingSettings);
    }

    @Override
    public MultilingualE5SmallInternalServiceSettings getServiceSettings() {
        return (MultilingualE5SmallInternalServiceSettings) super.getServiceSettings();
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
                                + "Multilingual-E5-Small model for this platform cannot be found."
                                + " Multilingual-E5-Small needs to be downloaded before it can be started"
                        )
                    );
                    return;
                }
                listener.onFailure(e);
            }
        };
    }

}
