/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettings;

import java.util.List;
import java.util.Map;

/**
  * This class is a custom implementation of {@link SimpleServiceIntegrationValidator} specifically for Nvidia embeddings models.
  * Uses the input type defined in {@link NvidiaEmbeddingsTaskSettings}, rather than the default INTERNAL_INGEST.
  **/
public class NvidiaServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_INPUT = List.of("how big");

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (model.getTaskType() != TaskType.TEXT_EMBEDDING) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Task type [{}] to be validated does not match the NvidiaServiceIntegrationValidator [text_embedding] task type",
                    RestStatus.CONFLICT,
                    model.getTaskType()
                )
            );
            return;
        }
        service.infer(
            model,
            null,
            null,
            null,
            TEST_INPUT,
            false,
            Map.of(),
            // Use the input type defined in the model's task settings because it may differ from the default INTERNAL_INGEST
            ((NvidiaEmbeddingsTaskSettings) model.getTaskSettings()).getInputType(),
            timeout,
            ActionListener.wrap(r -> {
                if (r != null) {
                    listener.onResponse(r);
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Could not complete inference endpoint creation as validation call to service returned null response.",
                            RestStatus.BAD_REQUEST
                        )
                    );
                }
            },
                e -> listener.onFailure(
                    new ElasticsearchStatusException(
                        "Could not complete inference endpoint creation as validation call to service threw an exception.",
                        RestStatus.BAD_REQUEST,
                        e
                    )
                )
            )
        );
    }
}
