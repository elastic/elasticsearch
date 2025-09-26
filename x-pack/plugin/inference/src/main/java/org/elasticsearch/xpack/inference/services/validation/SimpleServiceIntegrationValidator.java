
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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;

public class SimpleServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_TEXT_INPUT = List.of("how big");
    // The below data URL represents the base64 encoding of a single black pixel
    private static final List<String> TEST_URL_INPUT = List.of(
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVQImWNgYGAAAAAEAAGjChXjAAAAAElFTkSuQmCC"
    );
    private static final String QUERY = "test query";

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.infer(
            model,
            model.getTaskType().equals(TaskType.RERANK) ? QUERY : null,
            null,
            null,
            TEST_TEXT_INPUT,
            false,
            Map.of(),
            InputType.INTERNAL_INGEST,
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
            }, e -> {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Could not complete inference endpoint creation as validation call to service threw an exception.",
                        RestStatus.BAD_REQUEST,
                        e
                    )
                );
            }),
            TEST_URL_INPUT
        );
    }
}
