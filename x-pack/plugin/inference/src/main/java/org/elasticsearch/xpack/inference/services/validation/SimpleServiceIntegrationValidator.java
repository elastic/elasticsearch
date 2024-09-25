/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.List;
import java.util.Map;

public class SimpleServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_INPUT = List.of("how big");
    private static final String QUERY = "test query";

    @Override
    public void validate(InferenceService service, Model model, ActionListener<InferenceServiceResults> listener) {
        service.infer(
            model,
            model.getTaskType().equals(TaskType.RERANK) ? QUERY : null,
            TEST_INPUT,
            Map.of(),
            InputType.INGEST,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener.delegateFailureAndWrap((delegate, r) -> {
                if (r != null) {
                    delegate.onResponse(r);
                } else {
                    delegate.onFailure(
                        new ElasticsearchStatusException("Could not make a validation call to the selected service", RestStatus.BAD_REQUEST)
                    );
                }
            })
        );
    }
}
