
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.util.List;

import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.USER_ROLE;

/**
 * This class uses the unified chat completion method to perform validation.
 */
public class SimpleChatCompletionServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final List<String> TEST_INPUT = List.of("how big");

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        var chatCompletionInput = new UnifiedChatInput(TEST_INPUT, USER_ROLE, false);
        service.unifiedCompletionInfer(model, chatCompletionInput.getRequest(), timeout, ActionListener.wrap(r -> {
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
        }));
    }
}
