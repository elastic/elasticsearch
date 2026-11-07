
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;

import java.util.List;

import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.USER_ROLE;

/**
 * This class uses the unified chat completion method to perform validation.
 */
public class SimpleChatCompletionServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final String TEST_INPUT = "how big";
    private static final UnifiedCompletionRequest TEST_REQUEST = UnifiedCompletionRequest.streaming(
        UnifiedCompletionRequestBody.of(List.of(new Message(new ContentString(TEST_INPUT), USER_ROLE, null, null, null, null)))
    );

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.unifiedCompletionInfer(model, TEST_REQUEST, timeout, ServiceIntegrationValidator.wrapListenerForValidation(listener));
    }
}
