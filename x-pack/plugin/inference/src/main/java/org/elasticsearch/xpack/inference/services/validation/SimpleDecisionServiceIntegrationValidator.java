/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.DecisionRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;

import java.util.HashMap;
import java.util.Map;

/**
 * Validates a {@code decision} endpoint with a single predicate question. The question is passed in the request task settings, replacing
 * any questions stored on the endpoint, so the validation call succeeds regardless of whether the endpoint has stored questions.
 */
public class SimpleDecisionServiceIntegrationValidator implements ServiceIntegrationValidator {

    static final InferenceString TEST_INPUT = InferenceString.ofText("Hello, how are you?");
    static final String TEST_QUESTION_NAME = "is_greeting";

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.decisionInfer(model, createRequest(), timeout, ServiceIntegrationValidator.wrapListenerForValidation(listener));
    }

    /**
     * Creates a new request per validation, since services may remove the task settings they recognize from the (mutable) map.
     */
    static DecisionRequest createRequest() {
        Map<String, Object> taskSettings = new HashMap<>();
        taskSettings.put(
            DecisionQuestions.QUESTIONS_FIELD,
            Map.of(
                TEST_QUESTION_NAME,
                Map.of(
                    DecisionQuestion.TYPE_FIELD,
                    DecisionQuestion.Type.PREDICATE.toString(),
                    DecisionQuestion.INSTRUCTIONS_FIELD,
                    "Is the text a greeting?"
                )
            )
        );
        return new DecisionRequest(TEST_INPUT, taskSettings);
    }
}
