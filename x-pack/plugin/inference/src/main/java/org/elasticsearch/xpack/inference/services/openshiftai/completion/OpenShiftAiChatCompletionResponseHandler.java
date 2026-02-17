/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

/**
 * Handles streaming chat completion responses and error parsing for OpenShift AI inference endpoints.
 * Adapts the OpenAI handler to support OpenShift AI's error schema.
 */
public class OpenShiftAiChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String OPENSHIFT_AI_ERROR = "openshift_ai_error";
    private static final UnifiedChatCompletionErrorParserContract OPENSHIFT_AI_ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(OPENSHIFT_AI_ERROR);

    public OpenShiftAiChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, OPENSHIFT_AI_ERROR_PARSER::parse, OPENSHIFT_AI_ERROR_PARSER);
    }
}
