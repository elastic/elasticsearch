/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;

/**
 * Handles non-streaming chat completion responses for Ibm foundation models, extending the OpenAI chat completion response handler.
 * This class is specifically designed to handle Ibm Watson error response format.
 */
public class IbmWatsonCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs an IbmWatsonCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "IBM WatsonX completions).
     * @param parseFunction The function to parse the response.
     */
    public IbmWatsonCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse);
    }
}
