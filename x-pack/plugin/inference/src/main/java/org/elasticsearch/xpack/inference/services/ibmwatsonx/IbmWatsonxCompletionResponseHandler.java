/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;

public class IbmWatsonxCompletionResponseHandler extends OpenAiChatCompletionResponseHandler {

    /**
     * Constructs a IbmWatsonxCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "IBM Watsonx completions").
     * @param parseFunction The function to parse the response.
     */
    public IbmWatsonxCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse);
    }
}
