/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;

public class OpenAiChatCompletionResponseHandler extends OpenAiResponseHandler {
    public OpenAiChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    @Override
    protected RetryException buildExceptionHandling429(Request request, HttpResult result) {
        // We don't retry, if the chat completion input is too large
        return new RetryException(false, buildError(RATE_LIMIT, request, result));
    }
}
