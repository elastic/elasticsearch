/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiErrorResponseEntity;

import java.util.concurrent.Flow;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiResponseHandler extends BaseResponseHandler {

    static final String GOOGLE_VERTEX_AI_UNAVAILABLE = "The Google Vertex AI service may be temporarily overloaded or down";

    public GoogleVertexAiResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, GoogleVertexAiErrorResponseEntity::fromResponse);
    }

    public GoogleVertexAiResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        Function<HttpResult, ErrorResponse> errorParseFunction,
        boolean canHandleStreamingResponses
    ) {
        super(requestType, parseFunction, errorParseFunction, canHandleStreamingResponses);
    }

    @Override
    protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 503) {
            throw new RetryException(true, buildError(GOOGLE_VERTEX_AI_UNAVAILABLE, request, result));
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 404) {
            throw new RetryException(false, buildError(resourceNotFoundError(request), request, result));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(PERMISSION_DENIED, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    private static String resourceNotFoundError(Request request) {
        return format("Resource not found at [%s]", request.getURI());
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var googleVertexAiProcessor = new GoogleVertexAiStreamingProcessor();

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(googleVertexAiProcessor);
        return new StreamingChatCompletionResults(googleVertexAiProcessor);
    }
}
