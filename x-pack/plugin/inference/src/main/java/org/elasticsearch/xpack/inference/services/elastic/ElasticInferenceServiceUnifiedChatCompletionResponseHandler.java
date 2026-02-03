/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;

import java.util.Locale;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

public class ElasticInferenceServiceUnifiedChatCompletionResponseHandler extends ElasticInferenceServiceResponseHandler {
    public ElasticInferenceServiceUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        // EIS uses the unified API spec
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor((m, e) -> buildMidStreamError(request, m, e));

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingUnifiedChatCompletionResults(openAiProcessor);
    }

    @Override
    protected Exception buildError(String message, Request request, HttpResult result, ErrorResponse errorResponse) {
        assert request.isStreaming() : "Only streaming requests support this format";
        var responseStatusCode = result.response().getStatusLine().getStatusCode();
        if (request.isStreaming()) {
            var restStatus = toRestStatus(responseStatusCode);
            return new UnifiedChatCompletionException(
                restStatus,
                constructErrorMessage(message, request, errorResponse, responseStatusCode),
                "error",
                restStatus.name().toLowerCase(Locale.ROOT)
            );
        } else {
            return super.buildError(message, request, result, errorResponse);
        }
    }

    private static Exception buildMidStreamError(Request request, String message, Exception e) {
        var errorResponse = ElasticInferenceServiceErrorResponseEntity.fromString(message);
        if (errorResponse.errorStructureFound()) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    request.getInferenceEntityId(),
                    errorResponse.getErrorMessage()
                ),
                "error",
                "stream_error"
            );
        } else if (e != null) {
            return UnifiedChatCompletionException.fromThrowable(e);
        } else {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, request.getInferenceEntityId()),
                "error",
                "stream_error"
            );
        }
    }
}
