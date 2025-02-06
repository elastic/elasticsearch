/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.elastic;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.openai.OpenAiUnifiedStreamingProcessor;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;

import java.util.Locale;
import java.util.concurrent.Flow;

public class ElasticInferenceServiceUnifiedChatCompletionResponseHandler extends ElasticInferenceServiceResponseHandler {
    public ElasticInferenceServiceUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor(); // EIS uses the unified API spec

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
                errorMessage(message, request, result, errorResponse, responseStatusCode),
                "error",
                restStatus.name().toLowerCase(Locale.ROOT)
            );
        } else {
            return super.buildError(message, request, result, errorResponse);
        }
    }
}
