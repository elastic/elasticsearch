/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceErrorResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;

import java.util.Locale;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;

/**
 * Handles responses for the {@link TaskType#CHAT_COMPLETION} task type, which speaks the unified
 * (OpenAI-compatible) chat completion API. Both the streaming and non-streaming variants of that API report failures as a
 * {@link UnifiedChatCompletionException} so that the error shape a caller sees does not depend on whether they asked for a stream.
 * <p>
 * Contrast with {@link ElasticInferenceServiceCompletionResponseHandler}, which serves the older
 * {@link TaskType#COMPLETION} task type and reports failures as a plain
 * {@link ElasticsearchStatusException}.
 */
public class ElasticInferenceServiceUnifiedChatCompletionResponseHandler extends ElasticInferenceServiceResponseHandler {

    public static final String CHAT_COMPLETIONS_REQUEST_DESCRIPTION = Strings.format(
        "%s chat completion",
        ELASTIC_INFERENCE_SERVICE_IDENTIFIER
    );

    public ElasticInferenceServiceUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, true);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        // EIS uses the unified API spec
        var openAiProcessor = new OpenAiUnifiedStreamingProcessor((m, e) -> buildMidStreamError(outboundRequest, m, e));

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingUnifiedChatCompletionResults(openAiProcessor);
    }

    @Override
    protected ElasticsearchException buildError(String message, OutboundRequest outboundRequest, HttpResult result) {
        var statusCode = result.response().getStatusLine().getStatusCode();
        var restStatus = toRestStatus(statusCode);
        var errorResponse = ElasticInferenceServiceErrorResponseEntity.fromResponse(result);

        var error = new UnifiedChatCompletionException(
            restStatus,
            constructErrorMessage(message, outboundRequest, errorResponse, statusCode),
            "error",
            restStatus.name().toLowerCase(Locale.ROOT)
        );
        addRetryAfterHeaderIfPresent(result, error);
        return error;
    }

    private static Exception buildMidStreamError(OutboundRequest outboundRequest, String message, Exception e) {
        var errorResponse = ElasticInferenceServiceErrorResponseEntity.fromString(message);
        if (errorResponse.errorStructureFound()) {
            return new UnifiedChatCompletionException(
                RestStatus.INTERNAL_SERVER_ERROR,
                format(
                    "%s for request from inference entity id [%s]. Error message: [%s]",
                    SERVER_ERROR_OBJECT,
                    outboundRequest.getInferenceEntityId(),
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
                format("%s for request from inference entity id [%s]", SERVER_ERROR_OBJECT, outboundRequest.getInferenceEntityId()),
                "error",
                "stream_error"
            );
        }
    }
}
