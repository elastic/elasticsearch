/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ChatCompletionErrorResponseHandler;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.ocigenai.request.completion.OciGenAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiErrorResponseEntity;

import java.util.concurrent.Flow;

/**
 * Response handler of the {@code chat_completion} task: complete responses are parsed into a single unified chat completion chunk,
 * streaming responses (server-sent events) into a stream of unified chat completion chunks. Errors are reported in the OpenAI
 * compatible error format.
 */
public class OciGenAiUnifiedChatCompletionResponseHandler extends OciGenAiResponseHandler {

    private final ChatCompletionErrorResponseHandler chatCompletionErrorResponseHandler;

    public OciGenAiUnifiedChatCompletionResponseHandler(String requestType) {
        super(requestType, OciGenAiChatCompletionResponseEntity::fromResponse, OciGenAiErrorResponseEntity::fromResponse, true);
        this.chatCompletionErrorResponseHandler = new ChatCompletionErrorResponseHandler(OciGenAiErrorResponseEntity.ERROR_PARSER);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var ociGenAiProcessor = new OciGenAiUnifiedStreamingProcessor(
            modelIdOf(outboundRequest),
            (message, e) -> chatCompletionErrorResponseHandler.buildMidStreamChatCompletionError(
                outboundRequest.getInferenceEntityId(),
                message,
                e
            )
        );

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(ociGenAiProcessor);
        return new StreamingUnifiedChatCompletionResults(ociGenAiProcessor);
    }

    private static String modelIdOf(OutboundRequest outboundRequest) {
        return outboundRequest instanceof OciGenAiChatCompletionRequest chatCompletionRequest ? chatCompletionRequest.modelId() : "";
    }

    @Override
    protected UnifiedChatCompletionException buildError(String message, OutboundRequest outboundRequest, HttpResult result) {
        return chatCompletionErrorResponseHandler.buildChatCompletionError(message, outboundRequest, result);
    }
}
