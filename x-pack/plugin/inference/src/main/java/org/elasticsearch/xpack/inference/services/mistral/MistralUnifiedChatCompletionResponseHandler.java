/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ChatCompletionErrorResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorParserContract;
import org.elasticsearch.xpack.inference.external.http.retry.UnifiedChatCompletionErrorResponseUtils;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;

import java.util.concurrent.Flow;

/**
 * Handles streaming chat completion responses and error parsing for Mistral inference endpoints.
 * Adapts the OpenAI handler to support Mistral's error schema and to Mistral reasoning models, which
 * return {@code content} as a list of typed chunks, see {@link MistralUnifiedStreamingProcessor}.
 */
public class MistralUnifiedChatCompletionResponseHandler extends OpenAiUnifiedChatCompletionResponseHandler {

    private static final String MISTRAL_ERROR = "mistral_error";

    private static final UnifiedChatCompletionErrorParserContract ERROR_PARSER = UnifiedChatCompletionErrorResponseUtils
        .createErrorParserWithStringify(MISTRAL_ERROR);

    private static final ChatCompletionErrorResponseHandler CHAT_COMPLETION_ERROR_RESPONSE_HANDLER = new ChatCompletionErrorResponseHandler(
        ERROR_PARSER
    );

    /**
     * Constructs a MistralUnifiedChatCompletionResponseHandler with the specified request type and response parser.
     *
     * @param requestType The type of request being handled (e.g., "mistral completions").
     * @param parseFunction The function to parse the response.
     */
    public MistralUnifiedChatCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, ERROR_PARSER::parse, ERROR_PARSER);
    }

    @Override
    public StreamingUnifiedChatCompletionResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var mistralProcessor = new MistralUnifiedStreamingProcessor(
            (m, e) -> CHAT_COMPLETION_ERROR_RESPONSE_HANDLER.buildMidStreamChatCompletionError(outboundRequest.getInferenceEntityId(), m, e)
        );
        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(mistralProcessor);
        return new StreamingUnifiedChatCompletionResults(mistralProcessor);
    }
}
