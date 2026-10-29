/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiStreamingProcessor;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiCompletionResponseEntity;

import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;

/**
 * Handles responses for the {@link org.elasticsearch.inference.TaskType#COMPLETION} task type. EIS speaks the OpenAI wire format, so
 * the OpenAI response entity and streaming processor can be reused verbatim; what differs from
 * {@link ElasticInferenceServiceUnifiedChatCompletionResponseHandler} is the result and error shape the caller sees. This handler
 * produces {@link org.elasticsearch.xpack.core.inference.results.CompletionResults} (or {@link StreamingCompletionResults}) and reports
 * failures as a plain {@link org.elasticsearch.ElasticsearchStatusException}, which is what the {@code _inference} API contract requires
 * for {@code completion}. The {@code chat_completion} handler instead reports
 * {@link org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException}.
 */
public class ElasticInferenceServiceCompletionResponseHandler extends ElasticInferenceServiceResponseHandler {
    public static final String COMPLETIONS_REQUEST_DESCRIPTION = Strings.format("%s completion", ELASTIC_INFERENCE_SERVICE_IDENTIFIER);

    public ElasticInferenceServiceCompletionResponseHandler() {
        super(COMPLETIONS_REQUEST_DESCRIPTION, OpenAiCompletionResponseEntity::fromResponse, true);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var openAiProcessor = new OpenAiStreamingProcessor();

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(openAiProcessor);
        return new StreamingCompletionResults(openAiProcessor);
    }
}
