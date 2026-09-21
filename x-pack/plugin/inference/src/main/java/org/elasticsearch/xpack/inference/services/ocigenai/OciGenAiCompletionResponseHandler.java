/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiErrorResponseEntity;

import java.util.concurrent.Flow;

/**
 * Response handler of the legacy {@code completion} task: complete responses are parsed into completion results and streaming
 * responses (server-sent events) into a stream of text deltas.
 */
public class OciGenAiCompletionResponseHandler extends OciGenAiResponseHandler {

    public OciGenAiCompletionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, OciGenAiErrorResponseEntity::fromResponse, true);
    }

    @Override
    public InferenceServiceResults parseResult(OutboundRequest outboundRequest, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var ociGenAiProcessor = new OciGenAiStreamingProcessor();

        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(ociGenAiProcessor);
        return new StreamingCompletionResults(ociGenAiProcessor);
    }
}
