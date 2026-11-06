/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiChatCompletionResponseEntity;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.parseObjects;

/**
 * Converts the server-sent events of a streaming OCI Generative AI {@code chat} response into unified chat completion chunks.
 * All chunks of a stream share a generated id because OCI Generative AI does not provide one.
 */
public class OciGenAiUnifiedStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final Logger logger = LogManager.getLogger(OciGenAiUnifiedStreamingProcessor.class);
    static final String DONE_MESSAGE = "[DONE]";

    private final String id;
    private final String modelId;
    private final BiFunction<String, Exception, Exception> errorParser;

    public OciGenAiUnifiedStreamingProcessor(String modelId, BiFunction<String, Exception, Exception> errorParser) {
        this(UUIDs.randomBase64UUID(), modelId, errorParser);
    }

    // visible for testing so that the generated id can be controlled
    OciGenAiUnifiedStreamingProcessor(String id, String modelId, BiFunction<String, Exception, Exception> errorParser) {
        this.id = Objects.requireNonNull(id);
        this.modelId = Objects.requireNonNull(modelId);
        this.errorParser = Objects.requireNonNull(errorParser);
    }

    @Override
    protected void next(Deque<ServerSentEvent> events) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<ChatCompletionChunkResponse>(events.size());

        for (var event : events) {
            if (event.hasData() == false || DONE_MESSAGE.equals(event.data().strip())) {
                continue;
            }
            try {
                parseObjects(parserConfig, event.data(), parser -> {
                    var chunk = OciGenAiChatCompletionResponseEntity.parseStreamingEvent(parser, id, modelId);
                    return chunk == null ? Stream.<ChatCompletionChunkResponse>empty() : Stream.of(chunk);
                }).forEach(results::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse event from OCI Generative AI provider: {}", event.data());
                throw errorParser.apply(event.data(), e);
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingUnifiedChatCompletionResults.Results(results));
        }
    }
}
