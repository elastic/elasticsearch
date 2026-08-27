/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiUnifiedChatCompletionParser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.parseObjects;

public class OpenAiUnifiedStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final Logger logger = LogManager.getLogger(OpenAiUnifiedStreamingProcessor.class);
    private static final String DONE_MESSAGE = "[done]";

    private final BiFunction<String, Exception, Exception> errorParser;

    public OpenAiUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this.errorParser = errorParser;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var results = new ArrayDeque<ChatCompletionChunkResponse>(item.size());
        for (var event : item) {
            if ("error".equals(event.type()) && event.hasData()) {
                throw errorParser.apply(event.data(), null);
            } else if (event.hasData()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.forEach(results::offer);
                } catch (Exception e) {
                    logger.warn("Failed to parse event from inference provider: {}", event);
                    throw errorParser.apply(event.data(), e);
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingUnifiedChatCompletionResults.Results(results));
        }
    }

    public static Stream<ChatCompletionChunkResponse> parse(XContentParserConfiguration parserConfig, ServerSentEvent event) throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.data())) {
            return Stream.empty();
        }

        return parse(parserConfig, event.data());
    }

    public static Stream<ChatCompletionChunkResponse> parse(XContentParserConfiguration parserConfig, String data) throws IOException {
        return parseObjects(parserConfig, data, p -> Stream.of(OpenAiUnifiedChatCompletionParser.parseStreamingChunk(p)));
    }
}
