/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.Deque;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class GoogleVertexAiStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, InferenceServiceResults.Result> {

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = parseEvent(item, GoogleVertexAiStreamingProcessor::parse, parserConfig);

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }

    public static Stream<StreamingChatCompletionResults.Result> parse(XContentParserConfiguration parserConfig, ServerSentEvent event) {
        String data = event.data();
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            moveToFirstToken(jsonParser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

            var chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(jsonParser);

            return chunk.choices()
                .stream()
                .map(choice -> choice.delta())
                .filter(Objects::nonNull)
                .map(delta -> delta.content())
                .filter(content -> content != null && content.isEmpty() == false)
                .map(StreamingChatCompletionResults.Result::new);

        } catch (IOException e) {
            throw new ElasticsearchStatusException(
                "Failed to parse event from inference provider: {}",
                RestStatus.INTERNAL_SERVER_ERROR,
                e,
                event
            );
        }
    }
}
