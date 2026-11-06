/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiChatCompletionResponseEntity;

import java.io.IOException;
import java.util.Deque;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.parseObjects;

/**
 * Converts the server-sent events of a streaming OCI Generative AI {@code chat} response into text deltas for the legacy
 * {@code completion} task.
 */
public class OciGenAiStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, InferenceServiceResults.Result> {

    static final String DONE_MESSAGE = "[DONE]";

    private final String id = UUIDs.randomBase64UUID();

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = parseEvent(item, this::parse, parserConfig);

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingCompletionResults.Results(results));
        }
    }

    private Stream<StreamingCompletionResults.Result> parse(XContentParserConfiguration parserConfig, ServerSentEvent event) {
        if (DONE_MESSAGE.equals(event.data().strip())) {
            return Stream.empty();
        }

        try {
            return parseObjects(parserConfig, event.data(), parser -> {
                var chunk = OciGenAiChatCompletionResponseEntity.parseStreamingEvent(parser, id, "");
                if (chunk == null || chunk.choices() == null) {
                    return Stream.empty();
                }
                return chunk.choices()
                    .stream()
                    .map(choice -> choice.message())
                    .filter(Objects::nonNull)
                    .map(message -> message.content())
                    .filter(content -> Strings.isNullOrEmpty(content) == false)
                    .map(StreamingCompletionResults.Result::new);
            });
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
