/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.services.mistral.response.MistralChatCompletionResponseNormalizer;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedStreamingProcessor;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Streaming processor for the Mistral unified chat completion API.
 *
 * <p>Mistral payloads are normalized to the OpenAI shape before they reach the shared OpenAI parser,
 * see {@link MistralChatCompletionResponseNormalizer}.
 *
 * <p>Mistral can report an aborted generation in-band, as a chunk carrying {@code finish_reason: "error"},
 * rather than as an SSE error event. Such a chunk is turned into a typed error so that a truncated
 * answer is never presented to the caller as a successful completion. A stream that ends without any
 * {@code finish_reason} is reported as an error for the same reason.
 */
public class MistralUnifiedStreamingProcessor extends OpenAiUnifiedStreamingProcessor {

    static final String FINISH_REASON_ERROR = "error";
    static final String STREAM_ERROR_TYPE = "stream_error";

    private volatile boolean sawFinishReason = false;

    public MistralUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        super(errorParser);
    }

    @Override
    protected Stream<ChatCompletionChunkResponse> parseEvent(XContentParserConfiguration parserConfig, ServerSentEvent event)
        throws IOException {
        var data = event.data();
        if (DONE_MESSAGE.equalsIgnoreCase(data)) {
            return Stream.empty();
        }

        var chunks = parse(parserConfig, MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data)).toList();

        for (var chunk : chunks) {
            for (var choice : chunk.choices()) {
                if (FINISH_REASON_ERROR.equals(choice.finishReason())) {
                    throw new UnifiedChatCompletionException(
                        RestStatus.INTERNAL_SERVER_ERROR,
                        "Mistral ended the stream with finish reason [" + FINISH_REASON_ERROR + "], the response is incomplete",
                        STREAM_ERROR_TYPE,
                        STREAM_ERROR_TYPE
                    );
                }
                if (choice.finishReason() != null) {
                    sawFinishReason = true;
                }
            }
        }

        return chunks.stream();
    }

    /**
     * A stream that ends without any chunk carrying a {@code finish_reason} was cut short, so it is reported as an error
     * rather than as a successful completion.
     */
    @Override
    public void onComplete() {
        if (sawFinishReason) {
            super.onComplete();
        } else {
            onError(
                new UnifiedChatCompletionException(
                    RestStatus.INTERNAL_SERVER_ERROR,
                    "Mistral ended the stream without a finish reason, the response is incomplete",
                    STREAM_ERROR_TYPE,
                    STREAM_ERROR_TYPE
                )
            );
        }
    }
}
