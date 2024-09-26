/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventField;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.COMPLETION;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

/**
 * Parses the OpenAI chat completion streaming responses.
 * For a request like:
 *
 * <pre>
 *     <code>
 *         {
 *             "inputs": ["Please summarize this text: some text", "Answer the following question: Question"]
 *         }
 *     </code>
 * </pre>
 *
 * The response would look like:
 *
 * <pre>
 *     <code>
 *         {
 *              "id": "chatcmpl-123",
 *              "object": "chat.completion",
 *              "created": 1677652288,
 *              "model": "gpt-3.5-turbo-0613",
 *              "system_fingerprint": "fp_44709d6fcb",
 *              "choices": [
 *                  {
 *                      "index": 0,
 *                      "delta": {
 *                          "content": "\n\nHello there, how ",
 *                      },
 *                      "finish_reason": ""
 *                  }
 *              ]
 *          }
 *
 *         {
 *              "id": "chatcmpl-123",
 *              "object": "chat.completion",
 *              "created": 1677652288,
 *              "model": "gpt-3.5-turbo-0613",
 *              "system_fingerprint": "fp_44709d6fcb",
 *              "choices": [
 *                  {
 *                      "index": 1,
 *                      "delta": {
 *                          "content": "may I assist you today?",
 *                      },
 *                      "finish_reason": ""
 *                  }
 *              ]
 *          }
 *
 *         {
 *              "id": "chatcmpl-123",
 *              "object": "chat.completion",
 *              "created": 1677652288,
 *              "model": "gpt-3.5-turbo-0613",
 *              "system_fingerprint": "fp_44709d6fcb",
 *              "choices": [
 *                  {
 *                      "index": 2,
 *                      "delta": {},
 *                      "finish_reason": "stop"
 *                  }
 *              ]
 *          }
 *
 *          [DONE]
 *     </code>
 * </pre>
 */
public class OpenAiStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, ChunkedToXContent> {
    private static final Logger log = LogManager.getLogger(OpenAiStreamingProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in OpenAI chat completions response";
    private static final String RESULT = "delta";

    private static final String CHOICES_FIELD = "choices";
    private static final String DELTA_FIELD = "delta";
    private static final String CONTENT_FIELD = "content";
    private static final String FINISH_REASON_FIELD = "finish_reason";
    private static final String STOP_MESSAGE = "stop";
    private static final String DONE_MESSAGE = "[done]";

    @Override
    protected void next(Deque<ServerSentEvent> item) {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var results = new ArrayDeque<ChunkedToXContent>(item.size());
        for (ServerSentEvent event : item) {
            if (ServerSentEventField.DATA == event.name() && event.hasValue()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.map(this::deltaChunk).ifPresent(results::offer);
                } catch (Exception e) {
                    log.warn("Failed to parse event from inference provider: {}", event);
                    onError(new IOException("Failed to parse event from inference provider.", e));
                    return;
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(completionChunk(results.iterator()));
        }
    }

    private Optional<String> parse(XContentParserConfiguration parserConfig, ServerSentEvent event) throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.value())) {
            return Optional.empty();
        }

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.value())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            // choices is an array, but since we don't send 'n' in the request then we only get one value in the result
            positionParserAtTokenAfterField(jsonParser, CHOICES_FIELD, FAILED_TO_FIND_FIELD_TEMPLATE);

            jsonParser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

            positionParserAtTokenAfterField(jsonParser, DELTA_FIELD, FAILED_TO_FIND_FIELD_TEMPLATE);

            token = jsonParser.currentToken();

            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            while (token != null) {
                if (token == XContentParser.Token.FIELD_NAME && jsonParser.currentName().equals(CONTENT_FIELD)) {
                    jsonParser.nextToken();
                    var contentToken = jsonParser.currentToken();
                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
                    return Optional.ofNullable(jsonParser.text());
                } else if (token == XContentParser.Token.FIELD_NAME && jsonParser.currentName().equals(FINISH_REASON_FIELD)) {
                    jsonParser.nextToken();
                    var contentToken = jsonParser.currentToken();
                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
                    if (STOP_MESSAGE.equalsIgnoreCase(jsonParser.text())) {
                        return Optional.empty();
                    }
                }
                token = jsonParser.nextToken();
            }

            throw new IllegalStateException(format(FAILED_TO_FIND_FIELD_TEMPLATE, CONTENT_FIELD));
        }
    }

    private ChunkedToXContent deltaChunk(String delta) {
        return params -> Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.field(RESULT, delta),
            ChunkedToXContentHelper.endObject()
        );
    }

    private ChunkedToXContent completionChunk(Iterator<? extends ChunkedToXContent> delta) {
        return params -> Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.startArray(COMPLETION),
            Iterators.flatMap(delta, d -> d.toXContentChunked(params)),
            ChunkedToXContentHelper.endArray(),
            ChunkedToXContentHelper.endObject()
        );
    }
}
