/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
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
public class OpenAiStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, InferenceServiceResults.Result> {
    private static final Logger log = LogManager.getLogger(OpenAiStreamingProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in OpenAI chat completions response";

    private static final String CHOICES_FIELD = "choices";
    private static final String DELTA_FIELD = "delta";
    private static final String CONTENT_FIELD = "content";
    private static final String DONE_MESSAGE = "[done]";

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = parseEvent(item, OpenAiStreamingProcessor::parse, parserConfig, log);

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }

    private static Iterator<StreamingChatCompletionResults.Result> parse(XContentParserConfiguration parserConfig, ServerSentEvent event)
        throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.data())) {
            return Collections.emptyIterator();
        }

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.data())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, CHOICES_FIELD, FAILED_TO_FIND_FIELD_TEMPLATE);

            return parseList(jsonParser, parser -> {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

                positionParserAtTokenAfterField(parser, DELTA_FIELD, FAILED_TO_FIND_FIELD_TEMPLATE);

                var currentToken = parser.currentToken();

                ensureExpectedToken(XContentParser.Token.START_OBJECT, currentToken, parser);

                currentToken = parser.nextToken();

                // continue until the end of delta
                while (currentToken != null && currentToken != XContentParser.Token.END_OBJECT) {
                    if (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren();
                    }

                    if (currentToken == XContentParser.Token.FIELD_NAME && parser.currentName().equals(CONTENT_FIELD)) {
                        parser.nextToken();
                        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser);
                        var content = parser.text();
                        consumeUntilObjectEnd(parser); // end delta
                        consumeUntilObjectEnd(parser); // end choices
                        return content;
                    }

                    currentToken = parser.nextToken();
                }

                consumeUntilObjectEnd(parser); // end choices
                return ""; // stopped
            }).stream()
                .filter(Objects::nonNull)
                .filter(Predicate.not(String::isEmpty))
                .map(StreamingChatCompletionResults.Result::new)
                .iterator();
        }
    }
}
