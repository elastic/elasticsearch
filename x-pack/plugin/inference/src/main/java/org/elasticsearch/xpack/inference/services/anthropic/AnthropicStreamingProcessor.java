/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AnthropicStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, StreamingChatCompletionResults.Results> {
    private static final Logger log = LogManager.getLogger(AnthropicStreamingProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Anthropic chat completions response";

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        if (item.isEmpty()) {
            upstream().request(1);
            return;
        }

        var results = new ArrayDeque<StreamingChatCompletionResults.Result>(item.size());
        for (var event : item) {
            if (event.hasData()) {
                try (var parser = parser(event.data())) {
                    var eventType = eventType(parser);
                    switch (eventType) {
                        case "error" -> {
                            onError(parseError(parser));
                            return;
                        }
                        case "content_block_start" -> {
                            parseStartBlock(parser).ifPresent(results::offer);
                        }
                        case "content_block_delta" -> {
                            parseMessage(parser).ifPresent(results::offer);
                        }
                        case "message_start", "message_stop", "message_delta", "content_block_stop", "ping" -> {
                            log.debug("Skipping event type [{}] for line [{}].", eventType, item);
                        }
                        default -> {
                            // "handle unknown events gracefully" https://docs.anthropic.com/en/api/messages-streaming#other-events
                            // we'll ignore unknown events
                            log.debug("Unknown event type [{}] for line [{}].", eventType, item);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse line {}", event);
                    throw e;
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }

    private Throwable parseError(XContentParser parser) throws IOException {
        positionParserAtTokenAfterField(parser, "error", FAILED_TO_FIND_FIELD_TEMPLATE);
        var type = parseString(parser, "type");
        var message = parseString(parser, "message");
        var statusCode = switch (type) {
            case "invalid_request_error" -> RestStatus.BAD_REQUEST;
            case "authentication_error" -> RestStatus.UNAUTHORIZED;
            case "permission_error" -> RestStatus.FORBIDDEN;
            case "not_found_error" -> RestStatus.NOT_FOUND;
            case "request_too_large" -> RestStatus.REQUEST_ENTITY_TOO_LARGE;
            case "rate_limit_error" -> RestStatus.TOO_MANY_REQUESTS;
            default -> RestStatus.INTERNAL_SERVER_ERROR;
        };
        return new ElasticsearchStatusException(message, statusCode);
    }

    private Optional<StreamingChatCompletionResults.Result> parseStartBlock(XContentParser parser) throws IOException {
        positionParserAtTokenAfterField(parser, "content_block", FAILED_TO_FIND_FIELD_TEMPLATE);
        var text = parseString(parser, "text");
        return text.isBlank() ? Optional.empty() : Optional.of(new StreamingChatCompletionResults.Result(text));
    }

    private Optional<StreamingChatCompletionResults.Result> parseMessage(XContentParser parser) throws IOException {
        positionParserAtTokenAfterField(parser, "delta", FAILED_TO_FIND_FIELD_TEMPLATE);
        var text = parseString(parser, "text");
        return text.isBlank() ? Optional.empty() : Optional.of(new StreamingChatCompletionResults.Result(text));
    }

    private static XContentParser parser(String line) throws IOException {
        return XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, line);
    }

    private static String eventType(XContentParser parser) throws IOException {
        moveToFirstToken(parser);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        return parseString(parser, "type");
    }

    private static String parseString(XContentParser parser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(parser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser);
        return parser.text();
    }
}
