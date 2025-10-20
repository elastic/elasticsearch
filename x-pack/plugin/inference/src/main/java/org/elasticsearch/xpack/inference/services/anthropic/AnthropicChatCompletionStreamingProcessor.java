/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

/**
 * Chat Completions Streaming Processor for Anthropic provider
 */
public class AnthropicChatCompletionStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Anthropic chat completions response";
    private static final Logger logger = LogManager.getLogger(AnthropicChatCompletionStreamingProcessor.class);

    // Field names
    public static final String ROLE_FIELD = "role";
    public static final String INDEX_FIELD = "index";
    public static final String TYPE_FIELD = "type";
    public static final String MODEL_FIELD = "model";
    public static final String ID_FIELD = "id";
    public static final String NAME_FIELD = "name";
    public static final String INPUT_TOKENS_FIELD = "input_tokens";
    public static final String OUTPUT_TOKENS_FIELD = "output_tokens";
    public static final String STOP_REASON_FIELD = "stop_reason";
    public static final String TEXT_FIELD = "text";
    public static final String INPUT_FIELD = "input";
    public static final String PARTIAL_JSON_FIELD = "partial_json";

    // Event types
    public static final String MESSAGE_DELTA_EVENT_TYPE = "message_delta";
    public static final String CONTENT_BLOCK_START_EVENT_TYPE = "content_block_start";
    public static final String MESSAGE_START_EVENT_TYPE = "message_start";
    public static final String VERTEX_EVENT_EVENT_TYPE = "vertex_event";
    public static final String PING_EVENT_TYPE = "ping";
    public static final String CONTENT_BLOCK_STOP_EVENT_TYPE = "content_block_stop";
    public static final String CONTENT_BLOCK_DELTA_EVENT_TYPE = "content_block_delta";
    public static final String MESSAGE_STOP_EVENT_TYPE = "message_stop";
    public static final String ERROR_TYPE = "error";

    // Content block types
    public static final String TEXT_DELTA_TYPE = "text_delta";
    public static final String INPUT_JSON_DELTA_TYPE = "input_json_delta";
    public static final String TOOL_USE_TYPE = "tool_use";
    public static final String TEXT_TYPE = "text";

    private final BiFunction<String, Exception, Exception> errorParser;

    public AnthropicChatCompletionStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this.errorParser = errorParser;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(item.size());

        for (var event : item) {
            if (ERROR_TYPE.equals(event.type()) && event.hasData()) {
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

    /**
     * Parse a single ServerSentEvent into zero or more ChatCompletionChunk
     * @param parserConfig the parser configuration
     * @param event the server sent event
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parse(
        XContentParserConfiguration parserConfig,
        ServerSentEvent event
    ) throws IOException {
        // Handle known event types
        switch (event.type()) {
            case VERTEX_EVENT_EVENT_TYPE, PING_EVENT_TYPE, CONTENT_BLOCK_STOP_EVENT_TYPE:
                // No content to parse, just skip
                logger.debug("Skipping event type [{}] for line [{}].", event.type(), event.data());
                return Stream.empty();
            case MESSAGE_START_EVENT_TYPE:
                return parseMessageStart(parserConfig, event.data());
            case CONTENT_BLOCK_START_EVENT_TYPE:
                return parseContentBlockStart(parserConfig, event.data());
            case CONTENT_BLOCK_DELTA_EVENT_TYPE:
                return parseContentBlockDelta(parserConfig, event.data());
            case MESSAGE_DELTA_EVENT_TYPE:
                return parseMessageDelta(parserConfig, event.data());
            case MESSAGE_STOP_EVENT_TYPE:
                return Stream.empty();
            case null, default:
                logger.debug("Unknown event type [{}] for line [{}].", event.type(), event.data());
                return Stream.empty();
        }
    }

    /**
     * Parse a message start event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageStart(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var id = parseStringField(jsonParser, ID_FIELD);
            var role = parseStringField(jsonParser, ROLE_FIELD);
            var model = parseStringField(jsonParser, MODEL_FIELD);
            var finishReason = parseStringOrNullField(jsonParser, STOP_REASON_FIELD);
            var promptTokens = parseNumberField(jsonParser, INPUT_TOKENS_FIELD);
            var completionTokens = parseNumberField(jsonParser, OUTPUT_TOKENS_FIELD);
            var totalTokens = completionTokens + promptTokens;

            var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(completionTokens, promptTokens, totalTokens);
            var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, role, null);
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, finishReason, 0);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(id, List.of(choice), model, null, usage);

            return Stream.of(chunk);
        }
    }

    /**
     * Parse a content block start event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockStart(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var index = parseNumberField(jsonParser, INDEX_FIELD);
            var type = parseStringField(jsonParser, TYPE_FIELD);
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
            if (type.equals(TEXT_TYPE)) {
                var text = parseStringField(jsonParser, TEXT_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null);
            } else if (type.equals(TOOL_USE_TYPE)) {
                var id = parseStringField(jsonParser, ID_FIELD);
                var name = parseStringField(jsonParser, NAME_FIELD);
                var input = parseFieldValue(jsonParser, INPUT_FIELD);
                var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                    input != null ? input.toString() : null,
                    name
                );
                var toolCall = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(0, id, function, null);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, List.of(toolCall));
            } else {
                logger.debug("Unknown content block start type [{}] for line [{}].", type, data);
                return Stream.empty();
            }
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
            return Stream.of(chunk);
        }
    }

    /**
     * Parse a content block delta event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockDelta(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var index = parseNumberField(jsonParser, INDEX_FIELD);
            var type = parseStringField(jsonParser, TYPE_FIELD);
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
            if (type.equals(TEXT_DELTA_TYPE)) {
                var text = parseStringField(jsonParser, TEXT_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null);
            } else if (type.equals(INPUT_JSON_DELTA_TYPE)) {
                var partialJson = parseStringField(jsonParser, PARTIAL_JSON_FIELD);
                var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                    partialJson,
                    null
                );
                var toolCall = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(0, null, function, null);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, List.of(toolCall));
            } else {
                logger.debug("Unknown content block delta type [{}] for line [{}].", type, data);
                return Stream.empty();
            }

            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);

            return Stream.of(chunk);
        }
    }

    /**
     * Parse a message delta event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageDelta(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var finishReason = parseStringOrNullField(jsonParser, STOP_REASON_FIELD);
            var totalTokens = parseNumberField(jsonParser, OUTPUT_TOKENS_FIELD);

            var chunk = buildChatCompletionChunk(totalTokens, finishReason);

            return Stream.of(chunk);
        }
    }

    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk buildChatCompletionChunk(
        int totalTokens,
        String finishReason
    ) {
        var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(totalTokens, 0, totalTokens);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null),
            finishReason,
            0
        );
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, usage);
    }

    private static int parseNumberField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, jsonParser.currentToken(), jsonParser);
        return jsonParser.intValue();
    }

    private static String parseStringField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, jsonParser.currentToken(), jsonParser);
        return jsonParser.text();
    }

    private static String parseStringOrNullField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        return jsonParser.textOrNull();
    }

    private static Object parseFieldValue(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        return parseFieldsValue(jsonParser);
    }

}
