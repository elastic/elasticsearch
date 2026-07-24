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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.parseObjects;

/**
 * Chat Completions Streaming Processor for Anthropic provider.
 *
 * <p>Stateful: one instance handles exactly one response stream. Anthropic identifies streamed tool calls by their content
 * block index, whereas the unified format expects each tool call to carry a monotonically increasing index of its own (the
 * content block index also counts text blocks, so the two numberings diverge); the mapping between them is tracked across
 * events.
 */
public class AnthropicChatCompletionStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Anthropic chat completions response";
    private static final String UNEXPECTED_FIELD_TYPE_TEMPLATE = """
        Field [%s] in Anthropic chat completions response is of unexpected type [%s]. \
        Expected type is [%s].""";
    private static final Logger logger = LogManager.getLogger(AnthropicChatCompletionStreamingProcessor.class);

    // Field names
    private static final String ROLE_FIELD = "role";
    private static final String INDEX_FIELD = "index";
    private static final String TYPE_FIELD = "type";
    private static final String MODEL_FIELD = "model";
    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String INPUT_TOKENS_FIELD = "input_tokens";
    private static final String OUTPUT_TOKENS_FIELD = "output_tokens";
    private static final String STOP_REASON_FIELD = "stop_reason";
    private static final String TEXT_FIELD = "text";
    private static final String PARTIAL_JSON_FIELD = "partial_json";
    private static final String USAGE_FIELD = "usage";
    private static final String MESSAGE_FIELD = "message";
    private static final String CONTENT_BLOCK_FIELD = "content_block";
    private static final String DELTA_FIELD = "delta";

    // Event types
    private static final String MESSAGE_DELTA_EVENT_TYPE = "message_delta";
    private static final String CONTENT_BLOCK_START_EVENT_TYPE = "content_block_start";
    private static final String MESSAGE_START_EVENT_TYPE = "message_start";
    private static final String VERTEX_EVENT_EVENT_TYPE = "vertex_event";
    private static final String PING_EVENT_TYPE = "ping";
    private static final String CONTENT_BLOCK_STOP_EVENT_TYPE = "content_block_stop";
    private static final String CONTENT_BLOCK_DELTA_EVENT_TYPE = "content_block_delta";
    private static final String MESSAGE_STOP_EVENT_TYPE = "message_stop";
    private static final String ERROR_EVENT_TYPE = "error";

    // Content block types
    private static final String TEXT_DELTA_TYPE = "text_delta";
    private static final String INPUT_JSON_DELTA_TYPE = "input_json_delta";
    private static final String TOOL_USE_TYPE = "tool_use";
    private static final String TEXT_TYPE = "text";

    // Anthropic stop reasons
    private static final String END_TURN_STOP_REASON = "end_turn";
    private static final String STOP_SEQUENCE_STOP_REASON = "stop_sequence";
    private static final String PAUSE_TURN_STOP_REASON = "pause_turn";
    private static final String MAX_TOKENS_STOP_REASON = "max_tokens";
    private static final String TOOL_USE_STOP_REASON = "tool_use";
    private static final String REFUSAL_STOP_REASON = "refusal";

    // OpenAI finish reasons
    private static final String STOP_FINISH_REASON = "stop";
    private static final String LENGTH_FINISH_REASON = "length";
    private static final String TOOL_CALLS_FINISH_REASON = "tool_calls";
    private static final String CONTENT_FILTER_FINISH_REASON = "content_filter";

    private final BiFunction<String, Exception, Exception> errorParser;

    private int toolCallCount = 0;
    private final Map<Integer, Integer> contentBlockIndexToToolCallIndex = new HashMap<>();

    public AnthropicChatCompletionStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this.errorParser = errorParser;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(item.size());

        for (var event : item) {
            if (ERROR_EVENT_TYPE.equals(event.type()) && event.hasData()) {
                throw errorParser.apply(event.data(), null);
            } else if (event.hasData()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.forEach(results::offer);
                } catch (Exception e) {
                    logger.warn("Failed to parse event from Anthropic inference provider: {}", event);
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
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parse(
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
                return parseObjects(parserConfig, event.data(), this::parseMessageStart);
            case CONTENT_BLOCK_START_EVENT_TYPE:
                return parseObjects(parserConfig, event.data(), this::parseContentBlockStart);
            case CONTENT_BLOCK_DELTA_EVENT_TYPE:
                return parseObjects(parserConfig, event.data(), this::parseContentBlockDelta);
            case MESSAGE_DELTA_EVENT_TYPE:
                return parseObjects(parserConfig, event.data(), this::parseMessageDelta);
            case MESSAGE_STOP_EVENT_TYPE:
                return Stream.empty();
            case null, default:
                logger.debug("Unknown event type [{}] for line [{}].", event.type(), event.data());
                return Stream.empty();
        }
    }

    /**
     * Parse a message start event into a ChatCompletionChunk stream. Example of Anthropic message start:
     * <pre><code>
     * {
     *     "type": "message_start",
     *     "message": {
     *         "model": "claude-3-5-haiku-20241022",
     *         "id": "msg_vrtx_01XTaeM2111A1r9tCnM3PCh3",
     *         "type": "message",
     *         "role": "assistant",
     *         "content": [],
     *         "stop_reason": null,
     *         "stop_sequence": null,
     *         "usage": {
     *             "input_tokens": 13,
     *             "cache_creation_input_tokens": 0,
     *             "cache_read_input_tokens": 0,
     *             "output_tokens": 1
     *         }
     *     }
     * }
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link StreamingUnifiedChatCompletionResults.ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageStart(XContentParser parser)
        throws IOException {
        var messageMap = extractInnerStringObjectMap(parser.map(), MESSAGE_FIELD);
        var model = extractMandatoryString(messageMap, MODEL_FIELD);
        var id = extractMandatoryString(messageMap, ID_FIELD);
        var role = extractMandatoryString(messageMap, ROLE_FIELD);
        var finishReason = convertStopReason(extractOptionalString(messageMap, STOP_REASON_FIELD));
        var usageMap = extractInnerStringObjectMap(messageMap, USAGE_FIELD);
        var promptTokens = extractMandatoryInteger(usageMap, INPUT_TOKENS_FIELD);
        var completionTokens = extractMandatoryInteger(usageMap, OUTPUT_TOKENS_FIELD);
        var totalTokens = completionTokens + promptTokens;

        var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(completionTokens, promptTokens, totalTokens);
        var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, role, null);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, finishReason, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(id, List.of(choice), model, null, usage);

        return Stream.of(chunk);
    }

    /**
     * Parse a content block start event into a ChatCompletionChunk stream. Examples of Anthropic text content block:
     * <pre><code>
     * {
     *     "type": "content_block_start",
     *     "index": 0,
     *     "content_block": {
     *         "type": "text",
     *         "text": ""
     *     }
     * }
     * </code></pre>
     * or
     * <pre><code>
     * {
     *     "type": "content_block_start",
     *     "index": 0,
     *     "content_block": {
     *         "type": "tool_use",
     *         "id": "toolu_vrtx_01FeQCP5fWgjDfP2SDEtcdJF",
     *         "name": "get_weather",
     *         "input": {}
     *     }
     * }
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link StreamingUnifiedChatCompletionResults.ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockStart(XContentParser parser)
        throws IOException {
        var outerMap = parser.map();
        var index = extractMandatoryInteger(outerMap, INDEX_FIELD);
        var contentBlockMap = extractInnerStringObjectMap(outerMap, CONTENT_BLOCK_FIELD);
        var type = extractMandatoryString(contentBlockMap, TYPE_FIELD);
        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
        if (type.equals(TEXT_TYPE)) {
            var text = extractMandatoryString(contentBlockMap, TEXT_FIELD);
            delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null, null, null);
        } else if (type.equals(TOOL_USE_TYPE)) {
            var id = extractMandatoryString(contentBlockMap, ID_FIELD);
            var name = extractMandatoryString(contentBlockMap, NAME_FIELD);
            var toolCallIndex = toolCallCount++;
            contentBlockIndexToToolCallIndex.put(index, toolCallIndex);
            // A tool_use content block start always carries an empty input object; the actual tool input arrives
            // as input_json_delta fragments, which clients concatenate onto the arguments, so seed them empty.
            var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function("", name);
            var toolCall = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                toolCallIndex,
                id,
                function,
                null
            );
            delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, List.of(toolCall));
        } else {
            logger.debug("Unknown content block start type [{}] for line [{}].", type, outerMap);
            return Stream.empty();
        }
        // Anthropic streams a single message, so the chunk always holds one choice at index 0; parallel tool calls are
        // distinguished by the tool call index, not the choice index.
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    /**
     * Parse a content block delta event into a ChatCompletionChunk stream. Examples of Anthropic content block delta:
     * <pre><code>
     * {
     *     "type": "content_block_delta",
     *     "index": 0,
     *     "delta": {
     *         "type": "text_delta",
     *         "text": "Hello World"
     *     }
     * }
     * </code></pre>
     * or
     * <pre><code>
     * {
     *     "type": "content_block_delta",
     *     "index": 0,
     *     "delta": {
     *         "type": "input_json_delta",
     *         "partial_json": "{\"location\": \"San Francisco\"}"
     *     }
     * }
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link StreamingUnifiedChatCompletionResults.ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockDelta(XContentParser parser)
        throws IOException {
        var outerMap = parser.map();
        var index = extractMandatoryInteger(outerMap, INDEX_FIELD);
        var deltaMap = extractInnerStringObjectMap(outerMap, DELTA_FIELD);
        var type = extractMandatoryString(deltaMap, TYPE_FIELD);
        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
        if (type.equals(TEXT_DELTA_TYPE)) {
            var text = extractMandatoryString(deltaMap, TEXT_FIELD);
            delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null, null, null);
        } else if (type.equals(INPUT_JSON_DELTA_TYPE)) {
            var toolCallIndex = contentBlockIndexToToolCallIndex.get(index);
            if (toolCallIndex == null) {
                logger.warn("Received [{}] for unknown content block index [{}].", INPUT_JSON_DELTA_TYPE, index);
                return Stream.empty();
            }
            var partialJson = extractMandatoryString(deltaMap, PARTIAL_JSON_FIELD);
            var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(partialJson, null);
            var toolCall = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                toolCallIndex,
                null,
                function,
                null
            );
            delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                null,
                null,
                null,
                List.of(toolCall),
                null,
                null
            );
        } else {
            logger.debug("Unknown content block delta type [{}] for line [{}].", type, outerMap);
            return Stream.empty();
        }

        // Anthropic streams a single message, so the chunk always holds one choice at index 0; parallel tool calls are
        // distinguished by the tool call index, not the choice index.
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);

        return Stream.of(chunk);
    }

    /**
     * Parse a message delta event into a ChatCompletionChunk stream. Example of Anthropic message delta:
     * <pre><code>
     * {
     *     "type": "message_delta",
     *     "delta": {
     *         "stop_reason": "max_tokens",
     *         "stop_sequence": null
     *     },
     *     "usage": {
     *         "output_tokens": 10
     *     }
     * }
     * </code></pre>
     * or
     * <pre><code>
     * {
     *     "type": "message_delta",
     *     "delta": {
     *         "stop_reason": "tool_use",
     *         "stop_sequence": null
     *     },
     *     "usage": {
     *         "output_tokens": 41
     *     }
     * }
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link StreamingUnifiedChatCompletionResults.ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    public Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageDelta(XContentParser parser)
        throws IOException {
        var outerMap = parser.map();
        var deltaMap = extractInnerStringObjectMap(outerMap, DELTA_FIELD);
        var finishReason = convertStopReason(extractOptionalString(deltaMap, STOP_REASON_FIELD));
        var usageMap = extractInnerStringObjectMap(outerMap, USAGE_FIELD);
        var totalTokens = extractMandatoryInteger(usageMap, OUTPUT_TOKENS_FIELD);

        var chunk = buildChatCompletionChunk(totalTokens, finishReason);

        return Stream.of(chunk);
    }

    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk buildChatCompletionChunk(
        int totalTokens,
        @Nullable String finishReason
    ) {
        var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(totalTokens, 0, totalTokens);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null, null, null),
            finishReason,
            0
        );
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, usage);
    }

    /**
     * Converts an Anthropic stop reason to the equivalent OpenAI finish reason, so that OpenAI-compatible clients can
     * rely on the unified vocabulary (e.g. checking for [tool_calls] to decide whether to run tools). A null stop
     * reason means the message is still in progress and is passed through unchanged.
     */
    @Nullable
    private static String convertStopReason(@Nullable String stopReason) {
        if (stopReason == null) {
            return null;
        }
        return switch (stopReason) {
            case END_TURN_STOP_REASON, STOP_SEQUENCE_STOP_REASON, PAUSE_TURN_STOP_REASON -> STOP_FINISH_REASON;
            case MAX_TOKENS_STOP_REASON -> LENGTH_FINISH_REASON;
            case TOOL_USE_STOP_REASON -> TOOL_CALLS_FINISH_REASON;
            case REFUSAL_STOP_REASON -> CONTENT_FILTER_FINISH_REASON;
            default -> {
                logger.warn("Unhandled Anthropic stop reason [{}], defaulting to [{}].", stopReason, STOP_FINISH_REASON);
                yield STOP_FINISH_REASON;
            }
        };
    }

    private static String extractMandatoryString(Map<String, Object> map, String fieldName) {
        return extractMandatoryField(map, fieldName, String.class);
    }

    private static Integer extractMandatoryInteger(Map<String, Object> map, String fieldName) {
        return extractMandatoryField(map, fieldName, Integer.class);
    }

    private static String extractOptionalString(Map<String, Object> map, String fieldName) {
        return extractOptionalField(map, fieldName, String.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractInnerStringObjectMap(Map<String, Object> outerMap, String fieldName) {
        return extractMandatoryField(outerMap, fieldName, Map.class);
    }

    private static <T> T extractMandatoryField(Map<String, Object> map, String fieldName, Class<T> type) {
        Object value = map.get(fieldName);
        if (value == null) {
            throw new IllegalStateException(format(FAILED_TO_FIND_FIELD_TEMPLATE, fieldName));
        }
        return castFieldValueOrThrow(value, type, fieldName);
    }

    private static <T> T extractOptionalField(Map<String, Object> map, String fieldName, Class<T> type) {
        Object value = map.get(fieldName);
        if (value == null) {
            return null;
        }
        return castFieldValueOrThrow(value, type, fieldName);
    }

    @SuppressWarnings("unchecked")
    private static <T> T castFieldValueOrThrow(Object value, Class<T> type, String fieldName) {
        if (type.isInstance(value) == false) {
            throw new IllegalStateException(
                format(UNEXPECTED_FIELD_TYPE_TEMPLATE, fieldName, value.getClass().getSimpleName(), type.getSimpleName())
            );
        }
        return (T) value;
    }
}
