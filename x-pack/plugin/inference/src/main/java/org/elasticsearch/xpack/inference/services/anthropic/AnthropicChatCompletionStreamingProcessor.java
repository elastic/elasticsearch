/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChoiceResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunk;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionMessage;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionToolCall;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsage;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private static final String CACHE_READ_INPUT_TOKENS_FIELD = "cache_read_input_tokens";
    private static final String CACHE_CREATION_INPUT_TOKENS_FIELD = "cache_creation_input_tokens";
    private static final String STOP_REASON_FIELD = "stop_reason";
    private static final String TEXT_FIELD = "text";
    private static final String THINKING_FIELD = "thinking";
    private static final String SIGNATURE_FIELD = "signature";
    private static final String DATA_FIELD = "data";
    private static final String PARTIAL_JSON_FIELD = "partial_json";
    private static final String USAGE_FIELD = "usage";
    private static final String MESSAGE_FIELD = "message";
    private static final String CONTENT_BLOCK_FIELD = "content_block";
    private static final String DELTA_FIELD = "delta";

    // Event types
    private static final String MESSAGE_START_EVENT_TYPE = "message_start";
    private static final String MESSAGE_DELTA_EVENT_TYPE = "message_delta";
    private static final String MESSAGE_STOP_EVENT_TYPE = "message_stop";
    private static final String CONTENT_BLOCK_START_EVENT_TYPE = "content_block_start";
    private static final String CONTENT_BLOCK_DELTA_EVENT_TYPE = "content_block_delta";
    private static final String CONTENT_BLOCK_STOP_EVENT_TYPE = "content_block_stop";
    private static final String PING_EVENT_TYPE = "ping";
    private static final String VERTEX_EVENT_EVENT_TYPE = "vertex_event";
    private static final String ERROR_EVENT_TYPE = "error";

    // Content block and delta types
    private static final String TEXT_TYPE = "text";
    private static final String TEXT_DELTA_TYPE = "text_delta";
    private static final String TOOL_USE_TYPE = "tool_use";
    private static final String INPUT_JSON_DELTA_TYPE = "input_json_delta";
    private static final String THINKING_TYPE = "thinking";
    private static final String THINKING_DELTA_TYPE = "thinking_delta";
    private static final String REDACTED_THINKING_TYPE = "redacted_thinking";
    private static final String SIGNATURE_DELTA_TYPE = "signature_delta";

    // Anthropic stop reason values
    private static final String ANTHROPIC_STOP_REASON_END_TURN = "end_turn";
    private static final String ANTHROPIC_STOP_REASON_MAX_TOKENS = "max_tokens";
    private static final String ANTHROPIC_STOP_REASON_STOP_SEQUENCE = "stop_sequence";
    private static final String ANTHROPIC_STOP_REASON_TOOL_USE = "tool_use";
    private static final String ANTHROPIC_STOP_REASON_PAUSE_TURN = "pause_turn";
    private static final String ANTHROPIC_STOP_REASON_REFUSAL = "refusal";

    // OpenAI finish reasons
    private static final String STOP_FINISH_REASON = "stop";
    private static final String LENGTH_FINISH_REASON = "length";
    private static final String TOOL_CALLS_FINISH_REASON = "tool_calls";
    private static final String CONTENT_FILTER_FINISH_REASON = "content_filter";

    // Other constants
    private static final String OBJECT_VALUE = "chat.completion.chunk";
    private static final String FUNCTION_TYPE = "function";
    private static final String ANTHROPIC_CLAUDE_V1_FORMAT = "anthropic-claude-v1";

    // Per-stream state
    private String id;
    private String model;
    private int toolCallCount;
    private final Map<Integer, Integer> contentBlockIndexToToolCallIndex = new HashMap<>();
    private int reasoningBlockCount;
    private final Map<Integer, Integer> contentBlockIndexToReasoningIndex = new HashMap<>();
    private int inputTokens;
    private int outputTokens;
    private int cacheReadTokens;
    private int cacheCreationTokens;

    private final BiFunction<String, Exception, Exception> errorParser;
    private final boolean excludeReasoning;

    public AnthropicChatCompletionStreamingProcessor(BiFunction<String, Exception, Exception> errorParser, boolean excludeReasoning) {
        this.errorParser = errorParser;
        this.excludeReasoning = excludeReasoning;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<ChatCompletionChunk>(item.size());

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
    private Stream<ChatCompletionChunk> parse(XContentParserConfiguration parserConfig, ServerSentEvent event) throws IOException {
        // Handle known event types
        switch (event.type()) {
            case VERTEX_EVENT_EVENT_TYPE, PING_EVENT_TYPE, CONTENT_BLOCK_STOP_EVENT_TYPE:
                logger.debug("Skipping event type [{}].", event.type());
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
                return buildMessageStop();
            case null, default:
                logger.debug("Unknown event type [{}].", event.type());
                return Stream.empty();
        }
    }

    /**
     * Handles a message_start event; captures id, model, and initial usage into state and emits a role chunk.
     * Example:
     * <pre><code>
     * {
     *     "type": "message_start",
     *     "message": {
     *         "model": "claude-3-5-haiku-20241022",
     *         "id": "msg_vrtx_01XTaeM2111A1r9tCnM3PCh3",
     *         "type": "message",
     *         "role": "assistant",
     *         "stop_reason": null,
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
     * @return a stream of {@link ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<ChatCompletionChunk> parseMessageStart(XContentParser parser) throws IOException {
        var messageMap = extractInnerStringObjectMap(parser.map(), MESSAGE_FIELD);
        model = extractMandatoryString(messageMap, MODEL_FIELD);
        id = extractMandatoryString(messageMap, ID_FIELD);
        var role = extractMandatoryString(messageMap, ROLE_FIELD);
        var finishReason = convertStopReason(extractOptionalString(messageMap, STOP_REASON_FIELD));
        var usageMap = extractInnerStringObjectMap(messageMap, USAGE_FIELD);
        inputTokens = extractMandatoryInteger(usageMap, INPUT_TOKENS_FIELD);
        outputTokens = extractMandatoryInteger(usageMap, OUTPUT_TOKENS_FIELD);
        cacheReadTokens = Objects.requireNonNullElse(extractOptionalInteger(usageMap, CACHE_READ_INPUT_TOKENS_FIELD), 0);
        cacheCreationTokens = Objects.requireNonNullElse(extractOptionalInteger(usageMap, CACHE_CREATION_INPUT_TOKENS_FIELD), 0);

        var delta = new ChatCompletionMessage(null, null, role, null);
        var choice = new ChatCompletionChoiceResponse(delta, finishReason, 0);
        return Stream.of(newChunk(List.of(choice), null));
    }

    /**
     * Handles a content_block_start event for text, tool_use, thinking, and redacted_thinking blocks.
     * Examples:
     * <pre><code>
     * {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}
     * {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool_id","name":"get_weather","input":{}}}
     * {"type":"content_block_start","index":2,"content_block":{"type":"thinking","thinking":"..."}}
     * {"type":"content_block_start","index":3,"content_block":{"type":"redacted_thinking","data":"..."}}
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<ChatCompletionChunk> parseContentBlockStart(XContentParser parser) throws IOException {
        var outerMap = parser.map();
        var blockIndex = extractMandatoryInteger(outerMap, INDEX_FIELD);
        var contentBlockMap = extractInnerStringObjectMap(outerMap, CONTENT_BLOCK_FIELD);
        var type = extractMandatoryString(contentBlockMap, TYPE_FIELD);

        ChatCompletionMessage delta;
        switch (type) {
            case TEXT_TYPE -> {
                var text = extractMandatoryString(contentBlockMap, TEXT_FIELD);
                delta = new ChatCompletionMessage(text, null, null, null, null, null);
            }
            case TOOL_USE_TYPE -> {
                var id = extractMandatoryString(contentBlockMap, ID_FIELD);
                var name = extractMandatoryString(contentBlockMap, NAME_FIELD);
                var toolCallIndex = toolCallCount++;
                contentBlockIndexToToolCallIndex.put(blockIndex, toolCallIndex);
                // A tool_use content block start always carries an empty input object; the actual tool input arrives
                // as input_json_delta fragments, which clients concatenate onto the arguments, so seed them empty.
                var function = new ChatCompletionToolCall.Function("", name);
                var toolCall = new ChatCompletionToolCall(toolCallIndex, id, function, FUNCTION_TYPE);
                delta = new ChatCompletionMessage(null, null, null, List.of(toolCall));
            }
            case THINKING_TYPE -> {
                if (excludeReasoning) {
                    return Stream.empty();
                }
                var thinking = extractMandatoryString(contentBlockMap, THINKING_FIELD);
                var reasoningIdx = (long) reasoningBlockCount++;
                contentBlockIndexToReasoningIndex.put(blockIndex, (int) reasoningIdx);
                var reasoningDetail = new ReasoningDetail.TextReasoningDetail(
                    ANTHROPIC_CLAUDE_V1_FORMAT,
                    null,
                    reasoningIdx,
                    thinking,
                    null
                );
                delta = new ChatCompletionMessage(null, null, null, null, thinking, List.of(reasoningDetail));
            }
            case REDACTED_THINKING_TYPE -> {
                // Anthropic only emits redacted_thinking when extended thinking was enabled on the request;
                // if reasoning is excluded the entire block is dropped before being registered.
                if (excludeReasoning) {
                    return Stream.empty();
                }
                var data = extractMandatoryString(contentBlockMap, DATA_FIELD);
                var reasoningIdx = (long) reasoningBlockCount++;
                contentBlockIndexToReasoningIndex.put(blockIndex, (int) reasoningIdx);
                var reasoningDetail = new ReasoningDetail.EncryptedReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, reasoningIdx, data);
                delta = new ChatCompletionMessage(null, null, null, null, null, List.of(reasoningDetail));
            }
            default -> {
                logger.debug("Unknown content block start type [{}].", type);
                return Stream.empty();
            }
        }
        // Anthropic streams a single message, so the chunk always holds one choice at index 0; parallel tool calls are
        // distinguished by the tool call index, not the choice index.
        var choice = new ChatCompletionChoiceResponse(delta, null, 0);
        return Stream.of(newChunk(List.of(choice), null));
    }

    /**
     * Handles a content_block_delta event for text, tool input, thinking, and signature deltas.
     * Examples:
     * <pre><code>
     * {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}
     * {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"loc\":"}}
     * {"type":"content_block_delta","index":2,"delta":{"type":"thinking_delta","thinking":"..."}}
     * {"type":"content_block_delta","index":2,"delta":{"type":"signature_delta","signature":"..."}}
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    private Stream<ChatCompletionChunk> parseContentBlockDelta(XContentParser parser) throws IOException {
        var outerMap = parser.map();
        var blockIndex = extractMandatoryInteger(outerMap, INDEX_FIELD);
        var deltaMap = extractInnerStringObjectMap(outerMap, DELTA_FIELD);
        var type = extractMandatoryString(deltaMap, TYPE_FIELD);

        ChatCompletionMessage delta;
        switch (type) {
            case TEXT_DELTA_TYPE -> {
                var text = extractMandatoryString(deltaMap, TEXT_FIELD);
                delta = new ChatCompletionMessage(text, null, null, null, null, null);
            }
            case INPUT_JSON_DELTA_TYPE -> {
                var partialJson = extractMandatoryString(deltaMap, PARTIAL_JSON_FIELD);
                var toolCallIndex = contentBlockIndexToToolCallIndex.get(blockIndex);
                if (toolCallIndex == null) {
                    logger.debug("Received [{}] for unknown content block index [{}].", INPUT_JSON_DELTA_TYPE, blockIndex);
                    return Stream.empty();
                }
                var function = new ChatCompletionToolCall.Function(partialJson, null);
                var toolCall = new ChatCompletionToolCall(toolCallIndex, null, function, null);
                delta = new ChatCompletionMessage(null, null, null, List.of(toolCall), null, null);
            }
            case THINKING_DELTA_TYPE -> {
                if (excludeReasoning) {
                    return Stream.empty();
                }
                var thinking = extractMandatoryString(deltaMap, THINKING_FIELD);
                var reasoningIdx = contentBlockIndexToReasoningIndex.get(blockIndex);
                if (reasoningIdx == null) {
                    logger.debug("Received [{}] for unknown content block index [{}].", THINKING_DELTA_TYPE, blockIndex);
                    return Stream.empty();
                }
                var reasoningDetail = new ReasoningDetail.TextReasoningDetail(
                    ANTHROPIC_CLAUDE_V1_FORMAT,
                    null,
                    (long) reasoningIdx,
                    thinking,
                    null
                );
                delta = new ChatCompletionMessage(null, null, null, null, thinking, List.of(reasoningDetail));
            }
            case SIGNATURE_DELTA_TYPE -> {
                if (excludeReasoning) {
                    return Stream.empty();
                }
                var signature = extractMandatoryString(deltaMap, SIGNATURE_FIELD);
                var reasoningIdx = contentBlockIndexToReasoningIndex.get(blockIndex);
                if (reasoningIdx == null) {
                    logger.debug("Received [{}] for unknown content block index [{}].", SIGNATURE_DELTA_TYPE, blockIndex);
                    return Stream.empty();
                }
                var reasoningDetail = new ReasoningDetail.TextReasoningDetail(
                    ANTHROPIC_CLAUDE_V1_FORMAT,
                    null,
                    (long) reasoningIdx,
                    null,
                    signature
                );
                delta = new ChatCompletionMessage(null, null, null, null, null, List.of(reasoningDetail));
            }
            default -> {
                logger.debug("Unknown content block delta type [{}].", type);
                return Stream.empty();
            }
        }
        // Anthropic streams a single message, so the chunk always holds one choice at index 0; parallel tool calls are
        // distinguished by the tool call index, not the choice index.
        var choice = new ChatCompletionChoiceResponse(delta, null, 0);
        return Stream.of(newChunk(List.of(choice), null));
    }

    /**
     * Handles a message_delta event; updates cumulative usage and emits a finish-reason chunk when stop_reason is set.
     * Example:
     * <pre><code>
     * {
     *     "type": "message_delta",
     *     "delta": {"stop_reason": "tool_use", "stop_sequence": null},
     *     "usage": {"output_tokens": 41}
     * }
     * </code></pre>
     * @param parser the parser positioned at the root {@code START_OBJECT}
     * @return a stream of {@link ChatCompletionChunk}
     * @throws IOException if parsing fails
     */
    public Stream<ChatCompletionChunk> parseMessageDelta(XContentParser parser) throws IOException {
        var outerMap = parser.map();
        var deltaMap = extractInnerStringObjectMap(outerMap, DELTA_FIELD);
        var stopReason = extractOptionalString(deltaMap, STOP_REASON_FIELD);
        var usageMap = extractInnerStringObjectMap(outerMap, USAGE_FIELD);

        outputTokens = extractMandatoryInteger(usageMap, OUTPUT_TOKENS_FIELD);
        var updatedInput = extractOptionalInteger(usageMap, INPUT_TOKENS_FIELD);
        if (updatedInput != null && updatedInput > 0) {
            inputTokens = updatedInput;
        }
        var updatedCacheRead = extractOptionalInteger(usageMap, CACHE_READ_INPUT_TOKENS_FIELD);
        if (updatedCacheRead != null && updatedCacheRead > 0) {
            cacheReadTokens = updatedCacheRead;
        }
        var updatedCacheCreation = extractOptionalInteger(usageMap, CACHE_CREATION_INPUT_TOKENS_FIELD);
        if (updatedCacheCreation != null && updatedCacheCreation > 0) {
            cacheCreationTokens = updatedCacheCreation;
        }

        if (stopReason == null) {
            return Stream.empty();
        }

        var finishReason = convertStopReason(stopReason);
        var emptyDelta = new ChatCompletionMessage(null, null, null, null, null, null);
        var choice = new ChatCompletionChoiceResponse(emptyDelta, finishReason, 0);
        return Stream.of(newChunk(List.of(choice), null));
    }

    /**
     * Builds a usage-only chunk from accumulated state, emitted on message_stop.
     * Usage is emitted once here rather than at message_start/message_delta to avoid double-counting.
     */
    private Stream<ChatCompletionChunk> buildMessageStop() {
        var promptTokens = inputTokens + cacheReadTokens + cacheCreationTokens;
        var totalTokens = promptTokens + outputTokens;
        Integer cachedTokens = cacheReadTokens > 0 ? cacheReadTokens : null;
        Integer cachedWriteTokens = cacheCreationTokens > 0 ? cacheCreationTokens : null;
        var promptTokensDetails = ChatCompletionUsage.PromptTokensDetails.ofNullable(cachedTokens, cachedWriteTokens);
        var usage = new ChatCompletionUsage(outputTokens, promptTokens, totalTokens, promptTokensDetails, null);
        return Stream.of(newChunk(List.of(), usage));
    }

    private ChatCompletionChunk newChunk(@Nullable List<ChatCompletionChoiceResponse> choices, @Nullable ChatCompletionUsage usage) {
        return new ChatCompletionChunk(id, choices, model, OBJECT_VALUE, usage);
    }

    private static String convertStopReason(@Nullable String stopReason) {
        if (stopReason == null) {
            return null;
        }
        return switch (stopReason) {
            case ANTHROPIC_STOP_REASON_END_TURN, ANTHROPIC_STOP_REASON_STOP_SEQUENCE, ANTHROPIC_STOP_REASON_PAUSE_TURN ->
                STOP_FINISH_REASON;
            case ANTHROPIC_STOP_REASON_MAX_TOKENS -> LENGTH_FINISH_REASON;
            case ANTHROPIC_STOP_REASON_TOOL_USE -> TOOL_CALLS_FINISH_REASON;
            case ANTHROPIC_STOP_REASON_REFUSAL -> CONTENT_FILTER_FINISH_REASON;
            default -> {
                logger.debug("Unhandled Anthropic stop reason [{}], defaulting to [{}].", stopReason, STOP_FINISH_REASON);
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

    private static Integer extractOptionalInteger(Map<String, Object> map, String fieldName) {
        return extractOptionalField(map, fieldName, Integer.class);
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
