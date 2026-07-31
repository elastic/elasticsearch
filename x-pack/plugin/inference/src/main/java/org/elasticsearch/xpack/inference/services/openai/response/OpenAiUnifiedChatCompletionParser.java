/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.completion.Choice;
import org.elasticsearch.xpack.core.inference.results.completion.Message;
import org.elasticsearch.xpack.core.inference.results.completion.ToolCall;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunk;
import org.elasticsearch.xpack.core.inference.results.completion.Usage;
import org.elasticsearch.xpack.core.inference.results.completion.Usage.CompletionTokenDetails;
import org.elasticsearch.xpack.core.inference.results.completion.Usage.PromptTokensDetails;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHED_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_WRITE_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHOICES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DELTA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FINISH_REASON_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_ARGUMENTS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.OBJECT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REFUSAL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALLS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOTAL_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.USAGE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Shared parser for OpenAI-compatible chat completion responses (streaming and non-streaming).
 *
 * <p>Two axes of variation are handled by a factory chain parameterized at class-load time:
 * <ul>
 *   <li><b>Choice content field</b>: streaming uses {@code delta}; non-streaming uses {@code message}.
 *       The inner field names ({@code content}, {@code role}, {@code tool_calls}, etc.) are identical.</li>
 *   <li><b>Tool-call {@code index}</b>: streaming keeps it as a required constructor arg so a missing
 *       {@code index} fails loudly — streaming deltas need it to reassemble fragmented arguments into
 *       the correct call. Non-streaming makes it optional, falling back to positional numbering within
 *       each choice's {@code tool_calls} array.</li>
 * </ul>
 *
 * <p>Both {@link #parseStreamingChunk} and {@link #parseNonStreamingResponse} return a
 * {@link ChatCompletionChunk}. Streaming callers pass it to
 * {@code StreamingUnifiedChatCompletionResults.Results} which calls
 * {@link ChatCompletionChunk#toStreamingXContentChunked}; non-streaming callers use
 * {@link ChatCompletionChunk#toXContentChunked} via {@code InferenceAction.Response}.
 */
public final class OpenAiUnifiedChatCompletionParser {

    /**
     * Sentinel value stored in {@link ToolCall#index()} when the field was absent in the JSON.
     * Replaced by the positional index in {@link #parseToolCallsWithPositionalIndex}.
     */
    private static final int UNSET_INDEX = -1;

    private static final ConstructingObjectParser<ToolCall.Function, Void> FUNCTION_PARSER;
    private static final ConstructingObjectParser<CompletionTokenDetails, Void> COMPLETION_TOKENS_DETAILS_PARSER;
    private static final ConstructingObjectParser<PromptTokensDetails, Void> PROMPT_TOKENS_DETAILS_PARSER;
    private static final ConstructingObjectParser<Usage, Void> USAGE_PARSER;

    private static final ConstructingObjectParser<ChatCompletionChunk, Void> STREAMING_PARSER;
    private static final ConstructingObjectParser<ChatCompletionChunk, Void> NON_STREAMING_PARSER;

    static {
        FUNCTION_PARSER = new ConstructingObjectParser<>(
            FUNCTION_FIELD,
            true,
            args -> new ToolCall.Function((String) args[0], (String) args[1])
        );
        FUNCTION_PARSER.declareString(optionalConstructorArg(), new ParseField(FUNCTION_ARGUMENTS_FIELD));
        FUNCTION_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField(FUNCTION_NAME_FIELD));

        COMPLETION_TOKENS_DETAILS_PARSER = new ConstructingObjectParser<>(
            COMPLETION_TOKENS_DETAILS_FIELD,
            true,
            args -> new CompletionTokenDetails((Integer) args[0])
        );
        COMPLETION_TOKENS_DETAILS_PARSER.declareInt(optionalConstructorArg(), new ParseField(REASONING_TOKENS_FIELD));

        PROMPT_TOKENS_DETAILS_PARSER = new ConstructingObjectParser<>(
            PROMPT_TOKENS_DETAILS_FIELD,
            true,
            args -> new PromptTokensDetails((Integer) args[0], (Integer) args[1])
        );
        PROMPT_TOKENS_DETAILS_PARSER.declareInt(optionalConstructorArg(), new ParseField(CACHED_TOKENS_FIELD));
        PROMPT_TOKENS_DETAILS_PARSER.declareInt(optionalConstructorArg(), new ParseField(CACHE_WRITE_TOKENS_FIELD));

        USAGE_PARSER = new ConstructingObjectParser<>(
            USAGE_FIELD,
            true,
            args -> new Usage((int) args[0], (int) args[1], (int) args[2], (PromptTokensDetails) args[3], (CompletionTokenDetails) args[4])
        );
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(COMPLETION_TOKENS_FIELD));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(PROMPT_TOKENS_FIELD));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(TOTAL_TOKENS_FIELD));
        USAGE_PARSER.declareObject(optionalConstructorArg(), PROMPT_TOKENS_DETAILS_PARSER, new ParseField(PROMPT_TOKENS_DETAILS_FIELD));
        USAGE_PARSER.declareObjectOrNull(
            optionalConstructorArg(),
            COMPLETION_TOKENS_DETAILS_PARSER,
            null,
            new ParseField(COMPLETION_TOKENS_DETAILS_FIELD)
        );

        STREAMING_PARSER = buildRootParser(DELTA_FIELD, true);
        NON_STREAMING_PARSER = buildRootParser(MESSAGE_FIELD, false);
    }

    public static ChatCompletionChunk parseStreamingChunk(XContentParser parser) throws IOException {
        return STREAMING_PARSER.parse(parser, null);
    }

    public static ChatCompletionChunk parseNonStreamingResponse(XContentParser parser) throws IOException {
        return NON_STREAMING_PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ChatCompletionChunk, Void> buildRootParser(
        String choiceContentField,
        boolean indexRequired
    ) {
        var choiceParser = buildChoiceParser(choiceContentField, buildMessageParser(buildToolCallParser(indexRequired), indexRequired));
        var parser = new ConstructingObjectParser<ChatCompletionChunk, Void>(
            "chat_completion_chunk",
            true,
            args -> new ChatCompletionChunk(
                (String) args[0],
                (List<Choice>) args[1],
                Objects.requireNonNullElse((String) args[2], ""),
                Objects.requireNonNullElse((String) args[3], ""),
                (Usage) args[4]
            )
        );
        parser.declareString(constructorArg(), new ParseField(ID_FIELD));
        parser.declareObjectArray(constructorArg(), choiceParser, new ParseField(CHOICES_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(MODEL_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(OBJECT_FIELD));
        parser.declareObjectOrNull(optionalConstructorArg(), USAGE_PARSER, null, new ParseField(USAGE_FIELD));
        return parser;
    }

    private static ConstructingObjectParser<Choice, Void> buildChoiceParser(
        String contentFieldName,
        ConstructingObjectParser<Message, Void> messageParser
    ) {
        var parser = new ConstructingObjectParser<Choice, Void>(
            CHOICES_FIELD,
            true,
            args -> new Choice((Message) args[0], (String) args[1], (int) args[2])
        );
        parser.declareObject(constructorArg(), messageParser, new ParseField(contentFieldName));
        parser.declareStringOrNull(optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
        parser.declareInt(constructorArg(), new ParseField(INDEX_FIELD));
        return parser;
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Message, Void> buildMessageParser(
        ConstructingObjectParser<ToolCall, Void> toolCallParser,
        boolean indexRequired
    ) {
        var parser = new ConstructingObjectParser<Message, Void>(
            "message_delta",
            true,
            args -> new Message(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (List<ToolCall>) args[3],
                (String) args[4],
                (List<ReasoningDetail>) args[5]
            )
        );
        parser.declareStringOrNull(optionalConstructorArg(), new ParseField(CONTENT_FIELD));
        parser.declareStringOrNull(optionalConstructorArg(), new ParseField(REFUSAL_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(ROLE_FIELD));

        if (indexRequired) {
            parser.declareObjectArrayOrNull(optionalConstructorArg(), toolCallParser::apply, new ParseField(TOOL_CALLS_FIELD));
        } else {
            // Non-streaming: index may be absent; assign by array position.
            parser.declareField(
                optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : parseToolCallsWithPositionalIndex(p, toolCallParser),
                new ParseField(TOOL_CALLS_FIELD),
                ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL
            );
        }

        parser.declareString(optionalConstructorArg(), new ParseField(REASONING_FIELD));
        parser.declareObjectArrayOrNull(
            optionalConstructorArg(),
            ReasoningDetail.RESPONSE_PARSER::apply,
            new ParseField(REASONING_DETAILS_FIELD)
        );
        return parser;
    }

    private static ConstructingObjectParser<ToolCall, Void> buildToolCallParser(boolean indexRequired) {
        var parser = new ConstructingObjectParser<ToolCall, Void>(
            TOOL_CALLS_FIELD,
            true,
            args -> new ToolCall(
                args[0] == null ? UNSET_INDEX : (int) args[0],
                (String) args[1],
                (ToolCall.Function) args[2],
                (String) args[3]
            )
        );
        if (indexRequired) {
            parser.declareInt(constructorArg(), new ParseField(INDEX_FIELD));
        } else {
            parser.declareInt(optionalConstructorArg(), new ParseField(INDEX_FIELD));
        }
        parser.declareString(optionalConstructorArg(), new ParseField(ID_FIELD));
        parser.declareObject(optionalConstructorArg(), FUNCTION_PARSER, new ParseField(FUNCTION_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(TYPE_FIELD));
        return parser;
    }

    /**
     * Parses a {@code tool_calls} array, replacing any {@link #UNSET_INDEX} with the element's position.
     * A fresh counter starts at 0 for each call, so numbering restarts per-choice.
     */
    private static List<ToolCall> parseToolCallsWithPositionalIndex(
        XContentParser parser,
        ConstructingObjectParser<ToolCall, Void> toolCallParser
    ) throws IOException {
        var position = new AtomicInteger();
        return AbstractObjectParser.parseArray(parser, null, (itemParser, ctx) -> {
            var pos = position.getAndIncrement();
            var toolCall = toolCallParser.parse(itemParser, null);
            return toolCall.index() == UNSET_INDEX ? new ToolCall(pos, toolCall.id(), toolCall.function(), toolCall.type()) : toolCall;
        });
    }

    private OpenAiUnifiedChatCompletionParser() {}
}
