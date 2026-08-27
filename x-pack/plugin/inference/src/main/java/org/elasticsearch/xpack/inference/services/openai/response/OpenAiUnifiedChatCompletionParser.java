/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChoiceResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunk;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionMessage;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionToolCall;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsage;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsage.CompletionTokenDetails;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsage.PromptTokensDetails;

import java.io.IOException;
import java.util.ArrayList;
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
     * Sentinel value stored in {@link ChatCompletionToolCall#index()} when the field was absent in the JSON.
     * Replaced by the positional index in {@link #parseToolCallsWithPositionalIndex}.
     */
    private static final int UNSET_INDEX = -1;

    private static final ConstructingObjectParser<ChatCompletionToolCall.Function, Void> FUNCTION_PARSER;
    private static final ConstructingObjectParser<CompletionTokenDetails, Void> COMPLETION_TOKENS_DETAILS_PARSER;
    private static final ConstructingObjectParser<PromptTokensDetails, Void> PROMPT_TOKENS_DETAILS_PARSER;
    private static final ConstructingObjectParser<ChatCompletionUsage, Void> USAGE_PARSER;

    private static final ConstructingObjectParser<ChatCompletionChunk, Void> STREAMING_PARSER;
    private static final ConstructingObjectParser<ChatCompletionChunk, Void> NON_STREAMING_PARSER;

    static {
        FUNCTION_PARSER = new ConstructingObjectParser<>(
            FUNCTION_FIELD,
            true,
            args -> new ChatCompletionToolCall.Function((String) args[0], (String) args[1])
        );
        FUNCTION_PARSER.declareString(optionalConstructorArg(), new ParseField(FUNCTION_ARGUMENTS_FIELD));
        FUNCTION_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField(FUNCTION_NAME_FIELD));

        COMPLETION_TOKENS_DETAILS_PARSER = new ConstructingObjectParser<>(
            COMPLETION_TOKENS_DETAILS_FIELD,
            true,
            args -> CompletionTokenDetails.ofNullable((Integer) args[0])
        );
        COMPLETION_TOKENS_DETAILS_PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue(),
            new ParseField(REASONING_TOKENS_FIELD),
            ObjectParser.ValueType.INT_OR_NULL
        );

        PROMPT_TOKENS_DETAILS_PARSER = new ConstructingObjectParser<>(
            PROMPT_TOKENS_DETAILS_FIELD,
            true,
            args -> PromptTokensDetails.ofNullable((Integer) args[0], (Integer) args[1])
        );
        PROMPT_TOKENS_DETAILS_PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue(),
            new ParseField(CACHED_TOKENS_FIELD),
            ObjectParser.ValueType.INT_OR_NULL
        );
        PROMPT_TOKENS_DETAILS_PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue(),
            new ParseField(CACHE_WRITE_TOKENS_FIELD),
            ObjectParser.ValueType.INT_OR_NULL
        );

        USAGE_PARSER = new ConstructingObjectParser<>(
            USAGE_FIELD,
            true,
            args -> new ChatCompletionUsage(
                (int) args[0],
                (int) args[1],
                (int) args[2],
                (PromptTokensDetails) args[3],
                (CompletionTokenDetails) args[4]
            )
        );
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(COMPLETION_TOKENS_FIELD));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(PROMPT_TOKENS_FIELD));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField(TOTAL_TOKENS_FIELD));
        USAGE_PARSER.declareObjectOrNull(
            optionalConstructorArg(),
            PROMPT_TOKENS_DETAILS_PARSER,
            null,
            new ParseField(PROMPT_TOKENS_DETAILS_FIELD)
        );
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
    private static ConstructingObjectParser<ChatCompletionChunk, Void> buildRootParser(String choiceContentField, boolean indexRequired) {
        var choiceParser = buildChoiceParser(choiceContentField, buildMessageParser(buildToolCallParser(indexRequired), indexRequired));
        var parser = new ConstructingObjectParser<ChatCompletionChunk, Void>(
            "chat_completion_chunk",
            true,
            args -> new ChatCompletionChunk(
                (String) args[0],
                (List<ChatCompletionChoiceResponse>) args[1],
                Objects.requireNonNullElse((String) args[2], ""),
                Objects.requireNonNullElse((String) args[3], ""),
                (ChatCompletionUsage) args[4]
            )
        );
        parser.declareString(constructorArg(), new ParseField(ID_FIELD));
        parser.declareObjectArray(constructorArg(), choiceParser, new ParseField(CHOICES_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(MODEL_FIELD));
        parser.declareString(optionalConstructorArg(), new ParseField(OBJECT_FIELD));
        parser.declareObjectOrNull(optionalConstructorArg(), USAGE_PARSER, null, new ParseField(USAGE_FIELD));
        return parser;
    }

    private static ConstructingObjectParser<ChatCompletionChoiceResponse, Void> buildChoiceParser(
        String contentFieldName,
        ConstructingObjectParser<ChatCompletionMessage, Void> messageParser
    ) {
        var parser = new ConstructingObjectParser<ChatCompletionChoiceResponse, Void>(
            CHOICES_FIELD,
            true,
            args -> new ChatCompletionChoiceResponse((ChatCompletionMessage) args[0], (String) args[1], (int) args[2])
        );
        parser.declareObject(constructorArg(), messageParser, new ParseField(contentFieldName));
        parser.declareStringOrNull(optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
        parser.declareInt(constructorArg(), new ParseField(INDEX_FIELD));
        return parser;
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ChatCompletionMessage, Void> buildMessageParser(
        ConstructingObjectParser<ChatCompletionToolCall, Void> toolCallParser,
        boolean indexRequired
    ) {
        var parser = new ConstructingObjectParser<ChatCompletionMessage, Void>(
            "message_delta",
            true,
            args -> new ChatCompletionMessage(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (List<ChatCompletionToolCall>) args[3],
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

    private static ConstructingObjectParser<ChatCompletionToolCall, Void> buildToolCallParser(boolean indexRequired) {
        var parser = new ConstructingObjectParser<ChatCompletionToolCall, Void>(
            TOOL_CALLS_FIELD,
            true,
            args -> new ChatCompletionToolCall(
                args[0] == null ? UNSET_INDEX : (int) args[0],
                (String) args[1],
                (ChatCompletionToolCall.Function) args[2],
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
     * Intermediate holder used by the non-streaming parser so that a missing {@code index} is
     * representable as {@code null}.
     */
    private record ParsedToolCall(@Nullable Integer index, String id, ChatCompletionToolCall.Function function, String type) {}

    /**
     * Parses a {@code tool_calls} array, assigning positional indices when none are declared.
     *
     * <ul>
     *   <li>All indices absent → numbered {@code 0..n-1}.</li>
     *   <li>All indices present → declared values honored.</li>
     *   <li>Mixed → {@link XContentParseException} (ambiguous merge order).</li>
     * </ul>
     *
     * A fresh counter starts at 0 for each call, so numbering restarts per-choice.
     */
    private static List<ChatCompletionToolCall> parseToolCallsWithPositionalIndex(
        XContentParser parser,
        ConstructingObjectParser<ChatCompletionToolCall, Void> toolParser
    ) throws IOException {
        var position = new AtomicInteger();
        var parsed = new ArrayList<ParsedToolCall>();
        AbstractObjectParser.parseArray(parser, null, (itemParser, ctx) -> {
            var raw = toolParser.parse(itemParser, null);
            var declaredIndex = raw.index() == UNSET_INDEX ? null : (Integer) raw.index();
            parsed.add(new ParsedToolCall(declaredIndex, raw.id(), raw.function(), raw.type()));
            return null;
        });

        var allAbsent = parsed.stream().allMatch(p -> p.index() == null);
        var allPresent = parsed.stream().allMatch(p -> p.index() != null);
        if (allAbsent == false && allPresent == false) {
            throw new XContentParseException(
                "tool_calls array mixes elements with and without 'index'; cannot assign positional indices unambiguously"
            );
        }

        return parsed.stream().map(p -> {
            var idx = p.index() == null ? position.getAndIncrement() : p.index();
            return new ChatCompletionToolCall(idx, p.id(), p.function(), p.type());
        }).toList();
    }

    private OpenAiUnifiedChatCompletionParser() {}
}
