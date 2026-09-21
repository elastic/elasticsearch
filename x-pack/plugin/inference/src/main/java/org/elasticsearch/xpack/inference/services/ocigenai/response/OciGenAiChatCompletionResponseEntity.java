/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.CompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChoiceResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionMessageResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionToolCallResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsageResponse;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parses OCI Generative AI {@code chat} responses (both API formats, streaming events and complete responses) into the OpenAI
 * compatible {@link ChatCompletionChunkResponse} used by the inference API.
 * <p>
 * A complete {@code GENERIC} response looks like:
 * <pre>
 *     <code>
 * {
 *   "chatResponse": {
 *     "apiFormat": "GENERIC",
 *     "choices": [ {
 *       "finishReason": "stop",
 *       "index": 0,
 *       "message": { "role": "ASSISTANT", "content": [ { "type": "TEXT", "text": "Hello, how are you today?" } ], "toolCalls": [] }
 *     } ],
 *     "timeCreated": "2026-09-18T19:22:24.776Z",
 *     "usage": { "completionTokens": 8, "promptTokens": 45, "totalTokens": 53 }
 *   },
 *   "modelId": "meta.llama-3.3-70b-instruct",
 *   "modelVersion": "1.0.0"
 * }
 *     </code>
 * </pre>
 * and a complete {@code COHERE} response like:
 * <pre>
 *     <code>
 * {
 *   "chatResponse": {
 *     "apiFormat": "COHERE",
 *     "chatHistory": [ ... ],
 *     "finishReason": "COMPLETE",
 *     "text": "Hello, how are you?",
 *     "usage": { "completionTokens": 6, "promptTokens": 13, "totalTokens": 19 }
 *   },
 *   "modelId": "cohere.command-a-03-2025",
 *   "modelVersion": "1.0"
 * }
 *     </code>
 * </pre>
 * Streaming events carry a single choice without the {@code chatResponse} wrapper, for example
 * {@code {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":"Hello"}]},"pad":"aaa"}} for the {@code GENERIC}
 * format and {@code {"apiFormat":"COHERE","text":"Hello","pad":"aaa"}} for the {@code COHERE} format. The final event carries the
 * {@code finishReason}; for the {@code COHERE} format it also repeats the complete text, which is not a delta and is ignored.
 */
public class OciGenAiChatCompletionResponseEntity {

    public static final String CHAT_COMPLETION_OBJECT = "chat.completion";
    public static final String CHAT_COMPLETION_CHUNK_OBJECT = "chat.completion.chunk";

    private static final String CHAT_RESPONSE_FIELD = "chatResponse";
    private static final String MODEL_ID_FIELD = "modelId";
    private static final String API_FORMAT_FIELD = "apiFormat";
    private static final String CHOICES_FIELD = "choices";
    private static final String TEXT_FIELD = "text";
    private static final String FINISH_REASON_FIELD = "finishReason";
    private static final String USAGE_FIELD = "usage";
    private static final String INDEX_FIELD = "index";
    private static final String MESSAGE_FIELD = "message";
    private static final String ROLE_FIELD = "role";
    private static final String CONTENT_FIELD = "content";
    private static final String TYPE_FIELD = "type";
    private static final String TOOL_CALLS_FIELD = "toolCalls";
    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String ARGUMENTS_FIELD = "arguments";
    private static final String COMPLETION_TOKENS_FIELD = "completionTokens";
    private static final String PROMPT_TOKENS_FIELD = "promptTokens";
    private static final String TOTAL_TOKENS_FIELD = "totalTokens";

    private static final String TEXT_CONTENT_TYPE = "TEXT";
    private static final String FUNCTION_TOOL_TYPE = "function";

    private static final String COHERE_FINISH_REASON_COMPLETE = "COMPLETE";
    private static final String COHERE_FINISH_REASON_MAX_TOKENS = "MAX_TOKENS";
    private static final String FINISH_REASON_STOP = "stop";
    private static final String FINISH_REASON_LENGTH = "length";

    private record ContentItem(@Nullable String type, @Nullable String text) {}

    private record ToolCall(@Nullable String id, @Nullable String name, @Nullable String arguments) {}

    private record ChatMessage(@Nullable String role, @Nullable List<ContentItem> content, @Nullable List<ToolCall> toolCalls) {}

    private record Choice(@Nullable Integer index, @Nullable ChatMessage message, @Nullable String finishReason) {}

    private record Usage(@Nullable Integer completionTokens, @Nullable Integer promptTokens, @Nullable Integer totalTokens) {}

    private record ChatResponse(
        @Nullable String apiFormat,
        @Nullable List<Choice> choices,
        @Nullable String text,
        @Nullable String finishReason,
        @Nullable Usage usage
    ) {}

    private record Response(@Nullable String modelId, @Nullable ChatResponse chatResponse) {}

    /**
     * A streaming event: a {@code GENERIC} choice ({@code index}, {@code message}, {@code finishReason}) or a {@code COHERE} delta
     * ({@code apiFormat}, {@code text}, {@code finishReason}).
     */
    private record StreamEvent(
        @Nullable Integer index,
        @Nullable ChatMessage message,
        @Nullable String finishReason,
        @Nullable String apiFormat,
        @Nullable String text
    ) {}

    private static final ConstructingObjectParser<ContentItem, Void> CONTENT_ITEM_PARSER = new ConstructingObjectParser<>(
        "oci_genai_content_item",
        true,
        args -> new ContentItem((String) args[0], (String) args[1])
    );
    private static final ConstructingObjectParser<ToolCall, Void> TOOL_CALL_PARSER = new ConstructingObjectParser<>(
        "oci_genai_tool_call",
        true,
        args -> new ToolCall((String) args[0], (String) args[1], (String) args[2])
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ChatMessage, Void> MESSAGE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_message",
        true,
        args -> new ChatMessage((String) args[0], (List<ContentItem>) args[1], (List<ToolCall>) args[2])
    );
    private static final ConstructingObjectParser<Choice, Void> CHOICE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_choice",
        true,
        args -> new Choice((Integer) args[0], (ChatMessage) args[1], (String) args[2])
    );
    private static final ConstructingObjectParser<Usage, Void> USAGE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_usage",
        true,
        args -> new Usage((Integer) args[0], (Integer) args[1], (Integer) args[2])
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ChatResponse, Void> CHAT_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_chat_response",
        true,
        args -> new ChatResponse((String) args[0], (List<Choice>) args[1], (String) args[2], (String) args[3], (Usage) args[4])
    );
    private static final ConstructingObjectParser<Response, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_chat_completion_response",
        true,
        args -> new Response((String) args[0], (ChatResponse) args[1])
    );
    private static final ConstructingObjectParser<StreamEvent, Void> STREAM_EVENT_PARSER = new ConstructingObjectParser<>(
        "oci_genai_chat_completion_stream_event",
        true,
        args -> new StreamEvent((Integer) args[0], (ChatMessage) args[1], (String) args[2], (String) args[3], (String) args[4])
    );

    static {
        CONTENT_ITEM_PARSER.declareString(optionalConstructorArg(), new ParseField(TYPE_FIELD));
        CONTENT_ITEM_PARSER.declareString(optionalConstructorArg(), new ParseField(TEXT_FIELD));

        TOOL_CALL_PARSER.declareString(optionalConstructorArg(), new ParseField(ID_FIELD));
        TOOL_CALL_PARSER.declareString(optionalConstructorArg(), new ParseField(NAME_FIELD));
        TOOL_CALL_PARSER.declareString(optionalConstructorArg(), new ParseField(ARGUMENTS_FIELD));

        MESSAGE_PARSER.declareString(optionalConstructorArg(), new ParseField(ROLE_FIELD));
        MESSAGE_PARSER.declareObjectArray(optionalConstructorArg(), CONTENT_ITEM_PARSER, new ParseField(CONTENT_FIELD));
        MESSAGE_PARSER.declareObjectArray(optionalConstructorArg(), TOOL_CALL_PARSER, new ParseField(TOOL_CALLS_FIELD));

        CHOICE_PARSER.declareInt(optionalConstructorArg(), new ParseField(INDEX_FIELD));
        CHOICE_PARSER.declareObject(optionalConstructorArg(), MESSAGE_PARSER, new ParseField(MESSAGE_FIELD));
        CHOICE_PARSER.declareString(optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));

        USAGE_PARSER.declareInt(optionalConstructorArg(), new ParseField(COMPLETION_TOKENS_FIELD));
        USAGE_PARSER.declareInt(optionalConstructorArg(), new ParseField(PROMPT_TOKENS_FIELD));
        USAGE_PARSER.declareInt(optionalConstructorArg(), new ParseField(TOTAL_TOKENS_FIELD));

        CHAT_RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField(API_FORMAT_FIELD));
        CHAT_RESPONSE_PARSER.declareObjectArray(optionalConstructorArg(), CHOICE_PARSER, new ParseField(CHOICES_FIELD));
        CHAT_RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField(TEXT_FIELD));
        CHAT_RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
        CHAT_RESPONSE_PARSER.declareObject(optionalConstructorArg(), USAGE_PARSER, new ParseField(USAGE_FIELD));

        RESPONSE_PARSER.declareString(optionalConstructorArg(), new ParseField(MODEL_ID_FIELD));
        RESPONSE_PARSER.declareObject(optionalConstructorArg(), CHAT_RESPONSE_PARSER, new ParseField(CHAT_RESPONSE_FIELD));

        STREAM_EVENT_PARSER.declareInt(optionalConstructorArg(), new ParseField(INDEX_FIELD));
        STREAM_EVENT_PARSER.declareObject(optionalConstructorArg(), MESSAGE_PARSER, new ParseField(MESSAGE_FIELD));
        STREAM_EVENT_PARSER.declareString(optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
        STREAM_EVENT_PARSER.declareString(optionalConstructorArg(), new ParseField(API_FORMAT_FIELD));
        STREAM_EVENT_PARSER.declareString(optionalConstructorArg(), new ParseField(TEXT_FIELD));
    }

    /**
     * Parses a complete (non-streaming) chat response into the unified chat completion result.
     */
    public static ChatCompletionChunkResponse fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        return fromResponse(response.body(), UUIDs.randomBase64UUID());
    }

    /**
     * Parses a complete (non-streaming) chat response into the legacy {@code completion} task result.
     */
    public static CompletionResults fromResponseAsCompletion(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        var chunk = fromResponse(response.body(), UUIDs.randomBase64UUID());
        var results = new ArrayList<CompletionResults.Result>();
        if (chunk.choices() != null) {
            for (var choice : chunk.choices()) {
                if (choice.message() != null && choice.message().content() != null) {
                    results.add(new CompletionResults.Result(choice.message().content()));
                }
            }
        }
        return new CompletionResults(results);
    }

    static ChatCompletionChunkResponse fromResponse(byte[] body, String id) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, body)) {
            var response = RESPONSE_PARSER.apply(parser, null);
            if (response.chatResponse() == null) {
                throw new IllegalStateException(
                    Strings.format("Failed to find required field [%s] in OCI Generative AI chat response", CHAT_RESPONSE_FIELD)
                );
            }
            return toChunk(response, id);
        }
    }

    private static ChatCompletionChunkResponse toChunk(Response response, String id) {
        var chatResponse = response.chatResponse();
        var choices = new ArrayList<ChatCompletionChoiceResponse>();

        if (chatResponse.choices() != null) {
            for (var choice : chatResponse.choices()) {
                choices.add(
                    new ChatCompletionChoiceResponse(
                        toMessageResponse(choice.message()),
                        choice.finishReason(),
                        Objects.requireNonNullElse(choice.index(), 0)
                    )
                );
            }
        } else {
            // COHERE format: a single answer in the text field
            choices.add(
                new ChatCompletionChoiceResponse(
                    new ChatCompletionMessageResponse(chatResponse.text(), null, toOpenAiRole("ASSISTANT"), null),
                    toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, chatResponse.finishReason()),
                    0
                )
            );
        }

        return new ChatCompletionChunkResponse(
            id,
            choices,
            Objects.requireNonNullElse(response.modelId(), ""),
            CHAT_COMPLETION_OBJECT,
            toUsageResponse(chatResponse.usage())
        );
    }

    /**
     * Parses a single streaming event into a chunk.
     *
     * @param parser  a parser positioned at the start of the event object
     * @param id      the id shared by all chunks of the stream
     * @param modelId the model id to report, because OCI Generative AI streaming events do not carry it
     * @return the chunk, or {@code null} when the event carries no content, tool call or finish reason
     */
    @Nullable
    public static ChatCompletionChunkResponse parseStreamingEvent(XContentParser parser, String id, String modelId) throws IOException {
        var event = STREAM_EVENT_PARSER.apply(parser, null);

        final ChatCompletionChoiceResponse choice;
        if (OciGenAiChatApiFormat.COHERE.name().equals(event.apiFormat()) || (event.message() == null && event.text() != null)) {
            if (event.finishReason() != null) {
                // the final Cohere event repeats the full text, which must not be emitted again
                choice = new ChatCompletionChoiceResponse(
                    new ChatCompletionMessageResponse(null, null, null, null),
                    toOpenAiFinishReason(OciGenAiChatApiFormat.COHERE, event.finishReason()),
                    0
                );
            } else if (Strings.isNullOrEmpty(event.text()) == false) {
                choice = new ChatCompletionChoiceResponse(
                    new ChatCompletionMessageResponse(event.text(), null, toOpenAiRole("ASSISTANT"), null),
                    null,
                    0
                );
            } else {
                return null;
            }
        } else {
            var message = event.message() == null ? null : toMessageResponse(event.message());
            var hasContent = message != null && (message.content() != null || message.toolCalls() != null || message.role() != null);
            if (hasContent == false && event.finishReason() == null) {
                return null;
            }
            choice = new ChatCompletionChoiceResponse(
                message == null ? new ChatCompletionMessageResponse(null, null, null, null) : message,
                event.finishReason(),
                Objects.requireNonNullElse(event.index(), 0)
            );
        }

        return new ChatCompletionChunkResponse(id, List.of(choice), modelId, CHAT_COMPLETION_CHUNK_OBJECT, null);
    }

    private static ChatCompletionMessageResponse toMessageResponse(@Nullable ChatMessage message) {
        if (message == null) {
            return new ChatCompletionMessageResponse(null, null, null, null);
        }

        String content = null;
        if (message.content() != null) {
            var text = new StringBuilder();
            for (var item : message.content()) {
                if (item.text() != null && (item.type() == null || TEXT_CONTENT_TYPE.equals(item.type()))) {
                    text.append(item.text());
                }
            }
            content = text.isEmpty() ? null : text.toString();
        }

        List<ChatCompletionToolCallResponse> toolCalls = null;
        if (message.toolCalls() != null && message.toolCalls().isEmpty() == false) {
            toolCalls = new ArrayList<>(message.toolCalls().size());
            for (int i = 0; i < message.toolCalls().size(); i++) {
                var toolCall = message.toolCalls().get(i);
                toolCalls.add(
                    new ChatCompletionToolCallResponse(
                        i,
                        toolCall.id(),
                        new ChatCompletionToolCallResponse.Function(toolCall.arguments(), toolCall.name()),
                        FUNCTION_TOOL_TYPE
                    )
                );
            }
        }

        return new ChatCompletionMessageResponse(content, null, toOpenAiRole(message.role()), toolCalls);
    }

    @Nullable
    private static ChatCompletionUsageResponse toUsageResponse(@Nullable Usage usage) {
        if (usage == null || (usage.completionTokens() == null && usage.promptTokens() == null && usage.totalTokens() == null)) {
            return null;
        }
        return new ChatCompletionUsageResponse(
            Objects.requireNonNullElse(usage.completionTokens(), 0),
            Objects.requireNonNullElse(usage.promptTokens(), 0),
            Objects.requireNonNullElse(usage.totalTokens(), 0)
        );
    }

    @Nullable
    private static String toOpenAiRole(@Nullable String role) {
        return role == null ? null : role.toLowerCase(Locale.ROOT);
    }

    /**
     * The {@code GENERIC} format already uses OpenAI style finish reasons ({@code stop}, {@code length}, {@code tool_calls}); the
     * {@code COHERE} format uses its own ({@code COMPLETE}, {@code MAX_TOKENS}, ...) which are translated.
     */
    @Nullable
    static String toOpenAiFinishReason(OciGenAiChatApiFormat apiFormat, @Nullable String finishReason) {
        if (finishReason == null || apiFormat == OciGenAiChatApiFormat.GENERIC) {
            return finishReason;
        }
        return switch (finishReason) {
            case COHERE_FINISH_REASON_COMPLETE -> FINISH_REASON_STOP;
            case COHERE_FINISH_REASON_MAX_TOKENS -> FINISH_REASON_LENGTH;
            default -> finishReason.toLowerCase(Locale.ROOT);
        };
    }

    private OciGenAiChatCompletionResponseEntity() {}
}
