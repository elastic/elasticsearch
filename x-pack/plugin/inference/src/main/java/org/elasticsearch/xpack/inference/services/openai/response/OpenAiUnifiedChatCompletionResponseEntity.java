/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parses a non-streaming OpenAI-compatible chat completion response into {@link UnifiedChatCompletionResults}.
 *
 * <p>Example response:
 * <pre>{@code
 * {
 *   "id": "chatcmpl-123",
 *   "object": "chat.completion",
 *   "model": "gpt-4o",
 *   "choices": [
 *     {
 *       "index": 0,
 *       "message": {
 *         "role": "assistant",
 *         "content": "Hello!"
 *       },
 *       "finish_reason": "stop"
 *     }
 *   ],
 *   "usage": {
 *     "prompt_tokens": 9,
 *     "completion_tokens": 12,
 *     "total_tokens": 21
 *   }
 * }
 * }</pre>
 */
public class OpenAiUnifiedChatCompletionResponseEntity {

    public static UnifiedChatCompletionResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        return fromResponse(response.body());
    }

    public static UnifiedChatCompletionResults fromResponse(byte[] body) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, body)) {
            return ROOT_PARSER.apply(p, null);
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<UnifiedChatCompletionResults, Void> ROOT_PARSER = new ConstructingObjectParser<>(
        "unified_chat_completion_response",
        true,
        args -> new UnifiedChatCompletionResults(
            (String) args[0],
            (List<UnifiedChatCompletionResults.Choice>) args[1],
            (String) args[2],
            (String) args[3],
            (UnifiedChatCompletionResults.Usage) args[4]
        )
    );

    private static final ConstructingObjectParser<UnifiedChatCompletionResults.Choice, Void> CHOICE_PARSER = new ConstructingObjectParser<>(
        "unified_chat_completion_choice",
        true,
        args -> new UnifiedChatCompletionResults.Choice((int) args[0], (UnifiedChatCompletionResults.Message) args[1], (String) args[2])
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<UnifiedChatCompletionResults.Message, Void> MESSAGE_PARSER =
        new ConstructingObjectParser<>(
            "unified_chat_completion_message",
            true,
            args -> new UnifiedChatCompletionResults.Message(
                (String) args[0],
                (String) args[1],
                (List<UnifiedChatCompletionResults.ToolCall>) args[2],
                (String) args[3]
            )
        );

    private static final ConstructingObjectParser<UnifiedChatCompletionResults.ToolCall, Void> TOOL_CALL_PARSER =
        new ConstructingObjectParser<>(
            "unified_chat_completion_tool_call",
            true,
            args -> new UnifiedChatCompletionResults.ToolCall(
                (String) args[0],
                (String) args[1],
                (UnifiedChatCompletionResults.ToolCall.Function) args[2]
            )
        );

    private static final ConstructingObjectParser<UnifiedChatCompletionResults.ToolCall.Function, Void> FUNCTION_PARSER =
        new ConstructingObjectParser<>(
            "unified_chat_completion_function",
            true,
            args -> new UnifiedChatCompletionResults.ToolCall.Function((String) args[0], (String) args[1])
        );

    private static final ConstructingObjectParser<UnifiedChatCompletionResults.Usage, Void> USAGE_PARSER = new ConstructingObjectParser<>(
        "unified_chat_completion_usage",
        true,
        args -> new UnifiedChatCompletionResults.Usage((int) args[0], (int) args[1], (int) args[2])
    );

    static {
        ROOT_PARSER.declareString(constructorArg(), new ParseField("id"));
        ROOT_PARSER.declareObjectArray(constructorArg(), CHOICE_PARSER::apply, new ParseField("choices"));
        ROOT_PARSER.declareString(constructorArg(), new ParseField("model"));
        ROOT_PARSER.declareString(constructorArg(), new ParseField("object"));
        ROOT_PARSER.declareObject(optionalConstructorArg(), USAGE_PARSER::apply, new ParseField("usage"));

        CHOICE_PARSER.declareInt(constructorArg(), new ParseField("index"));
        CHOICE_PARSER.declareObject(constructorArg(), MESSAGE_PARSER::apply, new ParseField("message"));
        CHOICE_PARSER.declareString(optionalConstructorArg(), new ParseField("finish_reason"));

        MESSAGE_PARSER.declareString(optionalConstructorArg(), new ParseField("role"));
        MESSAGE_PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text(),
            new ParseField("content"),
            ObjectParser.ValueType.STRING_OR_NULL
        );
        MESSAGE_PARSER.declareObjectArray(optionalConstructorArg(), TOOL_CALL_PARSER::apply, new ParseField("tool_calls"));
        MESSAGE_PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text(),
            new ParseField("refusal"),
            ObjectParser.ValueType.STRING_OR_NULL
        );

        TOOL_CALL_PARSER.declareString(constructorArg(), new ParseField("id"));
        TOOL_CALL_PARSER.declareString(constructorArg(), new ParseField("type"));
        TOOL_CALL_PARSER.declareObject(constructorArg(), FUNCTION_PARSER::apply, new ParseField("function"));

        FUNCTION_PARSER.declareString(constructorArg(), new ParseField("name"));
        FUNCTION_PARSER.declareString(constructorArg(), new ParseField("arguments"));

        USAGE_PARSER.declareInt(constructorArg(), new ParseField("completion_tokens"));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField("prompt_tokens"));
        USAGE_PARSER.declareInt(constructorArg(), new ParseField("total_tokens"));
    }

    private OpenAiUnifiedChatCompletionResponseEntity() {}
}
