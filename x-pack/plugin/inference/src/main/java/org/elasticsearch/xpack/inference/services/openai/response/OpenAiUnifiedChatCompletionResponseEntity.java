/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;

/**
 * Parses a non-streaming OpenAI-compatible chat completion response into {@link ChatCompletionChunkResponse}.
 *
 * <p>Delegates parsing to {@link OpenAiUnifiedChatCompletionParser#parseNonStreamingResponse}, which shares
 * its parser tree with the streaming path ({@code OpenAiUnifiedStreamingProcessor}) to avoid duplication.
 * The only structural difference between the two paths is that non-streaming responses carry content under
 * {@code message} while streaming responses carry it under {@code delta}.
 *
 * <p>Example response:
 * <pre>{@code
 * {
 *   "id": "chatcmpl-BvPBJe8dMX8O7Gwr4DCWFVQC3DVNG",
 *   "object": "chat.completion",
 *   "model": "o4-mini-2025-04-16",
 *   "choices": [
 *     {
 *       "index": 0,
 *       "message": {
 *         "role": "assistant",
 *         "content": null,
 *         "tool_calls": [
 *           {
 *             "id": "call_GWnNsJk5JvBKDgHKNRuGCheZ",
 *             "type": "function",
 *             "function": {
 *               "name": "check_visa_requirements",
 *               "arguments": "{\"destination\":\"Japan\",\"passport_country\":\"USA\"}"
 *             }
 *           }
 *         ]
 *       },
 *       "finish_reason": "tool_calls"
 *     }
 *   ],
 *   "usage": {
 *     "prompt_tokens": 97,
 *     "completion_tokens": 27,
 *     "total_tokens": 124,
 *     "prompt_tokens_details": {"cached_tokens": 0},
 *     "completion_tokens_details": {"reasoning_tokens": 0}
 *   }
 * }
 * }</pre>
 */
public class OpenAiUnifiedChatCompletionResponseEntity {

    public static ChatCompletionChunkResponse fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        return fromResponse(response.body());
    }

    public static ChatCompletionChunkResponse fromResponse(byte[] body) throws IOException {
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, body)) {
            return OpenAiUnifiedChatCompletionParser.parseNonStreamingResponse(p);
        }
    }

    private OpenAiUnifiedChatCompletionResponseEntity() {}
}
