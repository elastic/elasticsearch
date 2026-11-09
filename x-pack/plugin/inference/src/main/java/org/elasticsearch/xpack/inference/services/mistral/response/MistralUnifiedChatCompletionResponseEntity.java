/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiUnifiedChatCompletionParser;

import java.io.IOException;

/**
 * Parses a non-streaming Mistral chat completion response into a {@link ChatCompletionChunkResponse}.
 * Identical to the OpenAI parse, except that list-typed {@code content} (Mistral reasoning models) is first
 * normalized, see {@link MistralChatCompletionResponseNormalizer}.
 */
public class MistralUnifiedChatCompletionResponseEntity {

    public static ChatCompletionChunkResponse fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        return fromResponse(response.body());
    }

    static ChatCompletionChunkResponse fromResponse(byte[] body) throws IOException {
        var normalized = MistralChatCompletionResponseNormalizer.normalizeNonStreamingResponse(body);
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, normalized)) {
            return OpenAiUnifiedChatCompletionParser.parseNonStreamingResponse(p);
        }
    }

    private MistralUnifiedChatCompletionResponseEntity() {}
}
