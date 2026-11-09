/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.xpack.core.inference.results.CompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiCompletionResponseEntity;

import java.io.IOException;

/**
 * Parses a non-streaming Mistral completion response. Identical to the OpenAI parse, except that list-typed
 * {@code content} (Mistral reasoning models) is first normalized, see {@link MistralChatCompletionResponseNormalizer}.
 */
public class MistralCompletionResponseEntity {

    public static CompletionResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        return fromResponse(response.body());
    }

    static CompletionResults fromResponse(byte[] body) throws IOException {
        return OpenAiCompletionResponseEntity.fromResponse(MistralChatCompletionResponseNormalizer.normalizeNonStreamingResponse(body));
    }

    private MistralCompletionResponseEntity() {}
}
