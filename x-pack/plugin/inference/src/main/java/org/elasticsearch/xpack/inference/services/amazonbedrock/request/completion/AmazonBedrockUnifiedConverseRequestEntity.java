/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequest;

import java.util.List;

public record AmazonBedrockUnifiedConverseRequestEntity (
    List<UnifiedCompletionRequest.Message> messages,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Integer maxTokenCount,
    @Nullable List<String> additionalModelFields
) {
    public AmazonBedrockUnifiedConverseRequestEntity(
        List<UnifiedCompletionRequest.Message> messages,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Integer maxTokenCount
    )  {
        this(messages, temperature, topP, maxTokenCount, null);
    }
}

