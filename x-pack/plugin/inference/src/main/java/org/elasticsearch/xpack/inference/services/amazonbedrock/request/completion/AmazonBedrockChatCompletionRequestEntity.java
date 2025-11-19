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

public record AmazonBedrockChatCompletionRequestEntity(
    List<UnifiedCompletionRequest.Message> messages,
    @Nullable String model,
    @Nullable Long maxCompletionTokens,
    @Nullable List<String> stop,
    @Nullable Float temperature,
    @Nullable UnifiedCompletionRequest.ToolChoice toolChoice,
    @Nullable List<UnifiedCompletionRequest.Tool> tools,
    @Nullable Float topP
) {}
