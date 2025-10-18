/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.util.Objects;

public class AmazonBedrockUnifiedChatCompletionEntityFactory {
    public static AmazonBedrockUnifiedConverseRequestEntity createEntity(
        AmazonBedrockChatCompletionModel model,
        UnifiedCompletionRequest request
    ) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(request);
        var serviceSettings = model.getServiceSettings();

        var messages = request.messages()
            .stream()
            .map(
                message -> new UnifiedCompletionRequest.Message(
                    message.content(),
                    toBedrockRole(message.role()),
                    message.toolCallId(),
                    message.toolCalls()
                )
            )
            .toList();

        switch (serviceSettings.provider()) {
            case ANTHROPIC, AI21LABS, AMAZONTITAN, COHERE, META, MISTRAL -> {
                return new AmazonBedrockUnifiedConverseRequestEntity(
                    messages,
                    request.model(),
                    request.maxCompletionTokens(),
                    request.stop(),
                    request.temperature(),
                    request.toolChoice(),
                    request.tools(),
                    request.topP()
                );
            }
            default -> {
                return null;
            }
        }
    }

    private static String toBedrockRole(String role) {
        return role == null ? "user" : role;
    }
}
