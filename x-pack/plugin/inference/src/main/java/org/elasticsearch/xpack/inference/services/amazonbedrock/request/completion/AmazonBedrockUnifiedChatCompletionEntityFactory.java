/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.additionalTopK;

public class AmazonBedrockUnifiedChatCompletionEntityFactory {
    public static AmazonBedrockUnifiedConverseRequestEntity createEntity(
        AmazonBedrockChatCompletionModel model, UnifiedCompletionRequest request) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(request);
        var serviceSettings = model.getServiceSettings();
        var taskSettings = model.getTaskSettings();

        var messages = request.messages().stream()
            .map(m -> new UnifiedCompletionRequest.Message(
                m.content(),
                toBedrockRole(m.role()),
                "toolCallId",
                List.of()
            ))
            .toList();

        switch (serviceSettings.provider()) {
            case AI21LABS, AMAZONTITAN, META -> {
                return new AmazonBedrockUnifiedConverseRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case ANTHROPIC, COHERE, MISTRAL -> {
                return new AmazonBedrockUnifiedConverseRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens(),
                    additionalTopK(taskSettings.topK())
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

//    private static List<ContentBlock> toBedrockContent(UnifiedCompletionRequest.Content content) {
//        if (content instanceof UnifiedCompletionRequest.ContentString contentString) {
//            return List.of(ContentBlock.fromText(contentString.content()));
//        }
//        return List.of();
//    }

//    private static BedrockToolConfig toBedrockContent(List<UnifiedCompletionRequest.Tool> tool) {
//
//
//
//    }
}
