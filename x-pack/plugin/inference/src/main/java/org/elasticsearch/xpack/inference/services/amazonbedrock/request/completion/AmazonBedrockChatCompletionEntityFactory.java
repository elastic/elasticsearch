/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.additionalTopK;

public final class AmazonBedrockChatCompletionEntityFactory {
    public static AmazonBedrockCompletionRequestEntity createEntity(AmazonBedrockChatCompletionModel model, List<String> messages) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(messages);
        var serviceSettings = model.getServiceSettings();
        var taskSettings = model.getTaskSettings();
        switch (serviceSettings.provider()) {
            case AI21LABS, AMAZONTITAN, META -> {
                return new AmazonBedrockCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case ANTHROPIC, COHERE, MISTRAL -> {
                return new AmazonBedrockCompletionRequestEntity(
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

    public static AmazonBedrockChatCompletionRequestEntity createEntity(
        AmazonBedrockChatCompletionModel model,
        UnifiedCompletionRequest request
    ) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(request);
        var serviceSettings = model.getServiceSettings();
        var taskSettings = model.getTaskSettings();

        var messages = request.messages()
            .stream()
            .map(message -> new Message(message.content(), message.role(), message.toolCallId(), message.toolCalls()))
            .toList();

        switch (serviceSettings.provider()) {
            case ANTHROPIC, AI21LABS, AMAZONTITAN, COHERE, META, MISTRAL -> {
                // Task settings configured at endpoint creation must be used when the per-request value
                // is null, otherwise endpoint-level configuration is silently ignored. top_k cannot be
                // set on the request at all, so it always comes from task settings.
                return new AmazonBedrockChatCompletionRequestEntity(
                    messages,
                    request.model(),
                    request.maxCompletionTokens() != null
                        ? request.maxCompletionTokens()
                        : (taskSettings.maxNewTokens() != null ? taskSettings.maxNewTokens().longValue() : null),
                    request.stop(),
                    request.temperature() != null
                        ? request.temperature()
                        : (taskSettings.temperature() != null ? taskSettings.temperature().floatValue() : null),
                    request.toolChoice(),
                    request.tools(),
                    request.topP() != null ? request.topP() : (taskSettings.topP() != null ? taskSettings.topP().floatValue() : null),
                    taskSettings.topK()
                );
            }
            default -> {
                return null;
            }
        }
    }
}
