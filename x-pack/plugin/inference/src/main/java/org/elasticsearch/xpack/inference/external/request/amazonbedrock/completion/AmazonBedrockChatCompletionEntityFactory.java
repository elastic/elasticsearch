/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.util.List;
import java.util.Objects;

public final class AmazonBedrockChatCompletionEntityFactory {
    public static AmazonBedrockConverseRequestEntity createEntity(AmazonBedrockChatCompletionModel model, List<String> messages) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(messages);
        var serviceSettings = model.getServiceSettings();
        var taskSettings = model.getTaskSettings();
        switch (serviceSettings.provider()) {
            case AI21LABS -> {
                return new AmazonBedrockAI21LabsCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case AMAZONTITAN -> {
                return new AmazonBedrockTitanCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case ANTHROPIC -> {
                return new AmazonBedrockAnthropicCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.topK(),
                    taskSettings.maxNewTokens()
                );
            }
            case COHERE -> {
                return new AmazonBedrockCohereCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.topK(),
                    taskSettings.maxNewTokens()
                );
            }
            case META -> {
                return new AmazonBedrockMetaCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case MISTRAL -> {
                return new AmazonBedrockMistralCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.topK(),
                    taskSettings.maxNewTokens()
                );
            }
            default -> {
                return null;
            }
        }
    }
}
