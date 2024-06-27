/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettings;

import java.util.List;

public final class AmazonBedrockChatCompletionEntityFactory {
    public static AmazonBedrockConverseRequestEntity createEntity(AmazonBedrockChatCompletionModel model, List<String> messages) {
        var serviceSettings = (AmazonBedrockChatCompletionServiceSettings) model.getServiceSettings();
        var taskSettings = (AmazonBedrockChatCompletionTaskSettings) model.getTaskSettings();
        switch (serviceSettings.provider()) {
            case AI21Labs -> {
                return new AmazonBedrockAI21LabsCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case AmazonTitan -> {
                return new AmazonBedrockTitanCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case Anthropic -> {
                return new AmazonBedrockAnthropicCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.topK(),
                    taskSettings.maxNewTokens()
                );
            }
            case Cohere -> {
                return new AmazonBedrockCohereCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.topK(),
                    taskSettings.maxNewTokens()
                );
            }
            case Meta -> {
                return new AmazonBedrockMetaCompletionRequestEntity(
                    messages,
                    taskSettings.temperature(),
                    taskSettings.topP(),
                    taskSettings.maxNewTokens()
                );
            }
            case Mistral -> {
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
