/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.InferenceConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.Message;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest.USER_ROLE;

public final class AmazonBedrockConverseUtils {

    public static List<Message> getConverseMessageList(List<String> texts) {
        return texts.stream()
            .map(text -> ContentBlock.builder().text(text).build())
            .map(content -> Message.builder().role(USER_ROLE).content(content).build())
            .toList();
    }

    public static Optional<InferenceConfiguration> inferenceConfig(AmazonBedrockConverseRequestEntity request) {
        if (request.temperature() != null || request.topP() != null || request.maxTokenCount() != null) {
            var builder = InferenceConfiguration.builder();
            if (request.temperature() != null) {
                builder.temperature(request.temperature().floatValue());
            }

            if (request.topP() != null) {
                builder.topP(request.topP().floatValue());
            }

            if (request.maxTokenCount() != null) {
                builder.maxTokens(request.maxTokenCount());
            }
            return Optional.of(builder.build());
        }
        return Optional.empty();
    }

    @Nullable
    public static List<String> additionalTopK(@Nullable Double topK) {
        if (topK == null) {
            return null;
        }

        return List.of(Strings.format("{\"top_k\":%f}", topK.floatValue()));
    }
}
