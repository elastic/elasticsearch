/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseUtils.getConverseMessageList;

public record AmazonBedrockMetaCompletionRequestEntity(
    List<String> messages,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Integer maxTokenCount
) implements AmazonBedrockConverseRequestEntity {

    public AmazonBedrockMetaCompletionRequestEntity {
        Objects.requireNonNull(messages);
    }

    @Override
    public ConverseRequest.Builder addMessages(ConverseRequest.Builder request) {
        return request.messages(getConverseMessageList(messages));
    }

    @Override
    public ConverseRequest.Builder addInferenceConfig(ConverseRequest.Builder request) {
        if (temperature == null && topP == null && maxTokenCount == null) {
            return request;
        }

        return request.inferenceConfig(config -> {
            if (temperature != null) {
                config.temperature(temperature.floatValue());
            }

            if (topP != null) {
                config.topP(topP.floatValue());
            }

            if (maxTokenCount != null) {
                config.maxTokens(maxTokenCount);
            }
        });
    }

    @Override
    public ConverseRequest.Builder addAdditionalModelFields(ConverseRequest.Builder request) {
        return request;
    }
}
