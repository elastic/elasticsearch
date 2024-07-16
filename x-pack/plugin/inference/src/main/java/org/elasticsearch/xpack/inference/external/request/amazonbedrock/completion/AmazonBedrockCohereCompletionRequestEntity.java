/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.InferenceConfiguration;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseUtils.getConverseMessageList;

public record AmazonBedrockCohereCompletionRequestEntity(
    List<String> messages,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Double topK,
    @Nullable Integer maxTokenCount
) implements AmazonBedrockConverseRequestEntity {

    public AmazonBedrockCohereCompletionRequestEntity {
        Objects.requireNonNull(messages);
    }

    @Override
    public ConverseRequest addMessages(ConverseRequest request) {
        return request.withMessages(getConverseMessageList(messages));
    }

    @Override
    public ConverseRequest addInferenceConfig(ConverseRequest request) {
        if (temperature == null && topP == null && maxTokenCount == null) {
            return request;
        }

        InferenceConfiguration inferenceConfig = new InferenceConfiguration();

        if (temperature != null) {
            inferenceConfig = inferenceConfig.withTemperature(temperature.floatValue());
        }

        if (topP != null) {
            inferenceConfig = inferenceConfig.withTopP(topP.floatValue());
        }

        if (maxTokenCount != null) {
            inferenceConfig = inferenceConfig.withMaxTokens(maxTokenCount);
        }

        return request.withInferenceConfig(inferenceConfig);
    }

    @Override
    public ConverseRequest addAdditionalModelFields(ConverseRequest request) {
        if (topK == null) {
            return request;
        }

        String topKField = Strings.format("{\"top_k\":%f}", topK.floatValue());
        return request.withAdditionalModelResponseFieldPaths(topKField);
    }
}
