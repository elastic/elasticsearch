/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.Message;

import org.elasticsearch.core.Strings;

import java.util.Collection;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseUtils.getConverseMessageList;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseUtils.inferenceConfig;

public final class AmazonBedrockConverseRequestUtils {
    public static ConverseRequest getConverseRequest(String modelId, AmazonBedrockConverseRequestEntity requestEntity) {
        var converseRequest = ConverseRequest.builder()
            .modelId(modelId)
            .messages(getConverseMessageList(requestEntity.messages()))
            .additionalModelResponseFieldPaths(requestEntity.additionalModelFields());
        inferenceConfig(requestEntity).ifPresent(converseRequest::inferenceConfig);
        return converseRequest.build();
    }

    public static boolean doesConverseRequestHasMessage(ConverseRequest converseRequest, String expectedMessage) {
        if (expectedMessage == null) {
            return false;
        }
        return converseRequest.messages()
            .stream()
            .map(Message::content)
            .flatMap(Collection::stream)
            .map(ContentBlock::text)
            .anyMatch(expectedMessage::equals);
    }

    public static boolean doesConverseRequestHaveAnyTemperatureInput(ConverseRequest converseRequest) {
        return converseRequest.inferenceConfig() != null
            && converseRequest.inferenceConfig().temperature() != null
            && (converseRequest.inferenceConfig().temperature().isNaN() == false);
    }

    public static boolean doesConverseRequestHaveAnyTopPInput(ConverseRequest converseRequest) {
        return converseRequest.inferenceConfig() != null
            && converseRequest.inferenceConfig().topP() != null
            && (converseRequest.inferenceConfig().topP().isNaN() == false);
    }

    public static boolean doesConverseRequestHaveAnyMaxTokensInput(ConverseRequest converseRequest) {
        return converseRequest.inferenceConfig() != null && converseRequest.inferenceConfig().maxTokens() != null;
    }

    public static boolean doesConverseRequestHaveTemperatureInput(ConverseRequest converseRequest, Double temperature) {
        return doesConverseRequestHaveAnyTemperatureInput(converseRequest)
            && converseRequest.inferenceConfig().temperature().equals(temperature.floatValue());
    }

    public static boolean doesConverseRequestHaveTopPInput(ConverseRequest converseRequest, Double topP) {
        return doesConverseRequestHaveAnyTopPInput(converseRequest) && converseRequest.inferenceConfig().topP().equals(topP.floatValue());
    }

    public static boolean doesConverseRequestHaveMaxTokensInput(ConverseRequest converseRequest, Integer maxTokens) {
        return doesConverseRequestHaveAnyMaxTokensInput(converseRequest) && converseRequest.inferenceConfig().maxTokens().equals(maxTokens);
    }

    public static boolean doesConverseRequestHaveAnyTopKInput(ConverseRequest converseRequest) {
        if (converseRequest.additionalModelResponseFieldPaths() == null) {
            return false;
        }

        for (String fieldPath : converseRequest.additionalModelResponseFieldPaths()) {
            if (fieldPath.contains("{\"top_k\":")) {
                return true;
            }
        }
        return false;
    }

    public static boolean doesConverseRequestHaveTopKInput(ConverseRequest converseRequest, Double topK) {
        if (doesConverseRequestHaveAnyTopKInput(converseRequest) == false) {
            return false;
        }

        var checkString = Strings.format("{\"top_k\":%f}", topK.floatValue());
        for (String fieldPath : converseRequest.additionalModelResponseFieldPaths()) {
            if (fieldPath.contains(checkString)) {
                return true;
            }
        }
        return false;
    }
}
