/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ContentBlock;
import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.Message;

import org.elasticsearch.core.Strings;

public final class AmazonBedrockConverseRequestUtils {
    public static ConverseRequest getConverseRequest(String modelId, AmazonBedrockConverseRequestEntity requestEntity) {
        var converseRequest = new ConverseRequest().withModelId(modelId);
        converseRequest = requestEntity.addMessages(converseRequest);
        converseRequest = requestEntity.addInferenceConfig(converseRequest);
        converseRequest = requestEntity.addAdditionalModelFields(converseRequest);
        return converseRequest;
    }

    public static boolean doesConverseRequestHasMessage(ConverseRequest converseRequest, String expectedMessage) {
        for (Message message : converseRequest.getMessages()) {
            var content = message.getContent();
            for (ContentBlock contentBlock : content) {
                if (contentBlock.getText().equals(expectedMessage)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean doesConverseRequestHaveAnyTemperatureInput(ConverseRequest converseRequest) {
        return converseRequest.getInferenceConfig() != null
            && converseRequest.getInferenceConfig().getTemperature() != null
            && (converseRequest.getInferenceConfig().getTemperature().isNaN() == false);
    }

    public static boolean doesConverseRequestHaveAnyTopPInput(ConverseRequest converseRequest) {
        return converseRequest.getInferenceConfig() != null
            && converseRequest.getInferenceConfig().getTopP() != null
            && (converseRequest.getInferenceConfig().getTopP().isNaN() == false);
    }

    public static boolean doesConverseRequestHaveAnyMaxTokensInput(ConverseRequest converseRequest) {
        return converseRequest.getInferenceConfig() != null && converseRequest.getInferenceConfig().getMaxTokens() != null;
    }

    public static boolean doesConverseRequestHaveTemperatureInput(ConverseRequest converseRequest, Double temperature) {
        return doesConverseRequestHaveAnyTemperatureInput(converseRequest)
            && converseRequest.getInferenceConfig().getTemperature().equals(temperature.floatValue());
    }

    public static boolean doesConverseRequestHaveTopPInput(ConverseRequest converseRequest, Double topP) {
        return doesConverseRequestHaveAnyTopPInput(converseRequest)
            && converseRequest.getInferenceConfig().getTopP().equals(topP.floatValue());
    }

    public static boolean doesConverseRequestHaveMaxTokensInput(ConverseRequest converseRequest, Integer maxTokens) {
        return doesConverseRequestHaveAnyMaxTokensInput(converseRequest)
            && converseRequest.getInferenceConfig().getMaxTokens().equals(maxTokens);
    }

    public static boolean doesConverseRequestHaveAnyTopKInput(ConverseRequest converseRequest) {
        if (converseRequest.getAdditionalModelResponseFieldPaths() == null) {
            return false;
        }

        for (String fieldPath : converseRequest.getAdditionalModelResponseFieldPaths()) {
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
        for (String fieldPath : converseRequest.getAdditionalModelResponseFieldPaths()) {
            if (fieldPath.contains(checkString)) {
                return true;
            }
        }
        return false;
    }
}
