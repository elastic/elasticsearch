/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalDoubleInRange;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_TEMPERATURE_TOP_P_TOP_K_VALUE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MIN_TEMPERATURE_TOP_P_TOP_K_VALUE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_P_FIELD;

public record AmazonBedrockChatCompletionRequestTaskSettings(
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Double topK,
    @Nullable Integer maxNewTokens
) {

    public static final AmazonBedrockChatCompletionRequestTaskSettings EMPTY_SETTINGS = new AmazonBedrockChatCompletionRequestTaskSettings(
        null,
        null,
        null,
        null
    );

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link AmazonBedrockChatCompletionRequestTaskSettings}
     */
    public static AmazonBedrockChatCompletionRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return AmazonBedrockChatCompletionRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        var temperature = extractOptionalDoubleInRange(
            map,
            TEMPERATURE_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var topP = extractOptionalDoubleInRange(
            map,
            TOP_P_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var topK = extractOptionalDoubleInRange(
            map,
            TOP_K_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Integer maxNewTokens = extractOptionalPositiveInteger(
            map,
            MAX_NEW_TOKENS_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockChatCompletionRequestTaskSettings(temperature, topP, topK, maxNewTokens);
    }
}
